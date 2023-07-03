using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.CustomAttribute;
using Fast.Framework.Enum;
using Fast.Framework.Extensions;
using Fast.Framework.Interfaces;
using Fast.Framework.Models;
using Fast.Framework.Utils;

namespace Fast.Framework.Implements
{


    /// <summary>
    /// 表达式解析Sql
    /// </summary>
    public class ExpressionResolveSql : IExpressionResolveSql
    {

        /// <summary>
        /// 成员信息
        /// </summary>
        private readonly Stack<MemberInfoEx> memberInfos;

        /// <summary>
        /// 数组索引
        /// </summary>
        private Stack<int> arrayIndexs;

        /// <summary>
        /// 体表达式
        /// </summary>
        private Expression bodyExpression;

        /// <summary>
        /// 解析Sql选项
        /// </summary>
        public ResolveSqlOptions ResolveSqlOptions { get; }

        /// <summary>
        /// Lambda参数信息
        /// </summary>
        public List<LambdaParameterInfo> LambdaParameterInfos { get; }

        /// <summary>
        /// 获取值
        /// </summary>
        public IExpressionResolveValue GetValue { get; }

        /// <summary>
        /// Sql构建
        /// </summary>
        public StringBuilder SqlBuilder { get; }

        /// <summary>
        /// 数据库参数
        /// </summary>
        public List<FastParameter> DbParameters { get; }

        /// <summary>
        /// 设置成员信息
        /// </summary>
        public List<SetMemberInfo> SetMemberInfos { get; }

        /// <summary>
        /// 是否Not
        /// </summary>
        public bool IsNot { get; set; }

        /// <summary>
        /// 构造方法
        /// </summary>
        public ExpressionResolveSql(ResolveSqlOptions options)
        {
            this.ResolveSqlOptions = options;
            this.memberInfos = new Stack<MemberInfoEx>();
            this.arrayIndexs = new Stack<int>();

            LambdaParameterInfos = new List<LambdaParameterInfo>();
            GetValue = new ExpressionResolveValue();
            SqlBuilder = new StringBuilder();
            DbParameters = new List<FastParameter>();
            SetMemberInfos = new List<SetMemberInfo>();

            if (options.ParentLambdaParameterInfos != null && options.ParentLambdaParameterInfos.Any())
            {
                if (options.UseCustomParameter)
                {
                    options.CustomParameterStartIndex += options.ParentLambdaParameterInfos.Max(m => m.ParameterIndex);
                }
                foreach (var item in options.ParentLambdaParameterInfos)
                {
                    item.IsUsing = false;//下一个查询重置引用关系
                }
            }
        }

        /// <summary>
        /// 访问
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        public Expression Visit(Expression node)
        {
            //Console.WriteLine($"当前访问 {node.NodeType} 类型表达式");
            switch (node)
            {
                case LambdaExpression:
                    {
                        return Visit(VisitLambda(node as LambdaExpression));
                    };
                case UnaryExpression:
                    {
                        return Visit(VisitUnary(node as UnaryExpression));
                    }
                case BinaryExpression:
                    {
                        return Visit(VisitBinary(node as BinaryExpression));
                    }
                case MethodCallExpression:
                    {
                        return Visit(VisitMethodCall(node as MethodCallExpression));
                    }
                case ConditionalExpression:
                    {
                        return Visit(VisitConditional(node as ConditionalExpression));
                    }
                case NewExpression:
                    {
                        return Visit(VisitNew(node as NewExpression));
                    }
                case MemberInitExpression:
                    {
                        return Visit(VisitMemberInit(node as MemberInitExpression));
                    }
                case NewArrayExpression:
                    {
                        return Visit(VisitNewArray(node as NewArrayExpression));
                    }
                case ListInitExpression:
                    {
                        return Visit(VisitListInit(node as ListInitExpression));
                    }
                case MemberExpression:
                    {
                        return Visit(VisitMember(node as MemberExpression));
                    }
                case ConstantExpression:
                    {
                        return VisitConstant(node as ConstantExpression);
                    };
                default: return null;
            }
        }

        /// <summary>
        /// 访问Lambda表达式
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitLambda(LambdaExpression node)
        {
            bodyExpression = node.Body;
            foreach (var item in node.Parameters)
            {
                LambdaParameterInfos.Add(new LambdaParameterInfo()
                {
                    ParameterType = item.Type,
                    ParameterIndex = ResolveSqlOptions.CustomParameterStartIndex,
                    ParameterName = item.Name,
                    ResolveName = ResolveSqlOptions.UseCustomParameter ? ResolveSqlOptions.CustomParameterName : item.Name
                });
                ResolveSqlOptions.CustomParameterStartIndex++;
            }
            return bodyExpression;
        }

        /// <summary>
        /// 访问一元表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitUnary(UnaryExpression node)
        {
            IsNot = node.NodeType == ExpressionType.Not;

            if (node.NodeType == ExpressionType.Negate)
            {
                SqlBuilder.Append('-');
            }

            if (node.NodeType == ExpressionType.ArrayLength)//获取数组长度
            {
                var array = (GetValue.Visit(node.Operand) as Array);
                SqlBuilder.Append(array.Length);
            }
            else
            {
                Visit(node.Operand);
            }

            return null;
        }

        /// <summary>
        /// 访问二元表达式
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        private Expression VisitBinary(BinaryExpression node)
        {
            #region 解析数组索引访问
            if (node.NodeType == ExpressionType.ArrayIndex)
            {
                var index = Convert.ToInt32(GetValue.Visit(node.Right));
                var array = GetValue.Visit(node.Left) as Array;
                GenerateDbParameter(array.GetValue(index));
                return null;
            }
            #endregion

            SqlBuilder.Append("( ");

            Visit(node.Left);

            #region 解析布尔类型特殊处理
            if ((ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having)
                && node.Left is not BinaryExpression && node.Left.Type.Equals(typeof(bool))
                && node.NodeType != ExpressionType.Equal
                && node.NodeType != ExpressionType.NotEqual
                && (node.Left.NodeType == ExpressionType.MemberAccess || node.Left.NodeType == ExpressionType.Constant
                || (node.Left.NodeType == ExpressionType.Not && (node.Left as UnaryExpression).Operand.NodeType != ExpressionType.Call)))
            {
                if (ResolveSqlOptions.DbType == DbType.PostgreSQL)
                {
                    if (IsNot)
                    {
                        SqlBuilder.Append(" = FALSE");
                        IsNot = false;
                    }
                    else
                    {
                        SqlBuilder.Append(" = TRUE");
                    }
                }
                else
                {
                    if (IsNot)
                    {
                        SqlBuilder.Append(" = 0");
                        IsNot = false;
                    }
                    else
                    {
                        SqlBuilder.Append(" = 1");
                    }
                }
            }
            #endregion

            var op = node.NodeType.ExpressionTypeMapping();

            #region IS NULL 和 IS NOT NULL 特殊处理
            if ((ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having) && (node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual) && node.Right.NodeType == ExpressionType.Constant)
            {
                var constantExpression = node.Right as ConstantExpression;
                if (constantExpression.Value == null)
                {
                    if (op == "=")
                    {
                        SqlBuilder.Append(" IS NULL )");
                    }
                    else if (op == "<>")
                    {
                        SqlBuilder.Append(" IS NOT NULL )");
                    }
                    return null;
                }
            }
            #endregion

            #region Sqlite字符串拼接特殊处理
            if (ResolveSqlOptions.DbType == DbType.SQLite && node.NodeType == ExpressionType.Add)
            {
                if (node.Left.Type.Equals(typeof(string)) || node.Right.Type.Equals(typeof(string)))
                {
                    op = "||";
                }
            }
            #endregion

            SqlBuilder.Append($" {op} ");

            Visit(node.Right);

            #region 解析布尔类型特殊处理
            if ((ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having)
                && node.Right is not BinaryExpression && node.Right.Type.Equals(typeof(bool))
                && node.NodeType != ExpressionType.Equal
                && node.NodeType != ExpressionType.NotEqual
                && (node.Right.NodeType == ExpressionType.MemberAccess || node.Right.NodeType == ExpressionType.Constant
                || (node.Right.NodeType == ExpressionType.Not && (node.Right as UnaryExpression).Operand.NodeType != ExpressionType.Call)))
            {
                if (ResolveSqlOptions.DbType == DbType.PostgreSQL)
                {
                    if (IsNot)
                    {
                        SqlBuilder.Append(" = FALSE");
                        IsNot = false;
                    }
                    else
                    {
                        SqlBuilder.Append(" = TRUE");
                    }
                }
                else
                {
                    if (IsNot)
                    {
                        SqlBuilder.Append(" = 0");
                        IsNot = false;
                    }
                    else
                    {
                        SqlBuilder.Append(" = 1");
                    }
                }
            }
            #endregion

            SqlBuilder.Append(" )");

            return null;
        }

        /// <summary>
        /// 访问方法表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name.Equals("get_Item") && node.Method.DeclaringType.FullName.StartsWith("System.Collections.Generic"))
            {
                var index = Convert.ToInt32(GetValue.Visit(node.Arguments[0]));
                var list = GetValue.Visit(node.Object) as IList;
                var value = list[index];
                GenerateDbParameter(value);
            }
            else if (node.Object != null && node.Object.Type.IsVariableBoundArray)//多维数组处理
            {
                var args = new List<int>();
                foreach (var item in node.Arguments)
                {
                    args.Add(Convert.ToInt32(GetValue.Visit(item)));
                }
                var array = GetValue.Visit(node.Object) as Array;
                var value = array.GetValue(args.ToArray());
                GenerateDbParameter(value);
            }
            else
            {
                if (ResolveSqlOptions.DbType.MethodMapping().ContainsKey(node.Method.Name))
                {
                    ResolveSqlOptions.DbType.MethodMapping()[node.Method.Name].Invoke(this, node, SqlBuilder);
                }
                else
                {
                    throw new NotImplementedException($"未实现 {node.Method.Name} 方法解析,可通过DbType枚举的扩展方法自定义添加.");
                }
            }
            return null;
        }

        /// <summary>
        /// 访问条件表达式树
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitConditional(ConditionalExpression node)
        {
            SqlBuilder.Append("CASE WHEN ");
            Visit(node.Test);
            if (node.Test is not BinaryExpression)
            {
                #region 解析布尔类型特殊处理
                if (node.Test.Type.Equals(typeof(bool)))
                {
                    if (ResolveSqlOptions.DbType == DbType.PostgreSQL)
                    {
                        if (IsNot)
                        {
                            SqlBuilder.Append(" = FALSE");
                            IsNot = false;
                        }
                        else
                        {
                            SqlBuilder.Append(" = TRUE");
                        }
                    }
                    else
                    {
                        if (IsNot)
                        {
                            SqlBuilder.Append(" = 0");
                            IsNot = false;
                        }
                        else
                        {
                            SqlBuilder.Append(" = 1");
                        }
                    }
                }
                #endregion
            }
            SqlBuilder.Append(" THEN ");
            Visit(node.IfTrue);
            SqlBuilder.Append(" ELSE ");
            Visit(node.IfFalse);
            SqlBuilder.Append(" END");
            return null;
        }

        /// <summary>
        /// 访问对象表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitNew(NewExpression node)
        {
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var methodCallExpression = node.Arguments[i] as MethodCallExpression;
                if (methodCallExpression != null && methodCallExpression.Method.CheckSetMemberInfos())
                {
                    SetMemberInfos.Add(new SetMemberInfo()
                    {
                        MemberInfo = node.Members[i],
                        Value = new Lazy<object>(() =>
                        {
                            GetValue.MethodCallAfter = (obj, result, exp) =>
                            {
                                if (exp.Method.Name.StartsWith("Query"))
                                {
                                    var query = result as IQuery;
                                    query.QueryBuilder.IncludeSubQuery = true;
                                    query.QueryBuilder.ParentLambdaParameterInfos = LambdaParameterInfos;
                                    if (ResolveSqlOptions.ParentLambdaParameterInfos != null && ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                                    {
                                        query.QueryBuilder.ParentLambdaParameterInfos.AddRange(ResolveSqlOptions.ParentLambdaParameterInfos);
                                    }
                                    query.QueryBuilder.ParentParameterCount = DbParameters.Count;
                                }
                            };
                            var value = GetValue.Visit(methodCallExpression);
                            if (value != null)
                            {
                                var type = value.GetType();
                                if (type.FullName.StartsWith("System.Threading.Tasks") || type.FullName.StartsWith("System.Runtime.CompilerServices.AsyncTaskMethodBuilder"))
                                {
                                    (value as dynamic).Wait();
                                }
                            }
                            return value;
                        }),
                        Index = i
                    });
                    SqlBuilder.Append($"{i} AS {ResolveSqlOptions.DbType.GetIdentifier().Insert(1, $"fast_args_index_{i}")}");
                }
                else
                {
                    if (ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewAs)
                    {
                        Visit(node.Arguments[i]);
                        SqlBuilder.Append(" AS ");
                        var name = node.Members[i].GetCustomAttribute<ColumnAttribute>(false)?.Name;
                        if (ResolveSqlOptions.IgnoreIdentifier)
                        {
                            SqlBuilder.Append(name ?? node.Members[i].Name);
                        }
                        else
                        {
                            SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetIdentifier().Insert(1, name ?? node.Members[i].Name)}");
                        }
                    }
                    else if (ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewAssignment)
                    {
                        var name = node.Members[i].GetCustomAttribute<ColumnAttribute>(false)?.Name;
                        if (ResolveSqlOptions.IgnoreIdentifier)
                        {
                            SqlBuilder.Append(name ?? node.Members[i].Name);
                        }
                        else
                        {
                            SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetIdentifier().Insert(1, name ?? node.Members[i].Name)}");
                        }
                        SqlBuilder.Append(" = ");
                        Visit(node.Arguments[i]);
                    }
                    else
                    {
                        Visit(node.Arguments[i]);
                    }
                }

                if (i + 1 < node.Arguments.Count)
                {
                    SqlBuilder.Append(',');
                }
            }
            return null;
        }

        /// <summary>
        /// 访问成员初始化表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitMemberInit(MemberInitExpression node)
        {
            for (int i = 0; i < node.Bindings.Count; i++)
            {
                if (node.Bindings[i].BindingType == MemberBindingType.Assignment)
                {
                    var memberAssignment = node.Bindings[i] as MemberAssignment;
                    var methodCallExpression = memberAssignment.Expression as MethodCallExpression;
                    if (methodCallExpression != null && methodCallExpression.Method.CheckSetMemberInfos())
                    {
                        SetMemberInfos.Add(new SetMemberInfo()
                        {
                            MemberInfo = memberAssignment.Member,
                            Value = new Lazy<object>(() =>
                            {
                                GetValue.MethodCallAfter = (obj, result, exp) =>
                                {
                                    if (exp.Method.Name.StartsWith("Query"))
                                    {
                                        var query = result as IQuery;
                                        query.QueryBuilder.IncludeSubQuery = true;
                                        query.QueryBuilder.ParentLambdaParameterInfos = LambdaParameterInfos;
                                        if (ResolveSqlOptions.ParentLambdaParameterInfos != null && ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                                        {
                                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(ResolveSqlOptions.ParentLambdaParameterInfos);
                                        }
                                        query.QueryBuilder.ParentParameterCount = DbParameters.Count;
                                    }
                                };
                                var value = GetValue.Visit(memberAssignment.Expression);
                                if (value != null)
                                {
                                    var type = value.GetType();
                                    if (type.FullName.StartsWith("System.Threading.Tasks") || type.FullName.StartsWith("System.Runtime.CompilerServices.AsyncTaskMethodBuilder"))
                                    {
                                        (value as dynamic).Wait();
                                    }
                                }
                                return value;
                            }),
                            Index = i
                        });
                        SqlBuilder.Append($"{i} AS {ResolveSqlOptions.DbType.GetIdentifier().Insert(1, $"fast_index_{i}")}");
                    }
                    else
                    {
                        if (ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewAs)
                        {
                            Visit(memberAssignment.Expression);
                            var name = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>(false)?.Name;
                            SqlBuilder.Append(" AS ");
                            if (ResolveSqlOptions.IgnoreIdentifier)
                            {
                                SqlBuilder.Append(name ?? memberAssignment.Member.Name);
                            }
                            else
                            {
                                SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetIdentifier().Insert(1, name ?? memberAssignment.Member.Name)}");
                            }
                        }
                        else if (ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewAssignment)
                        {
                            var name = memberAssignment.Member.GetCustomAttribute<ColumnAttribute>(false)?.Name;
                            if (ResolveSqlOptions.IgnoreIdentifier)
                            {
                                SqlBuilder.Append(name ?? memberAssignment.Member.Name);
                            }
                            else
                            {
                                SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetIdentifier().Insert(1, name ?? memberAssignment.Member.Name)}");
                            }
                            SqlBuilder.Append(" = ");
                            Visit(memberAssignment.Expression);
                        }
                        else
                        {
                            SqlBuilder.Append(memberAssignment.Member.Name);
                        }
                    }
                    if (i + 1 < node.Bindings.Count)
                    {
                        SqlBuilder.Append(',');
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 访问对象数组表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitNewArray(NewArrayExpression node)
        {
            for (int i = 0; i < node.Expressions.Count; i++)
            {
                Visit(node.Expressions[i]);
                if (i + 1 < node.Expressions.Count)
                {
                    SqlBuilder.Append(',');
                }
            }
            return null;
        }

        /// <summary>
        /// 访问列表初始化表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitListInit(ListInitExpression node)
        {
            if (node.CanReduce)
            {
                var blockExpression = node.Reduce() as BlockExpression;
                var expressions = blockExpression.Expressions.Skip(1).SkipLast(1).ToList();
                for (int i = 0; i < expressions.Count; i++)
                {
                    var methodCallExpression = expressions[i] as MethodCallExpression;
                    foreach (var item in methodCallExpression.Arguments)
                    {
                        Visit(item);
                    }
                    if (i + 1 < expressions.Count)
                    {
                        SqlBuilder.Append(',');
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 访问成员表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitMember(MemberExpression node)
        {
            #region 多级访问限制
            if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter && memberInfos.Count > 0)
            {
                var parameterExpression = node.Expression as ParameterExpression;
                throw new Exception($"不支持{parameterExpression.Name}.{node.Member.Name}.{memberInfos.Pop().MemberInfo.Name}多级访问.");
            }
            #endregion

            #region Datetime特殊处理
            if (node.Type.Equals(typeof(DateTime)) && node.Expression == null)
            {
                memberInfos.Push(new MemberInfoEx()
                {
                    ArrayIndex = arrayIndexs,
                    MemberInfo = node.Member
                });
                return Expression.Constant(default(DateTime));
            }
            #endregion

            if (node.Expression != null)
            {
                if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    if (!ResolveSqlOptions.IgnoreParameter)
                    {
                        var parameterExpression = node.Expression as ParameterExpression;
                        var parameterName = "";
                        if (LambdaParameterInfos.Any(a => a.ParameterName == parameterExpression.Name))
                        {
                            var lambdaParameterInfo = LambdaParameterInfos.First(f => f.ParameterName == parameterExpression.Name);
                            parameterName = ResolveSqlOptions.UseCustomParameter ? $"{lambdaParameterInfo.ResolveName}{lambdaParameterInfo.ParameterIndex}" : lambdaParameterInfo.ResolveName;
                        }
                        else if (ResolveSqlOptions.ParentLambdaParameterInfos.Any(a => a.ParameterName == parameterExpression.Name))
                        {
                            if (ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where)
                            {
                                ResolveSqlOptions.ResolveSqlType = ResolveSqlType.Join;//更改为联表条件
                            }
                            var parentLambdaParameterInfo = ResolveSqlOptions.ParentLambdaParameterInfos.First(f => f.ParameterName == parameterExpression.Name);
                            parentLambdaParameterInfo.IsUsing = true;//标记为被引用
                            parameterName = ResolveSqlOptions.UseCustomParameter ? $"{parentLambdaParameterInfo.ResolveName}{parentLambdaParameterInfo.ParameterIndex}" : parentLambdaParameterInfo.ResolveName;
                        }
                        else
                        {
                            throw new ArgumentException($"无法解析参数名称:{parameterExpression.Name},请检查作用域是否超出范围.");
                        }
                        if (ResolveSqlOptions.IgnoreIdentifier)
                        {
                            SqlBuilder.Append($"{parameterName}.");
                        }
                        else
                        {
                            SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetIdentifier().Insert(1, $"{parameterName}")}.");
                        }
                    }
                    var memberName = node.Member.Name;
                    if (!ResolveSqlOptions.IgnoreColumnAttribute)
                    {
                        var columnAttribute = node.Member.GetCustomAttribute<ColumnAttribute>(false);
                        if (columnAttribute != null && !string.IsNullOrWhiteSpace(columnAttribute.Name))
                        {
                            memberName = columnAttribute.Name;
                        }
                    }
                    if (ResolveSqlOptions.IgnoreIdentifier)
                    {
                        SqlBuilder.Append(memberName);
                    }
                    else
                    {
                        SqlBuilder.Append(ResolveSqlOptions.DbType.GetIdentifier().Insert(1, memberName));
                    }

                    #region 解析布尔类型特殊处理
                    if ((ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having)
                        && node.Type.Equals(typeof(bool)) && bodyExpression is not BinaryExpression)
                    {
                        if (ResolveSqlOptions.DbType == DbType.PostgreSQL)
                        {
                            if (IsNot)
                            {
                                SqlBuilder.Append(" = FALSE");
                                IsNot = false;
                            }
                            else
                            {
                                SqlBuilder.Append(" = TRUE");
                            }
                        }
                        else
                        {
                            if (IsNot)
                            {
                                SqlBuilder.Append(" = 0");
                                IsNot = false;
                            }
                            else
                            {
                                SqlBuilder.Append(" = 1");
                            }
                        }
                    }
                    #endregion

                }
                else if (node.Expression.NodeType == ExpressionType.MemberAccess || node.Expression.NodeType == ExpressionType.Constant)
                {
                    memberInfos.Push(new MemberInfoEx()
                    {
                        ArrayIndex = arrayIndexs,
                        MemberInfo = node.Member
                    });
                }
                else if (node.Expression.NodeType == ExpressionType.ListInit || node.Expression.NodeType == ExpressionType.NewArrayInit)
                {
                    memberInfos.Push(new MemberInfoEx()
                    {
                        ArrayIndex = arrayIndexs,
                        MemberInfo = node.Member
                    });

                    return Visit(Expression.Constant(GetValue.Visit(node.Expression)));
                }
            }

            if (arrayIndexs.Count > 0)
            {
                arrayIndexs = new Stack<int>();
            }
            return node.Expression;
        }

        /// <summary>
        /// 访问常量表达式
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        private Expression VisitConstant(ConstantExpression node)
        {
            var value = node.Value;
            if (memberInfos.Count > 0)
            {
                value = memberInfos.GetValue(value, out var memberName);//获取成员变量值
                if (value is IList)
                {
                    if (value.GetType().IsVariableBoundArray)//多维数组判断
                    {
                        return Expression.Constant(value);
                    }
                    var list = value as IList;
                    var parNames = new List<string>();
                    for (int i = 0; i < list.Count; i++)
                    {
                        var parameter = DbParameters.FirstOrDefault(f => f.ParameterName.StartsWith($"{memberName}_{i}_"));
                        if (parameter == null)
                        {
                            var newName = $"{memberName}_{i}_{ResolveSqlOptions.DbParameterStartIndex}";
                            parNames.Add(newName);

                            parameter = new FastParameter(newName, list[i]);

                            DbParameters.Add(parameter);

                            ResolveSqlOptions.DbParameterStartIndex++;
                        }
                        else
                        {
                            parNames.Add(parameter.ParameterName);
                        }
                    }
                    SqlBuilder.Append(string.Join(",", parNames.Select(s => $"{ResolveSqlOptions.DbType.GetSymbol()}{s}")));
                }
                else//普通成员变量处理
                {
                    var parameter = DbParameters.FirstOrDefault(f => f.ParameterName.StartsWith($"{memberName}_"));
                    if (parameter == null)
                    {
                        var newName = $"{memberName}_{ResolveSqlOptions.DbParameterStartIndex}";
                        parameter = new FastParameter(newName, value);
                        DbParameters.Add(parameter);

                        SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetSymbol()}{newName}");
                        ResolveSqlOptions.DbParameterStartIndex++;
                    }
                    else
                    {
                        SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetSymbol()}{parameter.ParameterName}");
                    }
                }
                memberInfos.Clear();
            }
            else
            {
                if (node.Type.Equals(typeof(bool)))
                {
                    if ((ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join || ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having)
                        && bodyExpression.NodeType == ExpressionType.Constant)
                    {
                        value = Convert.ToInt32(value);
                        value = $"1 = {value}";
                    }
                    else if (ResolveSqlOptions.DbType == DbType.PostgreSQL)
                    {
                        //PostgreSQL 特殊处理 bool转换成大写
                        SqlBuilder.Append(Convert.ToString(value).ToUpper());
                        return node;
                    }
                    else
                    {
                        value = Convert.ToInt32(value);
                    }
                }
                value = AddQuotes(node.Type, value);
                SqlBuilder.Append(Convert.ToString(value));
            }
            return null;
        }

        /// <summary>
        /// 生成数据库参数
        /// </summary>
        /// <param name="value">值</param>
        private void GenerateDbParameter(object value)
        {
            var parameterName = $"Constant_{ResolveSqlOptions.DbParameterStartIndex}";
            var parameter = new FastParameter(parameterName, value);
            DbParameters.Add(parameter);

            SqlBuilder.Append($"{ResolveSqlOptions.DbType.GetSymbol()}{parameterName}");
            ResolveSqlOptions.DbParameterStartIndex++;
        }

        /// <summary>
        /// 添加引号
        /// </summary>
        /// <param name="type">类型</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        private static object AddQuotes(Type type, object value)
        {
            if (type.IsValueType && !type.Equals(typeof(DateTime)))
            {
                return value;
            }
            return $"'{value}'";
        }
    }
}
