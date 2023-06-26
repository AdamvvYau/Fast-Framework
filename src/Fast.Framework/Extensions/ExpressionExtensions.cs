using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using Fast.Framework.Interfaces;
using Fast.Framework.Implements;
using Fast.Framework.Models;
using Fast.Framework.Enum;
using System.Collections;
using System.Linq;
using System.Reflection;

namespace Fast.Framework.Extensions
{

    /// <summary>
    /// 表达式扩展类
    /// </summary>
    public static class ExpressionExtensions
    {

        /// <summary>
        /// 表达式类型映射
        /// </summary>
        private static readonly Dictionary<ExpressionType, string> expressionTypeMapping;

        /// <summary>
        /// 方法映射
        /// </summary>
        private static readonly Dictionary<DbType, Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>> methodMapping;

        /// <summary>
        /// 设置成员信息方法映射
        /// </summary>
        private static readonly List<string> setMemberInfosMethodMapping;

        /// <summary>
        /// 构造方法
        /// </summary>
        static ExpressionExtensions()
        {
            expressionTypeMapping = new Dictionary<ExpressionType, string>()
            {
                { ExpressionType.Add,"+" },
                { ExpressionType.Subtract,"-" },
                { ExpressionType.Multiply,"*" },
                { ExpressionType.Divide,"/" },
                { ExpressionType.Assign,"AS" },
                { ExpressionType.And,"AND" },
                { ExpressionType.AndAlso,"AND" },
                { ExpressionType.OrElse,"OR" },
                { ExpressionType.Or,"OR" },
                { ExpressionType.Equal,"=" },
                { ExpressionType.NotEqual,"<>" },
                { ExpressionType.GreaterThan,">" },
                { ExpressionType.LessThan,"<" },
                { ExpressionType.GreaterThanOrEqual,">=" },
                { ExpressionType.LessThanOrEqual,"<=" }
            };
            methodMapping = new Dictionary<DbType, Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>>();

            setMemberInfosMethodMapping = new List<string>()
            {
                nameof(DbDataReaderExtensions.FirstBuild),
                nameof(DbDataReaderExtensions.FirstBuildAsync),
                nameof(DbDataReaderExtensions.ListBuild),
                nameof(DbDataReaderExtensions.ListBuildAsync),
                nameof(IQuery<object>.First),
                nameof(IQuery<object>.FirstAsync),
                nameof(IQuery<object>.ToArray),
                nameof(IQuery<object>.ToArrayAsync),
                nameof(IQuery<object>.ToList),
                nameof(IQuery<object>.ToListAsync),
                nameof(IQuery<object>.ToPageList),
                nameof(IQuery<object>.ToPageListAsync),
                nameof(IQuery<object>.ToDictionary),
                nameof(IQuery<object>.ToDictionaryAsync),
                nameof(IQuery<object>.ToDictionaryList),
                nameof(IQuery<object>.ToDictionaryListAsync),
                nameof(IQuery<object>.ToDictionaryPageList),
                nameof(IQuery<object>.ToDictionaryPageListAsync),
                nameof(IQuery<object>.ToDataTable),
                nameof(IQuery<object>.ToDataTableAsync),
                nameof(IQuery<object>.ToDataTablePage),
                nameof(IQuery<object>.ToDataTablePageAsync),
                nameof(IQuery<object>.ObjToJson),
                nameof(IQuery<object>.ObjToJsonAsync),
                nameof(IQuery<object>.ObjListToJson),
                nameof(IQuery<object>.ObjListToJsonAsync),
                nameof(IQuery<object>.ToJsonPageList),
                nameof(IQuery<object>.ToJsonPageListAsync),
                nameof(IQuery<object>.MaxAsync),
                nameof(IQuery<object>.MinAsync),
                nameof(IQuery<object>.CountAsync),
                nameof(IQuery<object>.SumAsync),
                nameof(IQuery<object>.AvgAsync),
                nameof(IQuery<object>.Insert),
                nameof(IQuery<object>.InsertAsync)
            };

            var sqlserverFunc = new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>();
            var mysqlFunc = new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>();
            var oracleFunc = new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>();
            var pgsqlFunc = new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>();
            var sqliteFunc = new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>();

            #region SqlServer 函数

            #region 类型转换
            sqlserverFunc.Add("ToString", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( VARCHAR(255),");

                var isDateTime = methodCall.Object != null && methodCall.Object.Type.Equals(typeof(DateTime));

                resolve.Visit(methodCall.Object);

                if (isDateTime)
                {
                    sqlBuilder.Append(',');
                    if (methodCall.Arguments.Count > 0)
                    {
                        var value = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                        if (value == "yyyy-MM-dd")
                        {
                            value = "23";
                        }
                        else if (value == "yyyy-MM-dd HH:mm:ss")
                        {
                            value = "120";
                        }
                        sqlBuilder.Append(value);
                    }
                    else
                    {
                        sqlBuilder.Append(120);
                    }
                }
                else if (methodCall.Arguments.Count > 0)
                {
                    resolve.Visit(methodCall.Arguments[0]);
                }
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToDateTime", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( DATETIME,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToDecimal", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( DECIMAL(10,6),");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToDouble", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( NUMERIC(10,6),");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToSingle", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( FLOAT,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToInt32", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( INT,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToInt64", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( BIGINT,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToBoolean", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( BIT,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToChar", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONVERT( CHAR(2),");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 聚合
            sqlserverFunc.Add("Max", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name == "Query")
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Max")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MAX");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqlserverFunc.Add("Min", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Min")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MIN");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqlserverFunc.Add("Count", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Count")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("COUNT");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqlserverFunc.Add("Sum", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Sum")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("SUM");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqlserverFunc.Add("Avg", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Avg")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("AVG");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });
            #endregion

            #region 数学
            sqlserverFunc.Add("Abs", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ABS");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Round", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROUND");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);

                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 字符串
            sqlserverFunc.Add("StartsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("+'%'");
            });

            sqlserverFunc.Add("EndsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("'%'+");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqlserverFunc.Add("Contains", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Object != null && methodCall.Object.Type.FullName.StartsWith("System.Collections.Generic"))
                {
                    resolve.Visit(methodCall.Arguments[0]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Object);
                    sqlBuilder.Append(" )");
                }
                else if (methodCall.Method.DeclaringType.Equals(typeof(Enumerable)))
                {
                    resolve.Visit(methodCall.Arguments[1]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT LIKE ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" LIKE ");
                    }
                    sqlBuilder.Append("'%'+");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append("+'%'");
                }
            });

            sqlserverFunc.Add("Substring", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("SUBSTRING");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Replace", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("REPLACE");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Len", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LEN");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("TrimStart", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("TrimEnd", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("RTRIM ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToUpper", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("UPPER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("ToLower", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LOWER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Concat", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONCAT");
                sqlBuilder.Append("( ");
                for (int i = 0; i < methodCall.Arguments.Count; i++)
                {
                    resolve.Visit(methodCall.Arguments[i]);
                    if (methodCall.Arguments.Count > 1)
                    {
                        if (i + 1 < methodCall.Arguments.Count)
                        {
                            sqlBuilder.Append(',');
                        }
                    }
                }
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Format", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Type.Equals(typeof(string)))
                {
                    var formatStr = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                    var list = new List<object>();
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        list.Add(resolve.GetValue.Visit(methodCall.Arguments[i]));
                    }
                    sqlBuilder.AppendFormat($"'{formatStr}'", list.ToArray());
                }
                else
                {
                    throw new NotImplementedException($"{methodCall.Type.Name}类型Format方法暂未实现.");
                }
            });

            #endregion

            #region 日期

            sqlserverFunc.Add("DateDiff", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEDIFF( ");
                sqlBuilder.Append(resolve.GetValue.Visit(methodCall.Arguments[0]));
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[2]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddYears", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( YEAR,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddMonths", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( MONTH,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddDays", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( DAY,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddHours", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( HOUR,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddMinutes", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( MINUTE,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddSeconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( SECOND,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("AddMilliseconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( MILLISECOND,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Year", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("YEAR");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Month", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("MONTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Day", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DAY");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            #endregion

            #region 查询
            sqlserverFunc.Add("In", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("NotIn", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("NOT IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("Any", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "Any")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        if (resolve.IsNot)
                        {
                            sqlBuilder.Append($"NOT EXISTS ( {sql} )");
                            resolve.IsNot = false;
                        }
                        else
                        {
                            sqlBuilder.Append($"EXISTS ( {sql} )");
                        }
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            sqlserverFunc.Add("First", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "First")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        sqlBuilder.Append($"( {sql} )");
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });
            #endregion

            #region 其它
            sqlserverFunc.Add("Operation", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append($" {resolve.GetValue.Visit(methodCall.Arguments[1])} ");
                resolve.Visit(methodCall.Arguments[2]);
            });

            sqlserverFunc.Add("NewGuid", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append("NEWID()");
            });

            sqlserverFunc.Add("Equals", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" = ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqlserverFunc.Add("IsNull", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ISNULL ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqlserverFunc.Add("RowNumber", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROW_NUMBER() OVER (ORDER BY  ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
                resolve.Visit(methodCall.Object);
            });

            sqlserverFunc.Add("Case", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqlserverFunc.Add("CaseWhen", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE WHEN ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqlserverFunc.Add("When", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" WHEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqlserverFunc.Add("Then", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" THEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqlserverFunc.Add("Else", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ELSE ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqlserverFunc.Add("End", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" END");
            });
            #endregion

            #endregion

            #region MySql 函数

            #region 类型转换
            mysqlFunc.Add("ToString", (resolve, methodCall, sqlBuilder) =>
            {
                var isDateTime = methodCall.Object != null && methodCall.Object.Type.Equals(typeof(DateTime));
                if (isDateTime)
                {
                    sqlBuilder.Append("DATE_FORMAT( ");

                    resolve.Visit(methodCall.Object);

                    sqlBuilder.Append(',');

                    if (methodCall.Arguments.Count > 0)
                    {
                        var value = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                        if (value == "yyyy-MM-dd")
                        {
                            value = "%Y-%m-%d";
                        }
                        else if (value == "yyyy-MM-dd HH:mm:ss")
                        {
                            value = "%Y-%m-%d %H:%i:%s";
                        }
                        sqlBuilder.Append($"'{value}'");
                    }
                    else
                    {
                        sqlBuilder.Append("'%Y-%m-%d %H:%i:%s'");
                    }
                    sqlBuilder.Append(" )");
                }
                else
                {
                    sqlBuilder.Append("CAST( ");
                    resolve.Visit(methodCall.Object);
                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    sqlBuilder.Append(" AS CHAR(510) )");
                }
            });

            mysqlFunc.Add("ToDateTime", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DATETIME )");
            });

            mysqlFunc.Add("ToDecimal", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DECIMAL(10,6) )");
            });

            mysqlFunc.Add("ToDouble", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DECIMAL(10,6) )");
            });

            mysqlFunc.Add("ToInt32", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DECIMAL(10) )");
            });

            mysqlFunc.Add("ToInt64", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DECIMAL(19) )");
            });

            mysqlFunc.Add("ToBoolean", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS UNSIGNED )");
            });

            mysqlFunc.Add("ToChar", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS CHAR(2) )");
            });
            #endregion

            #region 聚合
            mysqlFunc.Add("Max", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Max")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MAX");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            mysqlFunc.Add("Min", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Min")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MIN");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            mysqlFunc.Add("Count", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Count")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("COUNT");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            mysqlFunc.Add("Sum", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Sum")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("SUM");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            mysqlFunc.Add("Avg", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Avg")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("AVG");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });
            #endregion

            #region 数学
            mysqlFunc.Add("Abs", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ABS");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Round", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROUND");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);

                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 字符串
            mysqlFunc.Add("StartsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ,'%' )");
            });

            mysqlFunc.Add("EndsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( '%',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Contains", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Object != null && methodCall.Object.Type.FullName.StartsWith("System.Collections.Generic"))
                {
                    resolve.Visit(methodCall.Arguments[0]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Object);
                    sqlBuilder.Append(" )");
                }
                else if (methodCall.Method.DeclaringType.Equals(typeof(Enumerable)))
                {
                    resolve.Visit(methodCall.Arguments[1]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT LIKE ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" LIKE ");
                    }
                    sqlBuilder.Append("CONCAT( '%',");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(",'%' )");
                }
            });

            mysqlFunc.Add("Substring", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("SUBSTRING");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Replace", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("REPLACE");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Length", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LENGTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Trim", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("TRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("TrimStart", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("TrimEnd", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("RTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("ToUpper", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("UPPER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("ToLower", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LOWER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Concat", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONCAT");
                sqlBuilder.Append("( ");
                for (int i = 0; i < methodCall.Arguments.Count; i++)
                {
                    resolve.Visit(methodCall.Arguments[i]);
                    if (methodCall.Arguments.Count > 1)
                    {
                        if (i + 1 < methodCall.Arguments.Count)
                        {
                            sqlBuilder.Append(',');
                        }
                    }
                }
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Format", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Type.Equals(typeof(string)))
                {
                    var formatStr = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                    var list = new List<object>();
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        list.Add(resolve.GetValue.Visit(methodCall.Arguments[i]));
                    }
                    sqlBuilder.AppendFormat($"'{formatStr}'", list.ToArray());
                }
                else
                {
                    throw new NotImplementedException($"{methodCall.Type.Name}类型Format方法暂未实现.");
                }
            });

            #endregion

            #region 日期
            mysqlFunc.Add("DateDiff", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEDIFF( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[2]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("AddYears", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" YEAR )");
            });

            mysqlFunc.Add("AddMonths", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MONTH )");
            });

            mysqlFunc.Add("AddDays", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" DAY )");
            });

            mysqlFunc.Add("AddHours", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" HOUR )");
            });

            mysqlFunc.Add("AddMinutes", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MINUTE )");
            });

            mysqlFunc.Add("AddSeconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATE_ADD( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",INTERVAL ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" SECOND )");
            });

            mysqlFunc.Add("AddMilliseconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATEADD( MINUTE_SECOND,");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Year", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("YEAR");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Month", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("MONTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Day", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DAY");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            #endregion

            #region 查询
            mysqlFunc.Add("In", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("NotIn", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("NOT IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Any", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "Any")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        if (resolve.IsNot)
                        {
                            sqlBuilder.Append($"NOT EXISTS ( {sql} )");
                            resolve.IsNot = false;
                        }
                        else
                        {
                            sqlBuilder.Append($"EXISTS ( {sql} )");
                        }
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            mysqlFunc.Add("First", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "First")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        sqlBuilder.Append($"( {sql} )");
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            #endregion

            #region 其它
            mysqlFunc.Add("Operation", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append($" {resolve.GetValue.Visit(methodCall.Arguments[1])} ");
                resolve.Visit(methodCall.Arguments[2]);
            });

            mysqlFunc.Add("NewGuid", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append("UUID()");
            });

            mysqlFunc.Add("Equals", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" = ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            mysqlFunc.Add("IfNull", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("IFNULL");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            mysqlFunc.Add("Case", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            mysqlFunc.Add("CaseWhen", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE WHEN ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            mysqlFunc.Add("When", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" WHEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            mysqlFunc.Add("Then", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" THEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            mysqlFunc.Add("Else", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ELSE ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            mysqlFunc.Add("End", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" END");
            });
            #endregion

            #endregion

            #region Oracle 函数

            #region 类型转换
            oracleFunc.Add("ToString", (resolve, methodCall, sqlBuilder) =>
            {
                var isDateTime = methodCall.Object != null && methodCall.Object.Type.Equals(typeof(DateTime));

                if (isDateTime)
                {
                    sqlBuilder.Append("TO_CHAR( ");

                    resolve.Visit(methodCall.Object);

                    sqlBuilder.Append(',');

                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    else
                    {
                        sqlBuilder.Append("'yyyy-mm-dd hh24:mm:ss'");
                    }
                    sqlBuilder.Append(" )");
                }
                else
                {
                    sqlBuilder.Append("CAST( ");

                    resolve.Visit(methodCall.Object);

                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    sqlBuilder.Append(" AS ");
                    sqlBuilder.Append("VARCHAR(255)");
                    sqlBuilder.Append(" )");
                }
            });

            oracleFunc.Add("ToDateTime", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("TO_TIMESTAMP");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(",'yyyy-mm-dd hh24:mi:ss.ff' )");
            });

            oracleFunc.Add("ToDecimal", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("DECIMAL(10,6)");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToDouble", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("NUMBER(10,6)");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToSingle", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("FLOAT");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToInt32", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("INT");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToInt64", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("NUMBER");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToBoolean", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("CHAR(1)");
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToChar", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS ");
                sqlBuilder.Append("CHAR(2)");
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 聚合
            oracleFunc.Add("Max", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Max")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MAX");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            oracleFunc.Add("Min", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Min")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MIN");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            oracleFunc.Add("Count", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Count")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("COUNT");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            oracleFunc.Add("Sum", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Sum")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("SUM");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            oracleFunc.Add("Avg", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Avg")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("AVG");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });
            #endregion

            #region 数学
            oracleFunc.Add("Abs", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ABS");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Round", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROUND");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);

                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 字符串
            oracleFunc.Add("StartsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ,'%' )");
            });

            oracleFunc.Add("EndsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( '%',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Contains", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Object != null && methodCall.Object.Type.FullName.StartsWith("System.Collections.Generic"))
                {
                    resolve.Visit(methodCall.Arguments[0]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Object);
                    sqlBuilder.Append(" )");
                }
                else if (methodCall.Method.DeclaringType.Equals(typeof(Enumerable)))
                {
                    resolve.Visit(methodCall.Arguments[1]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT LIKE ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" LIKE ");
                    }
                    sqlBuilder.Append("CONCAT( '%',");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(",'%' )");
                }
            });

            oracleFunc.Add("Substring", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("SUBSTRING");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Replace", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("REPLACE");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Length", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LENGTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Trim", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("TRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("TrimStart", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("TrimEnd", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("RTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToUpper", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("UPPER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("ToLower", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LOWER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Concat", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONCAT");
                sqlBuilder.Append("( ");
                for (int i = 0; i < methodCall.Arguments.Count; i++)
                {
                    resolve.Visit(methodCall.Arguments[i]);
                    if (methodCall.Arguments.Count > 1)
                    {
                        if (i + 1 < methodCall.Arguments.Count)
                        {
                            sqlBuilder.Append(',');
                        }
                    }
                }
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Format", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Type.Equals(typeof(string)))
                {
                    var formatStr = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                    var list = new List<object>();
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        list.Add(resolve.GetValue.Visit(methodCall.Arguments[i]));
                    }
                    sqlBuilder.AppendFormat($"'{formatStr}'", list.ToArray());
                }
                else
                {
                    throw new NotImplementedException($"{methodCall.Type.Name}类型Format方法暂未实现.");
                }
            });

            #endregion

            #region 日期

            //oracleFunc.Add("AddYears", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddMonths", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddDays", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddHours", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddMinutes", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddSeconds", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            //oracleFunc.Add("AddMilliseconds", (resolve, methodCall, sqlBuilder) =>
            //{

            //});

            oracleFunc.Add("Year", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("EXTRACT( YEAR FROM ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Month", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("EXTRACT( MONTH FROM ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Day", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("EXTRACT( DAY FROM ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            #endregion

            #region 查询
            oracleFunc.Add("In", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("NotIn", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("NOT IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Any", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "Any")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        if (resolve.IsNot)
                        {
                            sqlBuilder.Append($"NOT EXISTS ( {sql} )");
                            resolve.IsNot = false;
                        }
                        else
                        {
                            sqlBuilder.Append($"EXISTS ( {sql} )");
                        }
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            oracleFunc.Add("First", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "First")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        sqlBuilder.Append($"( {sql} )");
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });
            #endregion

            #region 其它
            oracleFunc.Add("Operation", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append($" {resolve.GetValue.Visit(methodCall.Arguments[1])} ");
                resolve.Visit(methodCall.Arguments[2]);
            });

            oracleFunc.Add("Equals", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" = ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            oracleFunc.Add("Nvl", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("NVL ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            oracleFunc.Add("Case", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            oracleFunc.Add("CaseWhen", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE WHEN ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            oracleFunc.Add("When", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" WHEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            oracleFunc.Add("Then", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" THEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            oracleFunc.Add("Else", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ELSE ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            oracleFunc.Add("End", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" END");
            });
            #endregion

            #endregion

            #region PostgreSQL 函数

            #region 类型转换
            pgsqlFunc.Add("ToString", (resolve, methodCall, sqlBuilder) =>
            {
                var isDateTime = methodCall.Object != null && methodCall.Object.Type.Equals(typeof(DateTime));

                if (isDateTime)
                {
                    sqlBuilder.Append("TO_CHAR( ");

                    resolve.Visit(methodCall.Object);

                    sqlBuilder.Append(',');

                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    else
                    {
                        sqlBuilder.Append("'yyyy-mm-dd hh24:mm:ss'");
                    }
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);

                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    sqlBuilder.Append("::VARCHAR(255)");
                }
            });

            pgsqlFunc.Add("ToDateTime", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::TIMESTAMP");
            });

            pgsqlFunc.Add("ToDecimal", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::DECIMAL(10,6)");
            });

            pgsqlFunc.Add("ToDouble", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::NUMERIC(10,6)");
            });

            pgsqlFunc.Add("ToSingle", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::REAL");
            });

            pgsqlFunc.Add("ToInt32", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::INTEGER");
            });

            pgsqlFunc.Add("ToInt64", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::BIGINT");
            });

            pgsqlFunc.Add("ToBoolean", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::BOOLEAN");
            });

            pgsqlFunc.Add("ToChar", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("::CHAR(2)");
            });
            #endregion

            #region 聚合
            pgsqlFunc.Add("Max", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Max")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MAX");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            pgsqlFunc.Add("Min", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Min")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MIN");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            pgsqlFunc.Add("Count", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Count")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("COUNT");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            pgsqlFunc.Add("Sum", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Sum")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("SUM");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            pgsqlFunc.Add("Avg", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Avg")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("AVG");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });
            #endregion

            #region 数学
            pgsqlFunc.Add("Abs", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ABS");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Round", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROUND");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);

                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 字符串
            pgsqlFunc.Add("StartsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(",'%' )");
            });

            pgsqlFunc.Add("EndsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("CONCAT( '%',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Contains", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Object != null && methodCall.Object.Type.FullName.StartsWith("System.Collections.Generic"))
                {
                    resolve.Visit(methodCall.Arguments[0]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Object);
                    sqlBuilder.Append(" )");
                }
                else if (methodCall.Method.DeclaringType.Equals(typeof(Enumerable)))
                {
                    resolve.Visit(methodCall.Arguments[1]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT LIKE ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" LIKE ");
                    }
                    sqlBuilder.Append("CONCAT( '%',");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(",'%' )");
                }
            });

            pgsqlFunc.Add("Substring", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("SUBSTRING");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Replace", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("REPLACE");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Length", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LENGTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Trim", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("TRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("TrimStart", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("TrimEnd", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("RTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("ToUpper", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("UPPER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("ToLower", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LOWER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Concat", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CONCAT");
                sqlBuilder.Append("( ");
                for (int i = 0; i < methodCall.Arguments.Count; i++)
                {
                    resolve.Visit(methodCall.Arguments[i]);
                    if (methodCall.Arguments.Count > 1)
                    {
                        if (i + 1 < methodCall.Arguments.Count)
                        {
                            sqlBuilder.Append(',');
                        }
                    }
                }
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Format", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Type.Equals(typeof(string)))
                {
                    var formatStr = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                    var list = new List<object>();
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        list.Add(resolve.GetValue.Visit(methodCall.Arguments[i]));
                    }
                    sqlBuilder.AppendFormat($"'{formatStr}'", list.ToArray());
                }
                else
                {
                    throw new NotImplementedException($"{methodCall.Type.Name}类型Format方法暂未实现.");
                }
            });

            #endregion

            #region 日期

            pgsqlFunc.Add("AddYears", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" YEAR' )");
            });

            pgsqlFunc.Add("AddMonths", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MONTH' )");
            });

            pgsqlFunc.Add("AddDays", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" DAY' )");
            });

            pgsqlFunc.Add("AddHours", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" HOUR' )");
            });

            pgsqlFunc.Add("AddMinutes", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MINUTE' )");
            });

            pgsqlFunc.Add("AddSeconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" SECOND' )");
            });

            pgsqlFunc.Add("AddMilliseconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" + INTERVAL ");
                sqlBuilder.Append('\'');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MILLISECOND' )");
            });

            #endregion

            #region 查询
            pgsqlFunc.Add("In", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("NotIn", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("NOT IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            pgsqlFunc.Add("Any", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "Any")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        if (resolve.IsNot)
                        {
                            sqlBuilder.Append($"NOT EXISTS ( {sql} )");
                            resolve.IsNot = false;
                        }
                        else
                        {
                            sqlBuilder.Append($"EXISTS ( {sql} )");
                        }
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            pgsqlFunc.Add("First", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "First")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        sqlBuilder.Append($"( {sql} )");
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });
            #endregion

            #region 其它
            pgsqlFunc.Add("Operation", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append($" {resolve.GetValue.Visit(methodCall.Arguments[1])} ");
                resolve.Visit(methodCall.Arguments[2]);
            });

            pgsqlFunc.Add("Equals", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" = ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            pgsqlFunc.Add("Case", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            pgsqlFunc.Add("CaseWhen", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE WHEN ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            pgsqlFunc.Add("When", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" WHEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            pgsqlFunc.Add("Then", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" THEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            pgsqlFunc.Add("Else", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ELSE ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            pgsqlFunc.Add("End", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" END");
            });
            #endregion

            #endregion

            #region Sqlite 函数

            #region 类型转换
            sqliteFunc.Add("ToString", (resolve, methodCall, sqlBuilder) =>
            {
                var isDateTime = methodCall.Object != null && methodCall.Object.Type.Equals(typeof(DateTime));

                if (isDateTime)
                {
                    sqlBuilder.Append("STRFTIME( ");

                    resolve.Visit(methodCall.Object);

                    sqlBuilder.Append(',');

                    if (methodCall.Arguments.Count > 0)
                    {
                        var value = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                        if (value == "yyyy-MM-dd")
                        {
                            value = "%Y-%m-%d";
                        }
                        else if (value == "yyyy-MM-dd HH:mm:ss")
                        {
                            value = "%Y-%m-%d %H:%M:%S";
                        }
                        sqlBuilder.Append($"'{value}'");
                    }
                    else
                    {
                        sqlBuilder.Append("'%Y-%m-%d %H:%M:%S'");
                    }
                    sqlBuilder.Append(" )");
                }
                else
                {
                    sqlBuilder.Append("CAST( ");

                    resolve.Visit(methodCall.Object);

                    if (methodCall.Arguments.Count > 0)
                    {
                        resolve.Visit(methodCall.Arguments[0]);
                    }
                    sqlBuilder.Append(" AS TEXT )");
                }
            });

            sqliteFunc.Add("ToDateTime", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("ToDecimal", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS DECIMAL(10,6) )");
            });

            sqliteFunc.Add("ToDouble", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS NUMERIC(10,6) )");
            });

            sqliteFunc.Add("ToSingle", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS FLOAT )");
            });

            sqliteFunc.Add("ToInt32", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS INTEGER )");
            });

            sqliteFunc.Add("ToInt64", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS BIGINT )");
            });

            sqliteFunc.Add("ToBoolean", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS CHAR(1) )");
            });

            sqliteFunc.Add("ToChar", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CAST( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" AS CHAR(2) )");
            });
            #endregion

            #region 聚合
            sqliteFunc.Add("Max", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Max")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MAX");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqliteFunc.Add("Min", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Min")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("MIN");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqliteFunc.Add("Count", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Count")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("COUNT");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqliteFunc.Add("Sum", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Sum")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("SUM");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });

            sqliteFunc.Add("Avg", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Method.DeclaringType.FullName.StartsWith("Fast.Framework.Interfaces"))
                {
                    resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                    {
                        if (exp.Method.Name.StartsWith("Query"))
                        {
                            var query = result as IQuery;
                            query.QueryBuilder.IsSubQuery = true;
                            query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                            if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                            {
                                query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                            }
                            query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                        }
                        if (exp.Method.Name == "Avg")
                        {
                            var query = obj as IQuery;
                            var sql = query.QueryBuilder.ToSqlString();

                            sqlBuilder.Append($"( {sql} )");
                            resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                        }
                    };
                    resolve.GetValue.Visit(methodCall);
                }
                else
                {
                    sqlBuilder.Append("AVG");
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
            });
            #endregion

            #region 数学
            sqliteFunc.Add("Abs", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ABS");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Round", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("ROUND");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);

                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);
                }
                sqlBuilder.Append(" )");
            });
            #endregion

            #region 字符串
            sqliteFunc.Add("StartsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("||'%'");
            });

            sqliteFunc.Add("EndsWith", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Object);
                if (resolve.IsNot)
                {
                    sqlBuilder.Append(" NOT LIKE ");
                    resolve.IsNot = false;
                }
                else
                {
                    sqlBuilder.Append(" LIKE ");
                }
                sqlBuilder.Append("'%'||");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqliteFunc.Add("Contains", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Object != null && methodCall.Object.Type.FullName.StartsWith("System.Collections.Generic"))
                {
                    resolve.Visit(methodCall.Arguments[0]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Object);
                    sqlBuilder.Append(" )");
                }
                else if (methodCall.Method.DeclaringType.Equals(typeof(Enumerable)))
                {
                    resolve.Visit(methodCall.Arguments[1]);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT IN ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" IN ");
                    }
                    sqlBuilder.Append("( ");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append(" )");
                }
                else
                {
                    resolve.Visit(methodCall.Object);
                    if (resolve.IsNot)
                    {
                        sqlBuilder.Append(" NOT LIKE ");
                        resolve.IsNot = false;
                    }
                    else
                    {
                        sqlBuilder.Append(" LIKE ");
                    }
                    sqlBuilder.Append("'%'||");
                    resolve.Visit(methodCall.Arguments[0]);
                    sqlBuilder.Append("||'%'");
                }
            });

            sqliteFunc.Add("Substring", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("SUBSTRING");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                if (methodCall.Arguments.Count > 1)
                {
                    sqlBuilder.Append(',');
                    resolve.Visit(methodCall.Arguments[1]);

                }
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Replace", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("REPLACE");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Length", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LENGTH");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Trim", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("TRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("TrimStart", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("TrimEnd", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("RTRIM");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("ToUpper", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("UPPER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("ToLower", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("LOWER");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Format", (resolve, methodCall, sqlBuilder) =>
            {
                if (methodCall.Type.Equals(typeof(string)))
                {
                    var formatStr = Convert.ToString(resolve.GetValue.Visit(methodCall.Arguments[0]));
                    var list = new List<object>();
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        list.Add(resolve.GetValue.Visit(methodCall.Arguments[i]));
                    }
                    sqlBuilder.AppendFormat($"'{formatStr}'", list.ToArray());
                }
                else
                {
                    throw new NotImplementedException($"{methodCall.Type.Name}类型Format方法暂未实现.");
                }
            });

            #endregion

            #region 日期

            sqliteFunc.Add("AddYears", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" YEAR'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("AddMonths", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MONTH'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("AddDays", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" DAY'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("AddHours", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" HOUR'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("AddMinutes", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" MINUTE'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("AddSeconds", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("DATETIME");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Object);
                sqlBuilder.Append(",'");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" SECOND'");
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Year", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("STRFTIME");
                sqlBuilder.Append("( ");
                sqlBuilder.Append("'%Y',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Month", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("STRFTIME");
                sqlBuilder.Append("( ");
                sqlBuilder.Append("'%m',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Day", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("STRFTIME");
                sqlBuilder.Append("( ");
                sqlBuilder.Append("'%j',");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" )");
            });

            #endregion

            #region 查询
            sqliteFunc.Add("In", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("NotIn", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append("NOT IN ");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Any", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "Any")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        if (resolve.IsNot)
                        {
                            sqlBuilder.Append($"NOT EXISTS ( {sql} )");
                            resolve.IsNot = false;
                        }
                        else
                        {
                            sqlBuilder.Append($"EXISTS ( {sql} )");
                        }
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });

            sqliteFunc.Add("First", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.GetValue.MethodCallAfter = (obj, result, exp) =>
                {
                    if (exp.Method.Name.StartsWith("Query"))
                    {
                        var query = result as IQuery;
                        query.QueryBuilder.IsSubQuery = true;
                        query.QueryBuilder.ParentLambdaParameterInfos = resolve.LambdaParameterInfos;
                        if (resolve.ResolveSqlOptions.ParentLambdaParameterInfos != null && resolve.ResolveSqlOptions.ParentLambdaParameterInfos.Any())
                        {
                            query.QueryBuilder.ParentLambdaParameterInfos.AddRange(resolve.ResolveSqlOptions.ParentLambdaParameterInfos);
                        }
                        query.QueryBuilder.ParentParameterCount = resolve.DbParameters.Count;
                    }
                    if (exp.Method.Name == "First")
                    {
                        var query = obj as IQuery;
                        var sql = query.QueryBuilder.ToSqlString();
                        sqlBuilder.Append($"( {sql} )");
                        resolve.DbParameters.AddRange(query.QueryBuilder.DbParameters);
                    }
                };
                resolve.GetValue.Visit(methodCall);
            });
            #endregion

            #region 其它
            sqliteFunc.Add("Operation", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append($" {resolve.GetValue.Visit(methodCall.Arguments[1])} ");
                resolve.Visit(methodCall.Arguments[2]);
            });

            sqliteFunc.Add("Equals", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" = ");
                resolve.Visit(methodCall.Object);
            });

            sqliteFunc.Add("IfNull", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("IFNULL");
                sqlBuilder.Append("( ");
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(',');
                resolve.Visit(methodCall.Arguments[1]);
                sqlBuilder.Append(" )");
            });

            sqliteFunc.Add("Case", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqliteFunc.Add("CaseWhen", (resolve, methodCall, sqlBuilder) =>
            {
                sqlBuilder.Append("CASE WHEN ");
                resolve.Visit(methodCall.Arguments[0]);
            });

            sqliteFunc.Add("When", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" WHEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqliteFunc.Add("Then", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" THEN ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqliteFunc.Add("Else", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" ELSE ");
                resolve.Visit(methodCall.Arguments[1]);
            });

            sqliteFunc.Add("End", (resolve, methodCall, sqlBuilder) =>
            {
                resolve.Visit(methodCall.Arguments[0]);
                sqlBuilder.Append(" END");
            });
            #endregion

            #endregion

            methodMapping.Add(DbType.SQLServer, sqlserverFunc);
            methodMapping.Add(DbType.MySQL, mysqlFunc);
            methodMapping.Add(DbType.Oracle, oracleFunc);
            methodMapping.Add(DbType.PostgreSQL, pgsqlFunc);
            methodMapping.Add(DbType.SQLite, sqliteFunc);
        }

        /// <summary>
        /// 添加Sql函数
        /// </summary>
        /// <param name="dbType">数据库类型</param>
        /// <param name="methodName">方法名称</param>
        /// <param name="action">委托</param>
        public static void AddSqlFunc(this DbType dbType, string methodName, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder> action)
        {
            if (!methodMapping.ContainsKey(dbType))
            {
                methodMapping.Add(dbType, new Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>>());//初始化类型
            }
            dbType.MethodMapping().Add(methodName, action);
        }

        /// <summary>
        /// 表达式类型映射
        /// </summary>
        /// <param name="expressionType">表达式类型</param>
        /// <returns></returns>
        public static string ExpressionTypeMapping(this ExpressionType expressionType)
        {
            return expressionTypeMapping[expressionType];
        }

        /// <summary>
        /// 方法映射
        /// </summary>
        /// <param name="dbType">数据库类型</param>
        /// <returns></returns>
        public static Dictionary<string, Action<IExpressionResolveSql, MethodCallExpression, StringBuilder>> MethodMapping(this DbType dbType)
        {
            return methodMapping[dbType];
        }

        /// <summary>
        /// 检查设置成员信息
        /// </summary>
        /// <param name="methodInfo">方法信息</param>
        /// <returns></returns>
        public static bool CheckSetMemberInfos(this MethodInfo methodInfo)
        {
            if (!methodInfo.DeclaringType.FullName.StartsWith("Fast.Framework"))
            {
                return false;
            }
            if ((methodInfo.Name == nameof(IQuery<object>.First) || methodInfo.Name == nameof(IQuery<object>.FirstAsync)) && (!methodInfo.ReturnType.IsClass || methodInfo.ReturnType.Equals(typeof(string))))
            {
                return false;
            }
            return setMemberInfosMethodMapping.Contains(methodInfo.Name);
        }

        /// <summary>
        /// 解析Sql
        /// </summary>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public static ResolveSqlResult ResolveSql(this Expression expression, ResolveSqlOptions options)
        {
            var result = new ResolveSqlResult();
            var resolveSql = new ExpressionResolveSql(options);
            resolveSql.Visit(expression);

            result.SqlString = resolveSql.SqlBuilder.ToString();
            result.DbParameters = resolveSql.DbParameters;
            result.SetMemberInfos = resolveSql.SetMemberInfos;
            result.LambdaParameterInfos = resolveSql.LambdaParameterInfos;
            return result;
        }

    }
}

