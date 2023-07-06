using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.Enum;
using Fast.Framework.Extensions;
using Fast.Framework.Factory;
using Fast.Framework.Implements;
using Fast.Framework.Interfaces;
using Fast.Framework.Models;

namespace Fast.Framework.Abstract
{

    /// <summary>
    /// 查询建造者抽象类
    /// </summary>
    public abstract class QueryBuilder : ISqlBuilder
    {
        /// <summary>
        /// 数据库类型
        /// </summary>
        public virtual Enum.DbType DbType { get; private set; } = Enum.DbType.SQLServer;

        /// <summary>
        /// 表达式
        /// </summary>
        public IExpressions Expressions { get; }

        /// <summary>
        /// 是否包括
        /// </summary>
        public bool IsInclude { get; set; }

        /// <summary>
        /// 包括信息
        /// </summary>
        public List<IncludeInfo> IncludeInfos { get; }

        /// <summary>
        /// 设置成员信息
        /// </summary>
        public List<SetMemberInfo> SetMemberInfos { get; }

        /// <summary>
        /// 实体信息
        /// </summary>
        public EntityInfo EntityInfo { get; set; }

        /// <summary>
        /// 是否From查询
        /// </summary>
        public bool IsFromQuery { get; set; }

        /// <summary>
        /// From查询Sql
        /// </summary>
        public string FromQuerySql { get; set; }

        /// <summary>
        /// 是否去重
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// 跳过
        /// </summary>
        public virtual int? Skip { get; set; }

        /// <summary>
        /// 取
        /// </summary>
        public virtual int? Take { get; set; }

        /// <summary>
        /// 是否第一
        /// </summary>
        public virtual bool IsFirst { get; set; }

        /// <summary>
        /// 是否联合
        /// </summary>
        public bool IsUnion { get; set; }

        /// <summary>
        /// 联合
        /// </summary>
        public string Union { get; set; }

        /// <summary>
        /// 是否分页
        /// </summary>
        public bool IsPage { get; set; }

        /// <summary>
        /// 页
        /// </summary>
        public int Page { get; set; }

        /// <summary>
        /// 页大小
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// 选择值
        /// </summary>
        public string SelectValue { get; set; }

        /// <summary>
        /// 连接
        /// </summary>
        public List<JoinInfo> Join { get; }

        /// <summary>
        /// 条件
        /// </summary>
        public List<string> Where { get; }

        /// <summary>
        /// 数据库参数
        /// </summary>
        public List<FastParameter> DbParameters { get; set; }

        /// <summary>
        /// 包含子查询
        /// </summary>
        public bool IncludeSubQuery { get; set; }

        /// <summary>
        /// 是否子查询
        /// </summary>
        public bool IsSubQuery { get; set; }

        /// <summary>
        /// 父级Lambda参数信息
        /// </summary>
        public List<LambdaParameterInfo> ParentLambdaParameterInfos { get; set; }

        /// <summary>
        /// 父级参数计数
        /// </summary>
        public int ParentParameterCount { get; set; }

        /// <summary>
        /// 分组
        /// </summary>
        public List<string> GroupBy { get; }

        /// <summary>
        /// 有
        /// </summary>
        public List<string> Having { get; }

        /// <summary>
        /// 排序
        /// </summary>
        public List<string> OrderBy { get; }

        /// <summary>
        /// 是否插入
        /// </summary>
        public bool IsInsert { get; set; }

        /// <summary>
        /// 插入表名称
        /// </summary>
        public string InsertTableName { get; set; }

        /// <summary>
        /// 插入列
        /// </summary>
        public List<string> InsertColumns { get; set; }

        /// <summary>
        /// 选择模板
        /// </summary>
        public virtual string SelectTempalte { get; set; } = "SELECT {0} FROM {1}";

        /// <summary>
        /// 第一模板
        /// </summary>
        public virtual string FirstTemplate { get; set; }

        /// <summary>
        /// 条件模板
        /// </summary>
        public virtual string WhereTemplate { get; } = "WHERE {0}";

        /// <summary>
        /// 联接模板
        /// </summary>
        public virtual string JoinTemplate { get; } = "{0} JOIN {1} {2} ON {3}";

        /// <summary>
        /// 分页模板
        /// </summary>
        public virtual string PageTempalte { get; }

        /// <summary>
        /// 分组模板
        /// </summary>
        public virtual string GroupByTemplate { get; } = "GROUP BY {0}";

        /// <summary>
        /// 作为模板
        /// </summary>
        public virtual string HavingTemplate { get; } = "HAVING {0}";

        /// <summary>
        /// 排序模板
        /// </summary>
        public virtual string OrderByTemplate { get; } = "ORDER BY {0}";

        /// <summary>
        /// 最大值模板
        /// </summary>
        public virtual string MaxTemplate { get; } = "MAX( {0} )";

        /// <summary>
        /// 最小值模板
        /// </summary>
        public virtual string MinTemplate { get; } = "MIN( {0} )";

        /// <summary>
        /// 合计模板
        /// </summary>
        public virtual string SumTemplate { get; } = "SUM( {0} )";

        /// <summary>
        /// 平均模板
        /// </summary>
        public virtual string AvgTemplate { get; } = "AVG( {0} )";

        /// <summary>
        /// 计数模板
        /// </summary>
        public virtual string CountTemplate { get; } = "COUNT( {0} )";

        /// <summary>
        /// 插入模板
        /// </summary>
        public virtual string InsertTemplate { get; } = "INSERT INTO {0} ( {1} )";

        /// <summary>
        /// 构造方法
        /// </summary>
        public QueryBuilder()
        {
            Expressions = new ExpressionProvider();
            IncludeInfos = new List<IncludeInfo>();
            SetMemberInfos = new List<SetMemberInfo>();
            EntityInfo = new EntityInfo();
            DbParameters = new List<FastParameter>();
            ParentLambdaParameterInfos = new List<LambdaParameterInfo>();
            Join = new List<JoinInfo>();
            Where = new List<string>();
            GroupBy = new List<string>();
            Having = new List<string>();
            OrderBy = new List<string>();
            InsertColumns = new List<string>();
        }

        /// <summary>
        /// 解析表达式
        /// </summary>
        public virtual void ResolveExpressions()
        {
            if (!Expressions.ResolveComplete && Expressions.ExpressionInfos.Count > 0)
            {
                foreach (var item in Expressions.ExpressionInfos)
                {
                    item.ResolveSqlOptions.IgnoreParameter = Join.Count == 0 && !IsSubQuery && !IncludeSubQuery;

                    item.ResolveSqlOptions.DbParameterStartIndex = ParentParameterCount + DbParameters.Count + 1;//数据库参数起始索引

                    item.ResolveSqlOptions.ParentLambdaParameterInfos = ParentLambdaParameterInfos;//父级参数索引

                    var result = item.Expression.ResolveSql(item.ResolveSqlOptions);

                    if (item.IsFormat)
                    {
                        result.SqlString = string.Format(item.Template, result.SqlString);
                    }

                    var usingLambdaParameterInfos = item.ResolveSqlOptions.ParentLambdaParameterInfos.Where(w => w.IsUsing).OrderBy(o => o.ParameterIndex).ToList();

                    if (IncludeSubQuery)
                    {
                        if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where)
                        {
                            item.ResolveSqlOptions.ResolveSqlType = ResolveSqlType.Join;//更改为联表条件
                        }
                        foreach (var lambdaParameterInfo in usingLambdaParameterInfos)
                        {
                            var firstJoinInfo = Join.LastOrDefault(f =>
                            f.ExpressionId == lambdaParameterInfo.ParameterType.GUID.ToString()
                            && f.EntityInfo.Alias == $"{lambdaParameterInfo.ResolveName}{lambdaParameterInfo.ParameterIndex}");

                            if (firstJoinInfo == null)
                            {
                                var entityInfo = lambdaParameterInfo.ParameterType.GetEntityInfo();
                                entityInfo.Alias = $"{lambdaParameterInfo.ResolveName}{lambdaParameterInfo.ParameterIndex}";

                                var rightJoinInfo = new JoinInfo()
                                {
                                    ExpressionId = lambdaParameterInfo.ParameterType.GUID.ToString(),
                                    EntityInfo = entityInfo,
                                    JoinType = JoinType.Right
                                };

                                if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join)
                                {
                                    rightJoinInfo.Where = result.SqlString;
                                }
                                else
                                {
                                    //可在这自动查找映射关系
                                    throw new Exception($"未指定条件无法使用上级{lambdaParameterInfo.ParameterName}参数名.");
                                }
                                Join.Add(rightJoinInfo);
                            }
                        }
                    }

                    if (IsSubQuery || IncludeSubQuery)
                    {
                        var main = result.LambdaParameterInfos.First();
                        EntityInfo.Alias = item.ResolveSqlOptions.UseCustomParameter ? $"{main.ResolveName}{main.ParameterIndex}" : main.ResolveName;
                    }

                    if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Where)
                    {
                        this.Where.Add(result.SqlString);
                    }
                    else if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Join)
                    {
                        var joinInfo = this.Join.FirstOrDefault(f => f.ExpressionId == item.Id);
                        if (joinInfo != null)
                        {
                            var main = result.LambdaParameterInfos.First();
                            var join = result.LambdaParameterInfos.Last();

                            EntityInfo.Alias = item.ResolveSqlOptions.UseCustomParameter ? $"{item.ResolveSqlOptions.CustomParameterName}{main.ParameterIndex}" : main.ParameterName;

                            joinInfo.EntityInfo.Alias = item.ResolveSqlOptions.UseCustomParameter ? $"{item.ResolveSqlOptions.CustomParameterName}{join.ParameterIndex}" : join.ParameterName;
                            joinInfo.Where = result.SqlString;
                        }
                    }
                    else if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewAs || item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.NewColumn)
                    {
                        this.SelectValue = result.SqlString;
                    }
                    else if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.GroupBy)
                    {
                        this.SelectValue = result.SqlString;
                        this.GroupBy.Add(result.SqlString);
                    }
                    else if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.Having)
                    {
                        this.Having.Add(result.SqlString);
                    }
                    else if (item.ResolveSqlOptions.ResolveSqlType == ResolveSqlType.OrderBy)
                    {
                        if (item.Addedalue == null)
                        {
                            this.OrderBy.Add(result.SqlString);
                        }
                        else
                        {
                            this.OrderBy.Add($"{result.SqlString} {item.Addedalue}");
                        }
                    }

                    this.DbParameters.AddRange(result.DbParameters);
                    this.SetMemberInfos.AddRange(result.SetMemberInfos);
                }
                this.Expressions.ResolveComplete = true;
            }
        }

        /// <summary>
        /// 获取选择值
        /// </summary>
        /// <returns></returns>
        public virtual string GetSelectValue()
        {
            var sb = new StringBuilder();
            if (IsDistinct)
            {
                sb.Append("DISTINCT ");
            }

            sb.Append(SelectValue);

            return sb.ToString();
        }

        /// <summary>
        /// 获取来自值
        /// </summary>
        /// <returns></returns>
        private string GetFromValue()
        {
            var identifier = DbType.GetIdentifier();
            if (IsUnion)
            {
                return $"( {Union} ) ";
            }
            else if (IsFromQuery)
            {
                return $"( {FromQuerySql} ) x";
            }
            else
            {
                if (Join.Count == 0 && !IsSubQuery && !IncludeSubQuery)
                {
                    return identifier.Insert(1, EntityInfo.TableName);
                }
                else
                {
                    return $"{identifier.Insert(1, EntityInfo.TableName)} {identifier.Insert(1, EntityInfo.Alias)}";
                }
            }
        }

        /// <summary>
        /// 获取跳过值
        /// </summary>
        /// <returns></returns>
        public virtual int GetSkipValue()
        {
            return Skip.Value;
        }

        /// <summary>
        /// 获取取值
        /// </summary>
        /// <returns></returns>
        public virtual int GetTakeValue()
        {
            return Take.Value;
        }

        /// <summary>
        /// 到Sql字符串
        /// </summary>
        /// <returns></returns>
        public virtual string ToSqlString()
        {
            this.ResolveExpressions();
            var sb = new StringBuilder();
            var identifier = DbType.GetIdentifier();

            //插入
            if (IsInsert)
            {
                var selectValue = string.Join(",", InsertColumns.Select(s => $"{identifier.Insert(1, s)}"));

                if (string.IsNullOrWhiteSpace(SelectValue))
                {
                    SelectValue = selectValue;
                }

                sb.AppendFormat(InsertTemplate, identifier.Insert(1, InsertTableName), selectValue);
                sb.Append("\r\n\r\n");
            }

            //子查询初始化别名
            if ((IsSubQuery || IncludeSubQuery) && Expressions.ExpressionInfos.Count == 0)
            {
                EntityInfo.Alias = $"p{ParentLambdaParameterInfos.Max(m => m.ParameterIndex) + 1}";
            }

            //初始化列
            if (string.IsNullOrWhiteSpace(SelectValue))
            {
                var columnInfos = EntityInfo.ColumnsInfos.Where(w => !w.IsNotMapped && !w.IsNavigate);
                var columnNames = columnInfos.Select(s => s.ColumnName).ToList();
                var selectValues = columnInfos.Select(s => $"{(Join.Count == 0 && !IsSubQuery && !IncludeSubQuery ? "" : $"{EntityInfo.Alias}.")}{identifier.Insert(1, s.ColumnName)}").ToList();
                if (Join.Count > 0)
                {
                    Join.ForEach(i =>
                    {
                        //层级过滤已存在的列
                        var columnInfos = i.EntityInfo.ColumnsInfos.Where(w => !w.IsNotMapped && !w.IsNavigate);
                        var filterColumnInfos = columnInfos.Where(w => !columnNames.Exists(e => e == w.ColumnName));
                        selectValues.AddRange(filterColumnInfos.Select(s => $"{$"{i.EntityInfo.Alias}."}{identifier.Insert(1, s.ColumnName)}"));
                        columnNames.AddRange(filterColumnInfos.Select(s => s.ColumnName));
                    });
                }
                SelectValue = string.Join(",", selectValues);
            }

            sb.AppendFormat(SelectTempalte, GetSelectValue(), GetFromValue());

            if (Join.Count == 0 && (Where.Count > 0 || GroupBy.Count > 0 || Having.Count > 0 || OrderBy.Count > 0))
            {
                sb.Append(' ');
            }

            if (Join.Count > 0)
            {
                sb.Append("\r\n");
                sb.Append(string.Join("\r\n", Join.Select(s =>
                {
                    if (s.IsSubQuery)
                    {
                        return string.Format("{0} JOIN ( {1} ) {2} ON {3}", s.JoinType.ToString().ToUpper(), s.SubQuerySql, identifier.Insert(1, s.EntityInfo.Alias), s.Where);
                    }
                    else
                    {
                        return string.Format(JoinTemplate, s.JoinType.ToString().ToUpper(), identifier.Insert(1, s.EntityInfo.TableName), identifier.Insert(1, s.EntityInfo.Alias), s.Where);
                    }
                })));
            }

            if (Where.Count > 0)
            {
                sb.AppendFormat($"\r\n{WhereTemplate}", string.Join(" AND ", Where));
            }
            if (GroupBy.Count > 0)
            {
                sb.AppendFormat($"\r\n{GroupByTemplate}", string.Join(" AND ", GroupBy));
            }
            if (Having.Count > 0)
            {
                sb.AppendFormat($"\r\n{HavingTemplate}", string.Join(" AND ", Having));
            }
            if (OrderBy.Count > 0)
            {
                sb.AppendFormat($"\r\n{OrderByTemplate}", string.Join(",", OrderBy));
            }
            if (IsFirst && !string.IsNullOrWhiteSpace(FirstTemplate) && Skip == null && Take == null)
            {
                sb.Append($" {FirstTemplate}");
            }
            var sql = sb.ToString();
            if (IsPage)
            {
                return string.Format(PageTempalte, sql, Page, PageSize);
            }
            else if (Skip != null && Take != null)
            {
                if (IsFirst)
                {
                    Take = 1;
                }
                return string.Format(PageTempalte, sql, GetSkipValue(), GetTakeValue());
            }
            return sql;
        }

        /// <summary>
        /// 克隆
        /// </summary>
        /// <returns></returns>
        public virtual QueryBuilder Clone()
        {
            this.ResolveExpressions();
            var queryBuilder = BuilderFactory.CreateQueryBuilder(this.DbType);
            queryBuilder.SetMemberInfos.AddRange(this.SetMemberInfos);
            queryBuilder.EntityInfo = this.EntityInfo.Clone();
            queryBuilder.IsFromQuery = this.IsFromQuery;
            queryBuilder.FromQuerySql = this.FromQuerySql;
            queryBuilder.IsDistinct = this.IsDistinct;
            queryBuilder.Take = this.Take;
            queryBuilder.Skip = this.Skip;
            queryBuilder.IsFirst = this.IsFirst;
            queryBuilder.IsUnion = this.IsUnion;
            queryBuilder.Union = this.Union;
            queryBuilder.IsPage = this.IsPage;
            queryBuilder.Page = this.Page;
            queryBuilder.PageSize = this.PageSize;
            queryBuilder.SelectValue = this.SelectValue;
            queryBuilder.Join.AddRange(this.Join);
            queryBuilder.Where.AddRange(this.Where);
            queryBuilder.DbParameters.AddRange(this.DbParameters);
            queryBuilder.ParentLambdaParameterInfos = this.ParentLambdaParameterInfos;
            queryBuilder.ParentParameterCount = this.ParentParameterCount;
            queryBuilder.IncludeSubQuery = this.IncludeSubQuery;
            queryBuilder.IsSubQuery = this.IsSubQuery;
            queryBuilder.GroupBy.AddRange(this.GroupBy);
            queryBuilder.Having.AddRange(this.Having);
            queryBuilder.OrderBy.AddRange(this.OrderBy);
            queryBuilder.IsInsert = this.IsInsert;
            queryBuilder.InsertTableName = this.InsertTableName;
            queryBuilder.InsertColumns = this.InsertColumns;
            return queryBuilder;
        }

        /// <summary>
        /// 到字符串
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ToSqlString();
        }
    }
}
