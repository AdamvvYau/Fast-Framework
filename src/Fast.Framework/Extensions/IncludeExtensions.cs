using Fast.Framework.Implements;
using Fast.Framework.Interfaces;
using Fast.Framework.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.Enum;

namespace Fast.Framework.Extensions
{

    /// <summary>
    /// 包括扩展类
    /// </summary>
    public static class IncludeExtensions
    {

        /// <summary>
        /// 然后包括
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        /// <param name="include">包括</param>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public static IInclude<T, TProperty> ThenInclude<T, TPreviousProperty, TProperty>(this IInclude<T, IEnumerable<TPreviousProperty>> include, Expression<Func<TPreviousProperty, TProperty>> expression) where TProperty : class
        {
            var result = expression.ResolveSql(new ResolveSqlOptions()
            {
                DbType = include.Ado.DbOptions.DbType,
                ResolveSqlType = ResolveSqlType.NewColumn,
                IgnoreParameter = true,
                IgnoreIdentifier = true,
                IgnoreColumnAttribute = true
            });

            var navigate = (include.IncludeInfo.EntityInfo.ColumnsInfos.FirstOrDefault(f => f.PropertyInfo.Name == result.SqlString)?.Navigate) ?? throw new Exception($"{result.SqlString}未找到导航信息.");

            var propertyType = typeof(TProperty);

            var type = propertyType;

            if (type.IsArray)
            {
                type = type.GetElementType();
            }
            else if (type.IsGenericType)
            {
                type = type.GenericTypeArguments[0];
            }

            var queryBuilder = include.QueryBuilder.Clone();

            include.IncludeInfo.QueryBuilder.IsInclude = true;//标记为Include

            queryBuilder.EntityInfo = include.QueryBuilder.IncludeInfos.Last().EntityInfo.Clone();
            queryBuilder.EntityInfo.Alias = "Include_A";

            var includeInfo = new IncludeInfo
            {
                EntityInfo = type.GetEntityInfo()
            };

            includeInfo.EntityInfo.Alias = "Include_B";
            includeInfo.PropertyName = result.SqlString;
            includeInfo.PropertyType = propertyType;
            includeInfo.Type = type;
            includeInfo.QueryBuilder = queryBuilder;

            if (!string.IsNullOrWhiteSpace(navigate.MainName))
            {
                includeInfo.MainWhereColumn = includeInfo.QueryBuilder.EntityInfo.ColumnsInfos.FirstOrDefault(f => f.PropertyInfo.Name == navigate.MainName) ?? throw new Exception($"导航名称:{navigate.MainName}不存在.");
            }

            if (!string.IsNullOrWhiteSpace(navigate.ChildName))
            {
                includeInfo.ChildWhereColumn = includeInfo.EntityInfo.ColumnsInfos.FirstOrDefault(f => f.PropertyInfo.Name == navigate.ChildName) ?? throw new Exception($"导航名称:{navigate.ChildName}不存在.");
            }

            include.IncludeInfo.QueryBuilder.IncludeInfos.Add(includeInfo);
            return new IncludeProvider<T, TProperty>(include.Ado, include.QueryBuilder, includeInfo);
        }

        /// <summary>
        /// 选择
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        /// <param name="include">包括</param>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public static IInclude<T, TProperty> Select<T, TProperty>(this IInclude<T, TProperty> include, Expression<Func<T, TProperty, object>> expression) where TProperty : class
        {
            var queryBuilder = include.QueryBuilder.IncludeInfos.Last().QueryBuilder;

            queryBuilder.Expressions.ExpressionInfos.Add(new ExpressionInfo()
            {
                ResolveSqlOptions = new ResolveSqlOptions()
                {
                    DbType = include.Ado.DbOptions.DbType,
                    ResolveSqlType = ResolveSqlType.NewAs
                },
                Expression = expression
            });

            return include;
        }
    }
}
