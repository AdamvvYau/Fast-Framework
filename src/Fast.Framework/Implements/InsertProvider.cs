using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations.Schema;
using Fast.Framework.Abstract;
using Fast.Framework.Extensions;
using Fast.Framework.Interfaces;
using Fast.Framework.Models;
using System.Linq.Expressions;
using Fast.Framework.Enum;

namespace Fast.Framework.Implements
{

    /// <summary>
    /// 插入实现类
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class InsertProvider<T> : IInsert<T>
    {

        /// <summary>
        /// Ado
        /// </summary>
        private readonly IAdo ado;

        /// <summary>
        /// 插入建造者
        /// </summary>
        public InsertBuilder InsertBuilder { get; }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="ado">Ado</param>
        /// <param name="insertBuilder">插入建造者</param>
        public InsertProvider(IAdo ado, InsertBuilder insertBuilder)
        {
            this.ado = ado;
            this.InsertBuilder = insertBuilder;
        }

        /// <summary>
        /// 克隆
        /// </summary>
        /// <returns></returns>
        public IInsert<T> Clone()
        {
            var insertProvider = new InsertProvider<T>(ado.Clone(), InsertBuilder.Clone());
            return insertProvider;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <returns></returns>
        public IInsert<T> As(string tableName)
        {
            InsertBuilder.EntityInfo.TableName = tableName;
            return this;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public IInsert<T> As(Type type)
        {
            InsertBuilder.EntityInfo.TableName = type.GetTableName();
            return this;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <typeparam name="TType"></typeparam>
        /// <returns></returns>
        public IInsert<T> As<TType>()
        {
            return As(typeof(TType));
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IInsert<T> Columns(params string[] columns)
        {
            return Columns(columns.ToList());
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IInsert<T> Columns(List<string> columns)
        {
            var columnInfos = InsertBuilder.EntityInfo.ColumnsInfos.Where(r => columns.Exists(e => e == r.ColumnName)).ToList();

            InsertBuilder.EntityInfo.ColumnsInfos = columnInfos;

            if (InsertBuilder.IsListInsert)
            {
                InsertBuilder.IsCache = false;
            }
            else
            {
                InsertBuilder.DbParameters.RemoveAll(r => !columnInfos.Exists(e => e.ParameterName == r.ParameterName));
            }

            return this;
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="expression">列</param>
        /// <returns></returns>
        public IInsert<T> Columns(Expression<Func<T, object>> expression)
        {
            var result = expression.ResolveSql(new ResolveSqlOptions()
            {
                DbType = ado.DbOptions.DbType,
                IgnoreParameter = true,
                IgnoreIdentifier = true,
                ResolveSqlType = ResolveSqlType.NewColumn
            });
            var list = result.SqlString.Split(",");
            return Columns(list);
        }

        /// <summary>
        /// 忽略列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IInsert<T> IgnoreColumns(params string[] columns)
        {
            return IgnoreColumns(columns.ToList());
        }

        /// <summary>
        /// 忽略列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IInsert<T> IgnoreColumns(List<string> columns)
        {
            var columnInfos = InsertBuilder.EntityInfo.ColumnsInfos.Where(r => !columns.Exists(e => e == r.ColumnName)).ToList();

            InsertBuilder.EntityInfo.ColumnsInfos = columnInfos;

            if (InsertBuilder.IsListInsert)
            {
                InsertBuilder.IsCache = false;
            }
            else
            {
                InsertBuilder.DbParameters.RemoveAll(r => !columnInfos.Exists(e => e.ParameterName == r.ParameterName));
            }

            return this;
        }

        /// <summary>
        /// 忽略列
        /// </summary>
        /// <param name="expression">列</param>
        /// <returns></returns>
        public IInsert<T> IgnoreColumns(Expression<Func<T, object>> expression)
        {
            var result = expression.ResolveSql(new ResolveSqlOptions()
            {
                DbType = ado.DbOptions.DbType,
                IgnoreParameter = true,
                IgnoreIdentifier = true,
                ResolveSqlType = ResolveSqlType.NewColumn
            });
            var list = result.SqlString.Split(",");
            return IgnoreColumns(list);
        }

        /// <summary>
        /// 执行
        /// </summary>
        /// <returns></returns>
        public int Exceute()
        {
            var sql = InsertBuilder.ToSqlString();
            if (InsertBuilder.IsListInsert)
            {
                var beginTran = ado.DbTransaction == null;
                try
                {
                    var result = 0;
                    if (beginTran)
                    {
                        ado.BeginTran();
                    }
                    foreach (var item in InsertBuilder.CommandBatchs)
                    {
                        result += ado.ExecuteNonQuery(CommandType.Text, item.SqlString, ado.ConvertParameter(item.DbParameters));
                    }
                    if (beginTran)
                    {
                        ado.CommitTran();
                    }
                    return result;
                }
                catch
                {
                    if (beginTran)
                    {
                        ado.RollbackTran();
                    }
                    throw;
                }
            }
            else
            {
                return ado.ExecuteNonQuery(CommandType.Text, sql, ado.ConvertParameter(InsertBuilder.DbParameters));
            }
        }

        /// <summary>
        /// 执行异步
        /// </summary>
        /// <returns></returns>
        public async Task<int> ExceuteAsync()
        {
            var sql = InsertBuilder.ToSqlString();
            if (InsertBuilder.IsListInsert)
            {
                var beginTran = ado.MasterDbCommand.Transaction == null;
                try
                {
                    var result = 0;
                    if (beginTran)
                    {
                        await ado.BeginTranAsync();
                    }
                    foreach (var item in InsertBuilder.CommandBatchs)
                    {
                        result += await ado.ExecuteNonQueryAsync(CommandType.Text, item.SqlString, ado.ConvertParameter(item.DbParameters));
                    }
                    if (beginTran)
                    {
                        await ado.CommitTranAsync();
                    }
                    return result;
                }
                catch
                {
                    if (beginTran)
                    {
                        await ado.RollbackTranAsync();
                    }
                    throw;
                }
            }
            else
            {
                return await ado.ExecuteNonQueryAsync(CommandType.Text, sql, ado.ConvertParameter(InsertBuilder.DbParameters));
            }
        }

        /// <summary>
        /// 执行返回自增ID
        /// </summary>
        /// <returns></returns>
        public int ExceuteReturnIdentity()
        {
            if (InsertBuilder.IsListInsert)
            {
                throw new NotSupportedException("列表插入不支持该方法.");
            }
            InsertBuilder.IsReturnIdentity = true;
            var sql = InsertBuilder.ToSqlString();
            return ado.ExecuteScalar<int>(CommandType.Text, sql, ado.ConvertParameter(InsertBuilder.DbParameters));
        }

        /// <summary>
        /// 执行返回自增ID异步
        /// </summary>
        /// <returns></returns>
        public Task<int> ExceuteReturnIdentityAsync()
        {
            if (InsertBuilder.IsListInsert)
            {
                throw new NotSupportedException("列表插入不支持该方法.");
            }
            InsertBuilder.IsReturnIdentity = true;
            var sql = InsertBuilder.ToSqlString();
            return ado.ExecuteScalarAsync<int>(CommandType.Text, sql, ado.ConvertParameter(InsertBuilder.DbParameters));
        }

        /// <summary>
        /// 执行并返回计算ID
        /// </summary>
        /// <returns></returns>
        public object ExceuteReturnComputedId()
        {
            Exceute();
            return InsertBuilder.ComputedValues.FirstOrDefault();
        }

        /// <summary>
        /// 执行并返回计算ID异步
        /// </summary>
        /// <returns></returns>
        public async Task<object> ExceuteReturnComputedIdAsync()
        {
            await ExceuteAsync();
            return InsertBuilder.ComputedValues.FirstOrDefault();
        }

        /// <summary>
        /// 执行并返回计算ID
        /// </summary>
        /// <returns></returns>
        public List<object> ExceuteReturnComputedIds()
        {
            Exceute();
            return InsertBuilder.ComputedValues;
        }

        /// <summary>
        /// 执行并返回计算ID异步
        /// </summary>
        /// <returns></returns>
        public async Task<List<object>> ExceuteReturnComputedIdsAsync()
        {
            await ExceuteAsync();
            return InsertBuilder.ComputedValues;
        }

        /// <summary>
        /// 到Sql字符串
        /// </summary>
        /// <returns></returns>
        public string ToSqlString()
        {
            return this.InsertBuilder.ToSqlString();
        }

    }
}
