using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Data.Common;
using Fast.Framework.Interfaces;
using Fast.Framework.Extensions;
using Fast.Framework.Models;
using Fast.Framework.Abstract;
using Fast.Framework.Enum;


namespace Fast.Framework
{

    /// <summary>
    /// 更新实现类
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpdateProvider<T> : IUpdate<T>
    {

        /// <summary>
        /// Ado
        /// </summary>
        private readonly IAdo ado;

        /// <summary>
        /// 更新建造者
        /// </summary>
        public UpdateBuilder UpdateBuilder { get; }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="ado">ado</param>
        /// <param name="updateBuilder">更新建造者</param>
        public UpdateProvider(IAdo ado, UpdateBuilder updateBuilder)
        {
            this.ado = ado;
            this.UpdateBuilder = updateBuilder;
        }

        /// <summary>
        /// 克隆
        /// </summary>
        /// <returns></returns>
        public IUpdate<T> Clone()
        {
            var updateProvider = new UpdateProvider<T>(ado.Clone(), UpdateBuilder.Clone());
            return updateProvider;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <returns></returns>
        public IUpdate<T> As(string tableName)
        {
            UpdateBuilder.EntityInfo.TableName = tableName;
            return this;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public IUpdate<T> As(Type type)
        {
            var entityInfo = type.GetEntityInfo();
            UpdateBuilder.EntityInfo.TableName = entityInfo.TableName;
            return this;
        }

        /// <summary>
        /// 作为
        /// </summary>
        /// <typeparam name="TType"></typeparam>
        /// <returns></returns>
        public IUpdate<T> As<TType>()
        {
            return As(typeof(TType));
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> Columns(params string[] columns)
        {
            return Columns(columns.ToList());
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> Columns(List<string> columns)
        {
            var columnInfos = UpdateBuilder.EntityInfo.ColumnsInfos.Where(r => r.IsPrimaryKey || columns.Exists(e => e == r.ColumnName)).ToList();

            UpdateBuilder.EntityInfo.ColumnsInfos = columnInfos;

            if (UpdateBuilder.IsListUpdate)
            {
                UpdateBuilder.IsCache = false;
            }
            else
            {
                UpdateBuilder.DbParameters.RemoveAll(r => !columnInfos.Exists(e => e.ParameterName == r.ParameterName));
            }

            return this;
        }

        /// <summary>
        /// 列
        /// </summary>
        /// <param name="expression">列</param>
        /// <returns></returns>
        public IUpdate<T> Columns(Expression<Func<T, object>> expression)
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
        public IUpdate<T> IgnoreColumns(params string[] columns)
        {
            return IgnoreColumns(columns.ToList());
        }

        /// <summary>
        /// 忽略列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> IgnoreColumns(List<string> columns)
        {
            var columnInfos = UpdateBuilder.EntityInfo.ColumnsInfos.Where(w => w.IsPrimaryKey || !columns.Exists(e => e == w.ColumnName)).ToList();

            UpdateBuilder.EntityInfo.ColumnsInfos = columnInfos;

            if (UpdateBuilder.IsListUpdate)
            {
                UpdateBuilder.IsCache = false;
            }
            else
            {
                UpdateBuilder.DbParameters.RemoveAll(r => !columnInfos.Exists(e => e.ParameterName == r.ParameterName));
            }

            return this;
        }

        /// <summary>
        /// 忽略列
        /// </summary>
        /// <param name="expression">列</param>
        /// <returns></returns>
        public IUpdate<T> IgnoreColumns(Expression<Func<T, object>> expression)
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
        /// 设置列
        /// </summary>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public IUpdate<T> SetColumns(Expression<Func<T, object>> expression)
        {
            UpdateBuilder.Expressions.ExpressionInfos.Add(new ExpressionInfo()
            {
                ResolveSqlOptions = new ResolveSqlOptions()
                {
                    DbType = ado.DbOptions.DbType,
                    IgnoreParameter = true,
                    ResolveSqlType = ResolveSqlType.NewAssignment
                },
                Expression = expression
            });

            return this;
        }

        /// <summary>
        /// 条件列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> WhereColumns(params string[] columns)
        {
            return WhereColumns(columns.ToList());
        }

        /// <summary>
        /// 条件列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> WhereColumns(List<string> columns)
        {
            UpdateBuilder.EntityInfo.ColumnsInfos.ForEach(i =>
            {
                if (columns.Exists(e => e == i.ColumnName))
                {
                    i.IsWhere = true;
                }
            });
            return this;
        }

        /// <summary>
        /// 条件列
        /// </summary>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public IUpdate<T> WhereColumns(Expression<Func<T, object>> expression)
        {
            var result = expression.ResolveSql(new ResolveSqlOptions()
            {
                DbType = ado.DbOptions.DbType,
                ResolveSqlType = ResolveSqlType.NewColumn,
                IgnoreIdentifier = true,
                IgnoreParameter = true
            });
            var columns = result.SqlString.Split(",").ToList();
            return WhereColumns(columns);
        }

        /// <summary>
        /// 版本列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> VersionColumns(params string[] columns)
        {
            return VersionColumns(columns.ToList());
        }

        /// <summary>
        /// 版本列
        /// </summary>
        /// <param name="columns">列</param>
        /// <returns></returns>
        public IUpdate<T> VersionColumns(List<string> columns)
        {
            if (UpdateBuilder.IsListUpdate)
            {
                throw new NotSupportedException("列表更新不支持该方法.");
            }
            var columnInfos = UpdateBuilder.EntityInfo.ColumnsInfos.Where(f => columns.Exists(e => e == f.ColumnName));
            foreach (var item in columnInfos)
            {
                item.IsVersion = true;
            }
            return this;
        }

        /// <summary>
        /// 版本列
        /// </summary>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public IUpdate<T> VersionColumns(Expression<Func<T, object>> expression)
        {
            var result = expression.ResolveSql(new ResolveSqlOptions()
            {
                DbType = ado.DbOptions.DbType,
                ResolveSqlType = ResolveSqlType.NewColumn,
                IgnoreIdentifier = true,
                IgnoreParameter = true
            });
            var columns = result.SqlString.Split(",").ToList();
            return VersionColumns(columns);
        }

        /// <summary>
        /// 条件
        /// </summary>
        /// <param name="columnName">列名称</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        public IUpdate<T> Where(string columnName, object value)
        {
            if (UpdateBuilder.DbParameters.Any(a => a.ParameterName == columnName))
            {
                throw new Exception($"列名称{columnName}已存在条件,不允许重复添加.");
            }
            var whereStr = $"{ado.DbOptions.DbType.GetIdentifier().Insert(1, columnName)} = {ado.DbOptions.DbType.GetSymbol()}{columnName}";
            UpdateBuilder.Where.Add(whereStr);
            UpdateBuilder.DbParameters.Add(new FastParameter(columnName, value));
            return this;
        }

        /// <summary>
        /// 条件
        /// </summary>
        /// <param name="whereColumns">条件列</param>
        /// <returns></returns>
        public IUpdate<T> Where(Dictionary<string, object> whereColumns)
        {
            var sqlParameter = UpdateBuilder.DbParameters.FirstOrDefault(f => whereColumns.ContainsKey(f.ParameterName));
            if (sqlParameter != null)
            {
                throw new Exception($"列名称{sqlParameter.ParameterName}已存在条件,不允许重复添加.");
            }
            var whereStr = whereColumns.Keys.Select(s => $"{ado.DbOptions.DbType.GetIdentifier().Insert(1, s)} = {ado.DbOptions.DbType.GetSymbol()}{s}");
            UpdateBuilder.Where.Add(string.Join(" AND ", whereStr));
            UpdateBuilder.DbParameters.AddRange(whereColumns.Select(s => new FastParameter(s.Key, s.Value)));
            return this;
        }

        /// <summary>
        /// 条件
        /// </summary>
        /// <param name="whereStr">条件字符串</param>
        /// <returns></returns>
        public IUpdate<T> Where(string whereStr)
        {
            UpdateBuilder.Where.Add(whereStr);
            return this;
        }

        /// <summary>
        /// 条件
        /// </summary>
        /// <param name="obj">对象</param>
        /// <returns></returns>
        public IUpdate<T> Where(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }
            var type = obj.GetType();
            var entityInfo = type.GetEntityInfo();

            if (entityInfo.ColumnsInfos.Count == 0)
            {
                throw new Exception("未获取到属性.");
            }

            var dbParameters = entityInfo.ColumnsInfos.GenerateDbParameters(obj);
            var whereList = entityInfo.ColumnsInfos.Select(s => $"{ado.DbOptions.DbType.GetIdentifier().Insert(1, s.ColumnName)} = {ado.DbOptions.DbType.GetSymbol()}{s.ParameterName}");

            UpdateBuilder.Where.Add(string.Join(" AND ", whereList));
            UpdateBuilder.DbParameters.AddRange(dbParameters);
            return this;
        }

        /// <summary>
        /// 条件
        /// </summary>
        /// <param name="expression">表达式</param>
        /// <returns></returns>
        public IUpdate<T> Where(Expression<Func<T, bool>> expression)
        {
            if (UpdateBuilder.IsListUpdate)
            {
                UpdateBuilder.EntityInfo.Alias = expression.Parameters[0].Name;
            }
            UpdateBuilder.Expressions.ExpressionInfos.Add(new ExpressionInfo()
            {
                ResolveSqlOptions = new ResolveSqlOptions()
                {
                    DbType = ado.DbOptions.DbType,
                    ResolveSqlType = ResolveSqlType.Where
                },
                Expression = expression
            });
            return this;
        }

        /// <summary>
        /// 执行
        /// </summary>
        /// <returns></returns>
        public int Exceute()
        {
            var sql = UpdateBuilder.ToSqlString();
            if (UpdateBuilder.IsListUpdate)
            {
                var beginTran = ado.DbTransaction == null;
                try
                {
                    var result = 0;
                    if (beginTran)
                    {
                        ado.BeginTran();
                    }
                    foreach (var item in UpdateBuilder.CommandBatchs)
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
                return ado.ExecuteNonQuery(CommandType.Text, sql, ado.ConvertParameter(UpdateBuilder.DbParameters));
            }
        }

        /// <summary>
        /// 执行异步
        /// </summary>
        /// <returns></returns>
        public async Task<int> ExceuteAsync()
        {
            var sql = UpdateBuilder.ToSqlString();
            if (UpdateBuilder.IsListUpdate)
            {
                var beginTran = ado.MasterDbCommand.Transaction == null;
                try
                {
                    var result = 0;
                    if (beginTran)
                    {
                        await ado.BeginTranAsync();
                    }
                    foreach (var item in UpdateBuilder.CommandBatchs)
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
                return await ado.ExecuteNonQueryAsync(CommandType.Text, sql, ado.ConvertParameter(UpdateBuilder.DbParameters));
            }
        }

        /// <summary>
        /// 执行乐观锁前
        /// </summary>
        private void ExceuteWithOptLockBefore()
        {
            if (UpdateBuilder.IsListUpdate)
            {
                throw new NotSupportedException("列表更新不支持该方法.");
            }
            var columnsInfos = UpdateBuilder.EntityInfo.ColumnsInfos.Where(f => f.IsVersion).ToList();
            if (columnsInfos.Count == 0)
            {
                throw new Exception("未找到版本字段,实体对象请用OptLockAttribute标记属性,非实体对象使用VersionColumns方法指定版本列.");
            }

            UpdateBuilder.IsOptLock = true;

            var parameterIndex = 1;
            foreach (var item in columnsInfos)
            {
                var parameterName = $"{item.PropertyInfo.Name}_OptLock_{parameterIndex}";

                var newVersion = item.Clone();
                newVersion.ParameterName = parameterName;
                item.IsWhere = true;
                UpdateBuilder.EntityInfo.ColumnsInfos.Add(newVersion);

                var newValue = item.PropertyInfo.PropertyType.GetVersionId();

                var newParameter = new FastParameter(parameterName, newValue);
                UpdateBuilder.DbParameters.Add(newParameter);

                parameterIndex++;
            }
        }

        /// <summary>
        /// 执行乐观锁
        /// </summary>
        /// <param name="isVersionValidation">是否版本验证</param>
        /// <returns></returns>
        public int ExceuteWithOptLock(bool isVersionValidation = false)
        {
            this.ExceuteWithOptLockBefore();
            var result = this.Exceute();
            if (result == 0 && isVersionValidation)
            {
                throw new Exception("更新失败,版本已变更.");
            }
            return result;
        }

        /// <summary>
        /// 执行乐观锁异步
        /// </summary>
        /// <param name="isVersionValidation">是否版本验证</param>
        /// <returns></returns>
        public async Task<int> ExceuteWithOptLockAsync(bool isVersionValidation)
        {
            this.ExceuteWithOptLockBefore();
            var result = await this.ExceuteAsync();
            if (result == 0 && isVersionValidation)
            {
                throw new Exception("更新失败,版本已变更.");
            }
            return result;
        }

        /// <summary>
        /// 到Sql字符串
        /// </summary>
        /// <returns></returns>
        public string ToSqlString()
        {
            return this.UpdateBuilder.ToSqlString();
        }

    }
}

