using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Reflection;
using System.Collections;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Fast.Framework.CustomAttribute;
using Fast.Framework.Interfaces;
using Fast.Framework.Extensions;
using Fast.Framework.Aop;
using Fast.Framework.Models;
using Fast.Framework.Utils;
using Fast.Framework.Factory;
using Fast.Framework.Enum;
using Fast.Framework.Snowflake;



namespace Fast.Framework.Implements
{

    /// <summary>
    /// 数据库上下文实现类
    /// </summary>
    public class DbContext : IDbContext
    {
        /// <summary>
        /// 上下文ID
        /// </summary>
        public Guid ContextId { get; }

        /// <summary>
        /// 数据库选项
        /// </summary>
        public List<DbOptions> DbOptions { get; }

        /// <summary>
        /// Ado对象
        /// </summary>
        private IAdo ado;

        /// <summary>
        /// ado锁
        /// </summary>
        private readonly object ado_lock = new object();

        /// <summary>
        /// Ado属性
        /// </summary>
        public IAdo Ado
        {
            get
            {
                if (ado == null)
                {
                    lock (ado_lock)
                    {
                        if (ado == null)
                        {
                            var option = DbOptions.FirstOrDefault(f => f.IsDefault, DbOptions[0]);
                            ado = GetAdo(option.DbId);
                        }
                    }
                }
                return ado;
            }
            private set
            {
                ado = value;
            }
        }

        /// <summary>
        /// Aop
        /// </summary>
        public IAop Aop { get; }

        /// <summary>
        /// ado缓存
        /// </summary>
        private readonly ConcurrentDictionary<string, Lazy<IAdo>> adoCache;

        /// <summary>
        /// 是否事务
        /// </summary>
        private bool isTran;

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="options">选项</param>
        public DbContext(IOptionsSnapshot<List<DbOptions>> options) : this(options.Value)
        {
        }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="options">选项</param>
        public DbContext(List<DbOptions> options)
        {
            if (options == null || options.Count == 0)
            {
                throw new ArgumentException($"{nameof(options)}不包含任何元素.");
            }

            var list = options.GroupBy(g => g.DbId).Where(a => a.Count() > 1);

            if (list.Any())
            {
                throw new Exception($"数据库ID {string.Join(",", list.Select(s => s.Key))} 重复.");
            }

            adoCache = new ConcurrentDictionary<string, Lazy<IAdo>>();

            ContextId = Guid.NewGuid();
            DbOptions = options;

            Aop = new AopProvider();
        }

        /// <summary>
        /// 克隆
        /// </summary>
        /// <returns></returns>
        public virtual IDbContext Clone()
        {
            var dbContext = new DbContext(DbOptions);
            dbContext.Aop.DbLog = this.Aop.DbLog;
            dbContext.isTran = this.isTran;
            return dbContext;
        }

        #region 私有方法
        /// <summary>
        /// Ado循环
        /// </summary>
        /// <param name="action">委托</param>
        private void AdoFeach(Action<IAdo> action)
        {
            foreach (var item in adoCache)
            {
                action.Invoke(item.Value.Value);
            }
        }

        /// <summary>
        /// Ado循环异步
        /// </summary>
        /// <param name="action">委托</param>
        private async Task AdoFeachAsync(Func<IAdo, Task> action)
        {
            foreach (var item in adoCache)
            {
                await action.Invoke(item.Value.Value);
            }
        }
        #endregion

        #region 多租户接口实现

        /// <summary>
        /// 获取Ado
        /// </summary>
        /// <param name="dbId">数据库ID</param>
        /// <returns></returns>
        public IAdo GetAdo(string dbId)
        {
            return adoCache.GetOrAdd(dbId, k => new Lazy<IAdo>(() =>
            {
                var option = DbOptions.FirstOrDefault(f => f.DbId == dbId);
                if (option == null)
                {
                    throw new Exception($"DbId {dbId} 不存在.");
                }
                var adoProvider = ProviderFactory.CreateAdoProvider(option);
                if (isTran)
                {
                    adoProvider.BeginTran();
                }
                if (Aop.DbLog != null)
                {
                    adoProvider = DispatchProxyHelper.Create(adoProvider, new AdoIntercept(Aop));
                }
                return adoProvider;
            })).Value;
        }

        /// <summary>
        /// 获取Ado含属性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IAdo GetAdoWithAttr<T>() where T : class, new()
        {
            var entityInfo = typeof(T).GetEntityInfo();
            if (string.IsNullOrWhiteSpace(entityInfo.TenantId))
            {
                throw new Exception("未获取到TenantId.");
            }
            return GetAdo(entityInfo.TenantId);
        }

        /// <summary>
        /// 改变数据库
        /// </summary>
        /// <param name="dbId">数据库ID</param>
        /// <returns></returns>
        public void ChangeDb(string dbId)
        {
            if (Ado.DbOptions.DbId != dbId)
            {
                Ado = GetAdo(dbId);
            }
        }

        /// <summary>
        /// 开启事务
        /// </summary>
        public void BeginTran()
        {
            isTran = true;
            AdoFeach(ado =>
           {
               try
               {
                   ado.BeginTran();
               }
               catch
               {
                   try
                   {
                       Retry.Execute(() =>
                       {
                           ado.BeginTran();
                       }, TimeSpan.FromSeconds(3));
                   }
                   catch
                   {
                       throw;
                   }
               }
           });
        }

        /// <summary>
        /// 开启事务异步
        /// </summary>
        public Task BeginTranAsync()
        {
            isTran = true;
            return AdoFeachAsync(async ado =>
            {
                try
                {
                    await ado.BeginTranAsync();
                }
                catch
                {
                    try
                    {
                        await Retry.Execute(async () =>
                        {
                            await ado.BeginTranAsync();
                        }, TimeSpan.FromSeconds(3));
                    }
                    catch
                    {
                        throw;
                    }
                }
            });
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTran()
        {
            isTran = false;
            AdoFeach(ado =>
           {
               try
               {
                   ado.CommitTran();
               }
               catch
               {
                   try
                   {
                       Retry.Execute(() =>
                       {
                           ado.CommitTran();
                       }, TimeSpan.FromSeconds(3));
                   }
                   catch
                   {
                       throw;
                   }
               }
           });
        }

        /// <summary>
        /// 提交事务异步
        /// </summary>
        public Task CommitTranAsync()
        {
            isTran = false;
            return AdoFeachAsync(async ado =>
            {
                try
                {
                    await ado.CommitTranAsync();
                }
                catch
                {
                    try
                    {
                        await Retry.Execute(async () =>
                        {
                            await ado.CommitTranAsync();
                        }, TimeSpan.FromSeconds(3));
                    }
                    catch
                    {
                        throw;
                    }
                }
            });
        }

        /// <summary>
        /// 回滚事务
        /// </summary>
        /// <returns></returns>
        public void RollbackTran()
        {
            isTran = false;
            AdoFeach(ado =>
           {
               try
               {
                   ado.RollbackTran();
               }
               catch
               {
                   Retry.Execute(() =>
                   {
                       ado.RollbackTran();
                   }, TimeSpan.FromSeconds(3));
               }
           });
        }

        /// <summary>
        /// 回滚事务异步
        /// </summary>
        /// <returns></returns>
        public Task RollbackTranAsync()
        {
            isTran = false;
            return AdoFeachAsync(async ado =>
            {
                try
                {
                    await ado.RollbackTranAsync();
                }
                catch
                {
                    await Retry.Execute(async () =>
                    {
                        await ado.RollbackTranAsync();
                    }, TimeSpan.FromSeconds(3));
                }
            });
        }
        #endregion

        #region 增 删 改

        /// <summary>
        /// 插入
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IInsert<T> Insert<T>(T entity, IAdo ado) where T : class
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            var type = entity.GetType();
            var insertBuilder = BuilderFactory.CreateInsertBuilder(ado.DbOptions.DbType);

            if (type.Name.StartsWith("Dictionary`"))
            {
                var dictionary = entity as Dictionary<string, object>;
                if (dictionary == null)
                {
                    throw new Exception("字典插入请使用Dictionary<string, object>类型.");
                }
                insertBuilder.EntityInfo.ColumnsInfos.AddRange(dictionary.Select(s =>
                new ColumnInfo()
                {
                    ColumnName = s.Key,
                    ParameterName = s.Key
                }));
                insertBuilder.DbParameters.AddRange(dictionary.Select(s =>
                {
                    var value = s.Value;
                    if (value != null)
                    {
                        var type = value.GetType();
                        if (type.IsClass && !type.Equals(typeof(string)))
                        {
                            value = Json.Serialize(value);
                        }
                    }
                    var parameter = new FastParameter(s.Key, value);
                    return parameter;
                }));
            }
            else
            {
                insertBuilder.EntityInfo = type.GetEntityInfo();
                insertBuilder.DbParameters = insertBuilder.EntityInfo.ColumnsInfos.GenerateDbParameters(entity, c => c.DatabaseGeneratedOption != DatabaseGeneratedOption.Identity && !c.IsNotMapped);
                insertBuilder.ComputedValues.AddRange(insertBuilder.EntityInfo.ColumnsInfos.ComputedValues(insertBuilder.DbParameters));
            }

            insertBuilder.EntityInfo.TargetObj = entity;

            var insertProvider = new InsertProvider<T>(ado, insertBuilder);
            return insertProvider;
        }

        /// <summary>
        /// 插入
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entitys">实体</param>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IInsert<T> Insert<T>(List<T> entitys, IAdo ado) where T : class
        {
            if (entitys == null)
            {
                throw new ArgumentNullException(nameof(entitys));
            }
            if (entitys.Count == 0)
            {
                throw new ArgumentException($"{nameof(entitys)}元素不能为0.");
            }

            var type = entitys[0].GetType();
            var insertBuilder = BuilderFactory.CreateInsertBuilder(ado.DbOptions.DbType);

            if (type.Name.StartsWith("Dictionary`"))
            {
                var dictionary = entitys[0] as Dictionary<string, object>;
                if (dictionary == null)
                {
                    throw new Exception("字典插入请使用Dictionary<string, object>类型.");
                }
                insertBuilder.EntityInfo.ColumnsInfos.AddRange(dictionary.Select(s =>
                {
                    var obj = new ColumnInfo()
                    {
                        ColumnName = s.Key
                    };
                    if (s.Value != null)
                    {
                        var type = s.Value.GetType();
                        if (type.IsClass && !type.Equals(typeof(string)))
                        {
                            obj.IsJson = true;
                        }
                    }
                    return obj;
                }));
                insertBuilder.IsDictionaryList = true;
            }
            else
            {
                insertBuilder.EntityInfo = type.GetEntityInfo();
            }

            insertBuilder.EntityInfo.TargetObj = entitys;
            insertBuilder.IsListInsert = true;

            var insertProvider = new InsertProvider<T>(ado, insertBuilder);
            return insertProvider;
        }

        /// <summary>
        /// 插入
        /// </summary>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IInsert<T> Insert<T>(T entity) where T : class
        {
            return Insert(entity, Ado);
        }

        /// <summary>
        /// 插入
        /// </summary>
        /// <param name="entitys">实体</param>
        /// <returns></returns>
        public IInsert<T> Insert<T>(List<T> entitys) where T : class
        {
            return Insert(entitys, Ado);
        }

        /// <summary>
        /// 插入
        /// </summary>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IInsert<T> InsertWithAttr<T>(T entity) where T : class, new()
        {
            return Insert(entity, GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 插入
        /// </summary>
        /// <param name="entitys">实体</param>
        /// <returns></returns>
        public IInsert<T> InsertWithAttr<T>(List<T> entitys) where T : class, new()
        {
            return Insert(entitys, GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IDelete<T> Delete<T>(IAdo ado) where T : class
        {
            var type = typeof(T);
            var deleteBuilder = BuilderFactory.CreateDeleteBuilder(ado.DbOptions.DbType);

            var entityInfo = type.GetEntityInfo();
            deleteBuilder.EntityInfo = entityInfo;

            var deleteProvider = new DeleteProvider<T>(ado, deleteBuilder);
            return deleteProvider;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IDelete<T> Delete<T>(T entity, IAdo ado) where T : class
        {
            var type = entity.GetType();
            var deleteBuilder = BuilderFactory.CreateDeleteBuilder(ado.DbOptions.DbType);

            var entityInfo = type.GetEntityInfo();
            entityInfo.TargetObj = entity;

            if (!entityInfo.ColumnsInfos.Any(a => a.IsPrimaryKey))
            {
                throw new ArgumentNullException(nameof(entity), "未获取到标记KeyAuttribute特性属性.");
            }

            deleteBuilder.EntityInfo = entityInfo;
            var primary = entityInfo.ColumnsInfos.First(f => f.IsPrimaryKey);
            deleteBuilder.Where.Add($"{ado.DbOptions.DbType.GetIdentifier().Insert(1, primary.ColumnName)} = {ado.DbOptions.DbType.GetSymbol()}{primary.ColumnName}");
            deleteBuilder.DbParameters.Add(new FastParameter(primary.ColumnName, primary.PropertyInfo.GetValue(entity)));
            var deleteProvider = new DeleteProvider<T>(ado, deleteBuilder);
            return deleteProvider;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IDelete<T> Delete<T>() where T : class
        {
            return Delete<T>(Ado);
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IDelete<T> Delete<T>(T entity) where T : class
        {
            return Delete(entity, Ado);
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IDelete<T> DeleteWithAttr<T>() where T : class, new()
        {
            return Delete<T>(GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IDelete<T> DeleteWithAttr<T>(T entity) where T : class, new()
        {
            return Delete(entity, GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IUpdate<T> Update<T>(IAdo ado) where T : class
        {
            var type = typeof(T);
            var updateBuilder = BuilderFactory.CreateUpdateBuilder(ado.DbOptions.DbType);
            updateBuilder.EntityInfo = type.GetEntityInfo();

            var updateProvider = new UpdateProvider<T>(ado, updateBuilder);
            return updateProvider;
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IUpdate<T> Update<T>(T entity, IAdo ado) where T : class
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            var type = entity.GetType();
            var updateBuilder = BuilderFactory.CreateUpdateBuilder(ado.DbOptions.DbType);

            if (type.Name.StartsWith("Dictionary`"))
            {
                var dictionary = entity as Dictionary<string, object>;
                if (dictionary == null)
                {
                    throw new Exception("字典更新请使用Dictionary<string, object>类型.");
                }
                updateBuilder.EntityInfo.ColumnsInfos.AddRange(dictionary.Select(s =>
                new ColumnInfo()
                {
                    ColumnName = s.Key,
                    ParameterName = s.Key
                }));
                updateBuilder.DbParameters.AddRange(dictionary.Select(s =>
                {
                    var value = s.Value;
                    if (value != null)
                    {
                        var type = value.GetType();
                        if (type.IsClass && !type.Equals(typeof(string)))
                        {
                            value = Json.Serialize(value);
                        }
                    }
                    var parameter = new FastParameter(s.Key, value);
                    return parameter;
                }));
            }
            else
            {
                updateBuilder.EntityInfo = type.GetEntityInfo();
                updateBuilder.DbParameters = updateBuilder.EntityInfo.ColumnsInfos.GenerateDbParameters(entity, c => !c.IsNotMapped);
            }

            updateBuilder.EntityInfo.TargetObj = entity;

            var updateProvider = new UpdateProvider<T>(ado, updateBuilder);
            return updateProvider;
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entitys">实体</param>
        /// <param name="ado">Ado</param>
        /// <returns></returns>
        private static IUpdate<T> Update<T>(List<T> entitys, IAdo ado) where T : class
        {
            if (entitys == null)
            {
                throw new ArgumentNullException(nameof(entitys));
            }
            if (entitys.Count == 0)
            {
                throw new ArgumentException($"{nameof(entitys)}元素不能为0.");
            }

            var type = entitys[0].GetType();
            var updateBuilder = BuilderFactory.CreateUpdateBuilder(ado.DbOptions.DbType);

            if (type.Name.StartsWith("Dictionary`"))
            {
                var dictionary = entitys[0] as Dictionary<string, object>;
                if (dictionary == null)
                {
                    throw new Exception("字典更新请使用Dictionary<string, object>类型.");
                }
                updateBuilder.EntityInfo.ColumnsInfos.AddRange(dictionary.Select(s =>
                {
                    var obj = new ColumnInfo()
                    {
                        ColumnName = s.Key
                    };
                    if (s.Value != null)
                    {
                        var type = s.Value.GetType();
                        if (type.IsClass && !type.Equals(typeof(string)))
                        {
                            obj.IsJson = true;
                        }
                    }
                    return obj;
                }));
                updateBuilder.IsDictionaryList = true;
            }
            else
            {
                updateBuilder.EntityInfo = type.GetEntityInfo();
            }

            updateBuilder.EntityInfo.TargetObj = entitys;
            updateBuilder.IsListUpdate = true;

            var updateProvider = new UpdateProvider<T>(ado, updateBuilder);
            return updateProvider;
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IUpdate<T> Update<T>() where T : class
        {
            return Update<T>(Ado);
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IUpdate<T> Update<T>(T entity) where T : class
        {
            return Update(entity, Ado);
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entitys">实体</param>
        /// <returns></returns>
        public IUpdate<T> Update<T>(List<T> entitys) where T : class
        {
            return Update(entitys, Ado);
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IUpdate<T> UpdateWithAttr<T>() where T : class, new()
        {
            return Update<T>(GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">实体</param>
        /// <returns></returns>
        public IUpdate<T> UpdateWithAttr<T>(T entity) where T : class, new()
        {
            return Update(entity, GetAdoWithAttr<T>());
        }

        /// <summary>
        /// 更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entitys">实体</param>
        /// <returns></returns>
        public IUpdate<T> UpdateWithAttr<T>(List<T> entitys) where T : class, new()
        {
            return Update(entitys, GetAdoWithAttr<T>());
        }
        #endregion

        #region 查询

        /// <summary>
        /// 查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ado">Ado</param>
        /// <param name="subQuery">子查询</param>
        /// <returns></returns>
        private static IQuery<T> Query<T>(IAdo ado, IQuery<T> subQuery = null) where T : class
        {
            var type = typeof(T);
            var queryBuilder = BuilderFactory.CreateQueryBuilder(ado.DbOptions.DbType);

            var entityInfo = type.GetEntityInfo();
            queryBuilder.EntityInfo = entityInfo;

            if (subQuery != null)
            {
                queryBuilder.IsSubQuery = true;
                queryBuilder.SubQuerySql = subQuery.ToSqlString();
                queryBuilder.DbParameters.AddRange(subQuery.QueryBuilder.DbParameters);
                queryBuilder.SelectValue = "*";
            }

            return new QueryProvider<T>(ado, queryBuilder);
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IQuery<T> Query<T>() where T : class
        {
            return Query<T>(Ado);
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="subQuery">子查询</param>
        /// <returns></returns>
        public IQuery<T> Query<T>(IQuery<T> subQuery) where T : class
        {
            return Query(Ado, subQuery);
        }

        /// <summary>
        /// 联合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="isAll">是否全</param>
        /// <param name="querys">查询集合</param>
        /// <returns></returns>
        private IQuery<T> Union<T>(bool isAll, List<IQuery<T>> querys)
        {
            if (querys.Count < 2)
            {
                throw new Exception($"{nameof(querys)} 元素个数必须大于或等于2.");
            }
            var queryBuilder = BuilderFactory.CreateQueryBuilder(Ado.DbOptions.DbType);
            var sqlList = new List<string>();
            foreach (var item in querys)
            {
                sqlList.Add(item.QueryBuilder.ToSqlString());
                queryBuilder.DbParameters.AddRange(item.QueryBuilder.DbParameters.Where(w => !queryBuilder.DbParameters.Exists(e => e.ParameterName == w.ParameterName)));
            }
            queryBuilder.IsUnion = true;
            queryBuilder.SelectValue = "*";
            queryBuilder.Union = string.Join($"\r\n{(isAll ? "UNION ALL" : "UNION")}\r\n", sqlList);
            queryBuilder.EntityInfo.TableName = $"{(isAll ? "UNION_ALL" : "UNION")}_{querys.Count}";
            var queryProvider = new QueryProvider<T>(Ado, queryBuilder);
            return queryProvider;
        }

        /// <summary>
        /// 联合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="querys">查询对象数组</param>
        /// <returns></returns>
        public IQuery<T> Union<T>(params IQuery<T>[] querys)
        {
            return Union(querys.ToList());
        }

        /// <summary>
        /// 联合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="querys">查询对象列表</param>
        /// <returns></returns>
        public IQuery<T> Union<T>(List<IQuery<T>> querys)
        {
            return Union(false, querys);
        }

        /// <summary>
        /// 全联合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="querys">查询对象数组</param>
        /// <returns></returns>
        public IQuery<T> UnionAll<T>(params IQuery<T>[] querys)
        {
            return UnionAll(querys.ToList());
        }

        /// <summary>
        /// 全联合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="querys">查询对象列表</param>
        /// <returns></returns>
        public IQuery<T> UnionAll<T>(List<IQuery<T>> querys)
        {
            return Union(true, querys);
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IQuery<T> QueryWithAttr<T>() where T : class, new()
        {
            return Query<T>(GetAdoWithAttr<T>());
        }
        #endregion

        /// <summary>
        /// 快速
        /// </summary>
        /// <returns></returns>
        public IFast<T> Fast<T>() where T : class
        {
            return ProviderFactory.CreateFastProvider<T>(Ado);
        }
    }
}
