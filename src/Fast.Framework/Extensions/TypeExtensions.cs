using Fast.Framework.Cache;
using Fast.Framework.CustomAttribute;
using Fast.Framework.Models;
using Fast.Framework.Utils;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Fast.Framework.Extensions
{

    /// <summary>
    /// 类型扩展类
    /// </summary>
    public static class TypeExtensions
    {

        /// <summary>
        /// 类型默认值缓存
        /// </summary>
        private static readonly Dictionary<Type, object> typeDefaultValueCache;

        /// <summary>
        /// 静态构造方法
        /// </summary>
        static TypeExtensions()
        {
            typeDefaultValueCache = new Dictionary<Type, object>()
            {
                { typeof(object), default(object)},
                { typeof(string), default(string)},
                { typeof(bool), default(bool)},
                { typeof(DateTime), default(DateTime)},
                { typeof(Guid), default(Guid)},
                { typeof(int), default(int)},
                { typeof(uint), default(uint)},
                { typeof(long), default(long)},
                { typeof(ulong), default(ulong)},
                { typeof(decimal), default(decimal)},
                { typeof(double), default(double)},
                { typeof(float), default(float)},
                { typeof(short), default(short)},
                { typeof(ushort), default(ushort)},
                { typeof(byte), default(byte)},
                { typeof(sbyte), default(sbyte)},
                { typeof(char), default(char)}
            };
        }

        /// <summary>
        /// 获取类型默认值
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static object GetTypeDefaultValue(this Type type)
        {
            if (typeDefaultValueCache.ContainsKey(type))
            {
                return typeDefaultValueCache[type];
            }
            return null;
        }

        /// <summary>
        /// 初始化列信息
        /// </summary>
        /// <param name="columnInfo"></param>
        /// <param name="memberInfo"></param>
        private static void InitColumnInfo(ColumnInfo columnInfo, MemberInfo memberInfo)
        {
            var databaseGeneratedAttribute = memberInfo.GetCustomAttribute<DatabaseGeneratedAttribute>(false);
            if (databaseGeneratedAttribute != null)
            {
                columnInfo.DatabaseGeneratedOption = databaseGeneratedAttribute.DatabaseGeneratedOption;
            }
            var descriptionAttribute = memberInfo.GetCustomAttribute<DescriptionAttribute>(false);
            if (descriptionAttribute != null)
            {
                columnInfo.Description = descriptionAttribute.Description;
            }
            columnInfo.IsNotMapped = memberInfo.IsDefined(typeof(NotMappedAttribute), false);
            columnInfo.IsPrimaryKey = memberInfo.IsDefined(typeof(KeyAttribute), false);
            columnInfo.IsVersion = memberInfo.IsDefined(typeof(OptLockAttribute), false);

            var columnAttribute = memberInfo.GetCustomAttribute<ColumnAttribute>(false);
            if (columnAttribute == null)
            {
                columnInfo.ColumnName = memberInfo.Name;
            }
            else
            {
                if (string.IsNullOrWhiteSpace(columnAttribute.Name))
                {
                    columnInfo.ColumnName = memberInfo.Name;
                }
                else
                {
                    columnInfo.ColumnName = columnAttribute.Name;
                }
                columnInfo.TypeName = columnAttribute.TypeName;
                columnInfo.IsJson = columnInfo.TypeName != null && columnInfo.TypeName.ToLower().Equals("json");
            }
        }

        /// <summary>
        /// 获取实体信息
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static EntityInfo GetEntityInfo(this Type type)
        {
            var cacheKey = $"{type.FullName}_EntityInfo";

            var entityInfoCache = StaticCache<EntityInfo>.GetOrAdd(cacheKey, () =>
            {
                var entityInfo = new EntityInfo();
                entityInfo.EntityType = type;
                entityInfo.EntityName = type.Name;
                entityInfo.IsAnonymousType = type.FullName.StartsWith("<>f__AnonymousType");

                var tableAttribute = type.GetCustomAttribute<TableAttribute>(false);
                entityInfo.TableName = tableAttribute == null ? type.Name : tableAttribute.Name;

                if (!entityInfo.IsAnonymousType)
                {
                    var tenantAttribute = type.GetCustomAttribute<TenantAttribute>(false);
                    if (tenantAttribute != null)
                    {
                        entityInfo.TenantId = tenantAttribute.TenantId;
                    }
                    var descriptionAttribute = type.GetCustomAttribute<DescriptionAttribute>(false);
                    if (descriptionAttribute != null)
                    {
                        entityInfo.Description = descriptionAttribute.Description;
                    }
                }

                var propertyInfos = type.GetProperties();

                var fieldInfos = type.GetFields(BindingFlags.Public | BindingFlags.Instance);

                var propertyColumnInfos = propertyInfos.Select(s =>
                {
                    var obj = new ColumnInfo
                    {
                        PropertyInfo = s,
                        IsNullable = s.PropertyType.IsGenericType && s.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>))
                    };

                    if (entityInfo.IsAnonymousType)
                    {
                        obj.ColumnName = s.Name;
                        obj.IsJson = s.PropertyType.IsClass && !s.PropertyType.Equals(typeof(string));
                    }
                    else
                    {
                        InitColumnInfo(obj, s);
                    }
                    return obj;
                });

                var fieldColumnInfos = fieldInfos.Select(s =>
                {
                    var obj = new ColumnInfo
                    {
                        IsField = true,
                        FieldInfo = s,
                        IsNullable = s.FieldType.IsGenericType && s.FieldType.GetGenericTypeDefinition().Equals(typeof(Nullable<>))
                    };

                    InitColumnInfo(obj, s);
                    return obj;
                });

                entityInfo.ColumnsInfos.AddRange(propertyColumnInfos);
                entityInfo.ColumnsInfos.AddRange(fieldColumnInfos);
                return entityInfo;
            });
            return entityInfoCache.Clone();
        }

        ///// <summary>
        ///// 获取表名称
        ///// </summary>
        ///// <param name="type">类型</param>
        ///// <returns></returns>
        //public static string GetTableName(this Type type)
        //{
        //    var tableAttribute = type.GetCustomAttribute<TableAttribute>(false);
        //    return tableAttribute == null ? type.Name : tableAttribute.Name;
        //}

        /// <summary>
        /// 获取版本ID
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static object GetVersionId(this Type type)
        {
            if (type.Equals(typeof(object)) || type.Equals(typeof(string)))
            {
                return Guid.NewGuid().ToString();
            }
            else if (type.Equals(typeof(Guid)))
            {
                return Guid.NewGuid();
            }
            else
            {
                throw new NotSupportedException("该类型暂不支持");
            }
        }

        /// <summary>
        /// 获取数据库类型
        /// </summary>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static System.Data.DbType GetDbType(this Type type)
        {
            #region 判断
            if (type == typeof(byte[]))
            {
                return DbType.Binary;
            }
            else if (type == typeof(byte))
            {
                return DbType.Byte;
            }
            else if (type == typeof(bool))
            {
                return DbType.Boolean;
            }
            else if (type == typeof(DateTime))
            {
                return DbType.DateTime;
            }
            else if (type == typeof(decimal))
            {
                return DbType.Decimal;
            }
            else if (type == typeof(double))
            {
                return DbType.Double;
            }
            else if (type == typeof(Guid))
            {
                return DbType.Guid;
            }
            else if (type == typeof(short))
            {
                return DbType.Int16;
            }
            else if (type == typeof(int))
            {
                return DbType.Int32;
            }
            else if (type == typeof(long))
            {
                return DbType.Int64;
            }
            else if (type == typeof(sbyte))
            {
                return DbType.SByte;
            }
            else if (type == typeof(float))
            {
                return DbType.Single;
            }
            else if (type == typeof(string))
            {
                return DbType.String;
            }
            else if (type == typeof(TimeSpan))
            {
                return DbType.Time;
            }
            else if (type == typeof(ushort))
            {
                return DbType.UInt16;
            }
            else if (type == typeof(uint))
            {
                return DbType.UInt32;
            }
            else if (type == typeof(ulong))
            {
                return DbType.UInt64;
            }
            else if (type == typeof(DateTimeOffset))
            {
                return DbType.DateTimeOffset;
            }
            else
            {
                return DbType.Object;
            }
            #endregion
        }
    }
}
