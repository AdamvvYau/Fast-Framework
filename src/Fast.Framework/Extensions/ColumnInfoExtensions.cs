using Fast.Framework.Models;
using Fast.Framework.Snowflake;
using Fast.Framework.Utils;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fast.Framework.Extensions
{

    /// <summary>
    /// 列信息扩展
    /// </summary>
    public static class ColumnInfoExtensions
    {

        /// <summary>
        /// 生成数据库参数
        /// </summary>
        /// <param name="columnInfos">列信息</param>
        /// <param name="obj">对象</param>
        /// <param name="filter">过滤</param>
        /// <returns></returns>
        public static List<FastParameter> GenerateDbParameters(this List<ColumnInfo> columnInfos, object obj, Func<ColumnInfo, bool> filter = null)
        {
            var dbParameters = new List<FastParameter>();

            if (filter != null)
            {
                columnInfos = columnInfos.Where(filter).ToList();
            }

            foreach (var columnInfo in columnInfos)
            {
                columnInfo.ParameterName = columnInfo.ColumnName;

                var value = columnInfo.PropertyInfo.GetValue(obj);

                if (columnInfo.IsJson)
                {
                    value = Json.Serialize(value);
                }

                var parameter = new FastParameter(columnInfo.ParameterName, value);
                dbParameters.Add(parameter);
            }
            return dbParameters;
        }

        /// <summary>
        /// 计算值
        /// </summary>
        /// <param name="columnInfos">列信息</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public static List<object> ComputedValues(this List<ColumnInfo> columnInfos, List<FastParameter> dbParameters)
        {
            var list = new List<object>();
            // string guid long类型,如果为null或0将自动生成ID
            var genColumnInfos = columnInfos.Where(w => w.DatabaseGeneratedOption == DatabaseGeneratedOption.Computed && w.PropertyInfo.PropertyType != typeof(int));
            foreach (var columnInfo in genColumnInfos)
            {
                var parameter = dbParameters.First(f => f.ParameterName == columnInfo.ParameterName);
                if (columnInfo.PropertyInfo.PropertyType == typeof(string))
                {
                    if (string.IsNullOrWhiteSpace(Convert.ToString(parameter.Value)))
                    {
                        parameter.Value = Guid.NewGuid().ToString();
                    }
                }
                else if (columnInfo.PropertyInfo.PropertyType == typeof(Guid))
                {
                    if (Guid.Empty.ToString() == Convert.ToString(parameter.Value))
                    {
                        parameter.Value = Guid.NewGuid();
                    }
                }
                else if (columnInfo.PropertyInfo.PropertyType == typeof(long))
                {
                    if (Convert.ToInt64(parameter.Value) == 0)
                    {
                        var snowflakeIdOptions = JsonConfig.GetInstance().GetSection("SnowflakeIdOptions").Get<SnowflakeIdOptions>();
                        if (snowflakeIdOptions == null)
                        {
                            throw new Exception($"属性名称{columnInfo.PropertyInfo.Name} 标记为DatabaseGeneratedOption.Computed类型,且属性类型为long类型。为了避免分布式部署ID重复问题,请在appsettings.json配置文件根结点添加SnowflakeIdOptions选项.");
                        }
                        parameter.Value = new SnowflakeId(snowflakeIdOptions).NextId();
                    }
                }
                columnInfo.ComputedValue = parameter.Value;
                list.Add(parameter.Value);
            }
            return list;
        }
    }
}
