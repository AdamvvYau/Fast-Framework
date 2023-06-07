using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.Models;


namespace Fast.Framework.Interfaces
{

    /// <summary>
    /// 表达式解析Sql接口类
    /// </summary>
    public interface IExpressionResolveSql
    {

        /// <summary>
        /// 解析Sql选项
        /// </summary>
        ResolveSqlOptions ResolveSqlOptions { get; }

        /// <summary>
        /// 参数索引
        /// </summary>
        Dictionary<string, int> ParameterIndexs { get; }

        /// <summary>
        /// Sql构建
        /// </summary>
        StringBuilder SqlBuilder { get; }

        /// <summary>
        /// 数据库参数
        /// </summary>
        List<FastParameter> DbParameters { get; }

        /// <summary>
        /// 获取值
        /// </summary>
        IExpressionResolveValue GetValue { get; }

        /// <summary>
        /// 访问
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        Expression Visit(Expression node);

        /// <summary>
        /// 是否Not
        /// </summary>
        bool IsNot { get; set; }
    }
}
