using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Fast.Framework.Interfaces
{

    /// <summary>
    /// 表达式解析值接口
    /// </summary>
    public interface IExpressionResolveValue
    {
        /// <summary>
        /// 访问
        /// </summary>
        /// <param name="node">节点</param>
        /// <returns></returns>
        object Visit(Expression node);

        /// <summary>
        /// 方法调用前
        /// </summary>
        /// <returns></returns>
        Action<object, MethodCallExpression> MethodCallBefore { get; set; }

        /// <summary>
        /// 方法调用后
        /// </summary>
        /// <returns></returns>
        Action<object, object, MethodCallExpression> MethodCallAfter { get; set; }
    }
}
