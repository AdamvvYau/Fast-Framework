using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using Fast.Framework.Models;

namespace Fast.Framework.Interfaces
{

    /// <summary>
    /// IAop接口类
    /// </summary>
    public interface IAop
    {

        /// <summary>
        /// 数据库日志
        /// </summary>
        Action<string, List<DbParameter>> DbLog { get; set; }
    }
}
