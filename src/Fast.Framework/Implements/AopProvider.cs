using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.Interfaces;
using Fast.Framework.Models;

namespace Fast.Framework.Implements
{

    /// <summary>
    /// Aop实现类
    /// </summary>
    public class AopProvider : IAop
    {

        /// <summary>
        /// 数据库日志
        /// </summary>
        public Action<string, List<DbParameter>> DbLog { get; set; }
    }
}
