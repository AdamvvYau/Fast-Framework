using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fast.Framework.Models
{


    /// <summary>
    /// Jwt选项
    /// </summary>
    public class JwtOptions
    {

        /// <summary>
        /// 颁发者
        /// </summary>
        public string Issuer { get; set; } = "fast.framework";

        /// <summary>
        /// 接收者
        /// </summary>
        public string Audience { get; set; } = "user";

        /// <summary>
        /// 对称安全密钥
        /// </summary>
        public string SymmetricSecurityKey { get; set; } = Guid.NewGuid().ToString();
    }
}
