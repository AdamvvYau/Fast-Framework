using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Fast.Framework.Utils
{

    /// <summary>
    /// 字符串工具类
    /// </summary>
    public static class Str
    {

        /// <summary>
        /// 获取中文字符数量
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static int GetChineseCharCount(string input)
        {
            return Regex.Matches(input, "[\u4e00-\u9fa5]|[^\x00-\xff]").Count;
        }
    }
}
