using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Fast.Framework.Abstract;
using Fast.Framework.Extensions;
using Fast.Framework.Enum;


namespace Fast.Framework.SqlServer
{

    /// <summary>
    /// SqlServer更新建造者
    /// </summary>
    public class SqlServerUpdateBuilder : UpdateBuilder
    {

        /// <summary>
        /// 数据库类型
        /// </summary>
        public override DbType DbType => DbType.SQLServer;

        /// <summary>
        /// 列表更新模板
        /// </summary>
        public override string ListUpdateTemplate { get; set; } = $@"UPDATE {DbType.SQLServer.GetIdentifier().Insert(1, "{4}")} 
SET {{1}} 
FROM
	{{0}} {DbType.SQLServer.GetIdentifier().Insert(1, "{4}")}
	INNER JOIN ( {{2}} ) {DbType.SQLServer.GetIdentifier().Insert(1, "{5}")} ON {{3}}";
    }
}
