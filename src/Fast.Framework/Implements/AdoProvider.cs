using System;
using System.Text;
using System.Linq;
using System.Threading;
using System.Reflection;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Fast.Framework.Interfaces;
using Fast.Framework.Extensions;
using Fast.Framework.Models;
using Fast.Framework.Factory;
using System.Text.RegularExpressions;
using System.Transactions;
using Fast.Framework.Utils;
using System.Drawing;

namespace Fast.Framework.Implements
{

    /// <summary>
    /// Ado实现类
    /// </summary>
    public class AdoProvider : IAdo
    {

        /// <summary>
        /// 数据库提供者工厂
        /// </summary>
        public DbProviderFactory DbProviderFactory { get; }

        /// <summary>
        /// 数据库选项
        /// </summary>
        public DbOptions DbOptions { get; }

        /// <summary>
        /// 主库连接对象
        /// </summary>
        public DbConnection MasterDbConnection { get; private set; }

        /// <summary>
        /// 主库执行对象
        /// </summary>
        public DbCommand MasterDbCommand { get; private set; }

        /// <summary>
        /// 数据库事务
        /// </summary>
        public DbTransaction DbTransaction { get; private set; }

        /// <summary>
        /// 从库连接对象
        /// </summary>
        public DbConnection SlaveDbConnection { get; private set; }

        /// <summary>
        /// 从库执行对象
        /// </summary>
        public DbCommand SlaveDbCommand { get; private set; }

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="dbOptions">数据选项</param>
        public AdoProvider(DbOptions dbOptions)
        {
            DbOptions = dbOptions;
            DbProviderFactories.RegisterFactory(DbOptions.ProviderName, DbOptions.FactoryName);
            DbProviderFactory = DbProviderFactories.GetFactory(DbOptions.ProviderName);
            MasterDbConnection = DbProviderFactory.CreateConnection();
            MasterDbCommand = MasterDbConnection.CreateCommand();
            MasterDbConnection.ConnectionString = DbOptions.ConnectionStrings;
            if (dbOptions.UseMasterSlaveSeparation)
            {
                if (dbOptions.SlaveItems.Count == 0)
                {
                    throw new Exception("使用主从分离,必须配置一个或多个从库.");
                }

                SlaveDbConnection = DbProviderFactory.CreateConnection();
                SlaveDbCommand = SlaveDbConnection.CreateCommand();

                if (dbOptions.SlaveItems.Count > 1)
                {
                    var weight_sum = dbOptions.SlaveItems.Sum(s => s.Weight);//总权重

                    if (weight_sum <= dbOptions.SlaveItems.Count)
                    {
                        throw new Exception("从库配置大于1总权重不能小于配置个数.");
                    }

                    //计算权重范围
                    var weight_range = dbOptions.SlaveItems.Select(s => new
                    {
                        W = (Convert.ToDecimal(s.Weight) / weight_sum) * weight_sum,
                        s.ConnectionStrings
                    }).ToList();

                    //取最大权重
                    var max_weight = Convert.ToInt32(weight_range.Max(a => a.W));

                    //随机权重
                    var randomWeight = new Random(DateTime.Now.Millisecond).Next(0, max_weight);

                    //匹配权重范围
                    SlaveDbConnection.ConnectionString = weight_range.First(f => randomWeight <= f.W).ConnectionStrings;
                }
                else
                {
                    SlaveDbConnection.ConnectionString = dbOptions.SlaveItems[0].ConnectionStrings;
                }
            }
        }

        /// <summary>
        /// 克隆
        /// </summary>
        /// <returns></returns>
        public virtual IAdo Clone()
        {
            return ProviderFactory.CreateAdoProvider(DbOptions);
        }

        /// <summary>
        /// 开启事务
        /// </summary>
        public void BeginTran()
        {
            if (MasterDbConnection.State != ConnectionState.Open)
            {
                MasterDbConnection.Open();
            }
            DbTransaction = MasterDbConnection.BeginTransaction();
        }

        /// <summary>
        /// 开启事务异步
        /// </summary>
        public async Task BeginTranAsync()
        {
            if (MasterDbConnection.State != ConnectionState.Open)
            {
                await MasterDbConnection.OpenAsync();
            }
            DbTransaction = await MasterDbConnection.BeginTransactionAsync();
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTran()
        {
            DbTransaction.Commit();
            DbTransaction = null;
            MasterDbCommand.Transaction = null;
            MasterDbConnection.Close();
        }

        /// <summary>
        /// 提交事务异步
        /// </summary>
        public async Task CommitTranAsync()
        {
            await DbTransaction.CommitAsync();
            DbTransaction = null;
            MasterDbCommand.Transaction = null;
            await MasterDbConnection.CloseAsync();
        }

        /// <summary>
        /// 回滚事务异步
        /// </summary>
        public void RollbackTran()
        {
            try
            {
                if (DbTransaction != null)
                {
                    DbTransaction.Rollback();
                    DbTransaction = null;
                    MasterDbCommand.Transaction = null;
                }
            }
            finally
            {
                MasterDbConnection.Close();
            }
        }

        /// <summary>
        /// 回滚事务异步
        /// </summary>
        /// <returns></returns>
        public async Task RollbackTranAsync()
        {
            try
            {
                if (DbTransaction != null)
                {
                    await DbTransaction.RollbackAsync();
                    DbTransaction = null;
                    MasterDbCommand.Transaction = null;
                }
            }
            finally
            {
                await MasterDbConnection.CloseAsync();
            }
        }

        /// <summary>
        /// 测试连接
        /// </summary>
        /// <returns></returns>
        public virtual bool TestConnection()
        {
            try
            {
                if (MasterDbConnection.State != ConnectionState.Open)
                {
                    MasterDbConnection.Open();
                }
                return true;
            }
            finally
            {
                MasterDbConnection.Close();
            }
        }

        /// <summary>
        /// 测试连接异步
        /// </summary>
        /// <returns></returns>
        public virtual async Task<bool> TestConnectionAsync()
        {
            try
            {
                if (MasterDbConnection.State != ConnectionState.Open)
                {
                    await MasterDbConnection.OpenAsync();
                }
                return true;
            }
            finally
            {
                await MasterDbConnection.CloseAsync();
            }
        }

        /// <summary>
        /// 准备命令
        /// </summary>
        /// <param name="command">命令</param>
        /// <param name="connection">连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual bool PrepareCommand(DbCommand command, DbConnection connection, DbTransaction transaction, CommandType commandType, string commandText, List<DbParameter> dbParameters)
        {
            var mustCloseConnection = false;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
                mustCloseConnection = true;
            }
            if (transaction != null)
            {
                command.Transaction = transaction;
                mustCloseConnection = false;
            }
            command.CommandType = commandType;
            command.CommandText = commandText;
            if (dbParameters != null && dbParameters.Any())
            {
                command.Parameters.AddRange(dbParameters.ToArray());
            }
            return mustCloseConnection;
        }

        /// <summary>
        /// 准备命令
        /// </summary>
        /// <param name="command">命令</param>
        /// <param name="connection">连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<bool> PrepareCommandAsync(DbCommand command, DbConnection connection, DbTransaction transaction, CommandType commandType, string commandText, List<DbParameter> dbParameters)
        {
            var mustCloseConnection = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
                mustCloseConnection = true;
            }
            if (transaction != null)
            {
                command.Transaction = transaction;
                mustCloseConnection = false;
            }
            command.CommandType = commandType;
            command.CommandText = commandText;
            if (dbParameters != null && dbParameters.Any())
            {
                command.Parameters.AddRange(dbParameters.ToArray());
            }
            return mustCloseConnection;
        }

        /// <summary>
        /// 执行非查询
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual int ExecuteNonQuery(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var mustCloseConnection = PrepareCommand(MasterDbCommand, MasterDbConnection, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                return MasterDbCommand.ExecuteNonQuery();
            }
            finally
            {
                MasterDbCommand.Parameters.Clear();
                if (mustCloseConnection)
                {
                    MasterDbConnection.Close();
                }
            }
        }

        /// <summary>
        /// 执行非查询异步
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<int> ExecuteNonQueryAsync(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var mustCloseConnection = await PrepareCommandAsync(MasterDbCommand, MasterDbConnection, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                return await MasterDbCommand.ExecuteNonQueryAsync();
            }
            finally
            {
                MasterDbCommand.Parameters.Clear();
                if (mustCloseConnection)
                {
                    await MasterDbConnection.CloseAsync();
                }
            }
        }

        /// <summary>
        /// 执行标量
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual T ExecuteScalar<T>(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
            var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

            var mustCloseConnection = PrepareCommand(cmd, con, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                var obj = cmd.ExecuteScalar();
                if (obj is DBNull)
                {
                    return default;
                }
                return obj.ChangeType<T>();
            }
            finally
            {
                cmd.Parameters.Clear();
                if (mustCloseConnection)
                {
                    con.Close();
                }
            }
        }

        /// <summary>
        /// 执行标量异步
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<T> ExecuteScalarAsync<T>(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
            var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

            var mustCloseConnection = await PrepareCommandAsync(cmd, con, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                var obj = await cmd.ExecuteScalarAsync();
                if (obj is DBNull)
                {
                    return default;
                }
                return obj.ChangeType<T>();
            }
            finally
            {
                cmd.Parameters.Clear();
                if (mustCloseConnection)
                {
                    await con.CloseAsync();
                }
            }
        }

        /// <summary>
        /// 执行阅读器
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual DbDataReader ExecuteReader(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
            var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

            var mustCloseConnection = PrepareCommand(cmd, con, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                if (mustCloseConnection)
                {
                    return cmd.ExecuteReader(CommandBehavior.CloseConnection);
                }
                else
                {
                    return cmd.ExecuteReader();
                }
            }
            finally
            {
                cmd.Parameters.Clear();
            }
        }

        /// <summary>
        /// 执行阅读器异步
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<DbDataReader> ExecuteReaderAsync(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
            var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

            var mustCloseConnection = await PrepareCommandAsync(cmd, con, DbTransaction, commandType, commandText, dbParameters);
            try
            {
                if (mustCloseConnection)
                {
                    return await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
                }
                else
                {
                    return await cmd.ExecuteReaderAsync();
                }
            }
            finally
            {
                cmd.Parameters.Clear();
            }
        }

        /// <summary>
        /// 执行数据集
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual DataSet ExecuteDataSet(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var ds = new DataSet();
            using (var adapter = DbProviderFactory.CreateDataAdapter())
            {
                var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
                var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

                var mustCloseConnection = PrepareCommand(cmd, con, DbTransaction, commandType, commandText, dbParameters);
                try
                {
                    adapter.SelectCommand = cmd;
                    adapter.Fill(ds);
                }
                finally
                {
                    cmd.Parameters.Clear();
                    if (mustCloseConnection)
                    {
                        con.Close();
                    }
                }
            }
            return ds;
        }

        /// <summary>
        /// 执行数据集异步
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<DataSet> ExecuteDataSetAsync(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var ds = new DataSet();
            using (var adapter = DbProviderFactory.CreateDataAdapter())
            {
                var con = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbConnection : MasterDbConnection;
                var cmd = DbOptions.UseMasterSlaveSeparation && DbTransaction == null ? SlaveDbCommand : MasterDbCommand;

                var mustCloseConnection = await PrepareCommandAsync(cmd, con, DbTransaction, commandType, commandText, dbParameters);
                try
                {
                    adapter.SelectCommand = cmd;
                    adapter.Fill(ds);
                }
                finally
                {
                    cmd.Parameters.Clear();
                    if (mustCloseConnection)
                    {
                        await con.CloseAsync();
                    }
                }
            }
            return ds;
        }

        /// <summary>
        /// 执行数据表格
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual DataTable ExecuteDataTable(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var ds = ExecuteDataSet(commandType, commandText, dbParameters);
            if (ds.Tables.Count > 0)
            {
                return ds.Tables[0];
            }
            return null;
        }

        /// <summary>
        /// 执行数据表格异步
        /// </summary>
        /// <param name="commandType">命令类型</param>
        /// <param name="commandText">命令文本</param>
        /// <param name="dbParameters">数据库参数</param>
        /// <returns></returns>
        public virtual async Task<DataTable> ExecuteDataTableAsync(CommandType commandType, string commandText, List<DbParameter> dbParameters = null)
        {
            var ds = await ExecuteDataSetAsync(commandType, commandText, dbParameters);
            if (ds.Tables.Count > 0)
            {
                return ds.Tables[0];
            }
            return null;
        }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <param name="parameterName">参数名</param>
        /// <param name="parameterValue">参数值</param>
        /// <param name="parameterDirection">参数方向</param>
        /// <returns></returns>
        public virtual DbParameter CreateParameter(string parameterName, object parameterValue, ParameterDirection parameterDirection = ParameterDirection.Input)
        {
            var parameter = DbProviderFactory.CreateParameter();
            parameter.ParameterName = $"{DbOptions.DbType.GetSymbol()}{parameterName}";
            parameter.Value = parameterValue ?? DBNull.Value;
            if (parameterValue != null)
            {
                parameter.Size = parameterValue is byte[] byteArray ? byteArray.Length : parameterValue is string stringValue ? stringValue.Length : 0;

                if (parameter.Size < 4000)
                {
                    parameter.Size = 4000;
                }
            }
            if (parameter.Size == 0)
            {
                parameter.Size = 4000;
            }
            parameter.Direction = parameterDirection;
            return parameter;
        }

        /// <summary>
        /// 创建参数
        /// </summary>
        /// <param name="keyValues">键值</param>
        /// <returns></returns>
        public virtual List<DbParameter> CreateParameter(Dictionary<string, object> keyValues)
        {
            var parameters = new List<DbParameter>();
            foreach (var item in keyValues)
            {
                parameters.Add(CreateParameter(item.Key, item.Value));
            }
            return parameters;
        }

        /// <summary>
        /// 转换参数
        /// </summary>
        /// <param name="fastParameter">数据库参数</param>
        /// <returns></returns>
        public DbParameter ConvertParameter(FastParameter fastParameter)
        {
            var parameter = DbProviderFactory.CreateParameter();
            parameter.ParameterName = $"{DbOptions.DbType.GetSymbol()}{fastParameter.ParameterName}";
            parameter.Value = fastParameter.Value ?? DBNull.Value;
            parameter.Size = fastParameter.Size;
            parameter.Direction = parameter.Direction;
            return parameter;
        }

        /// <summary>
        /// 转换参数
        /// </summary>
        /// <param name="fastParameters">数据库参数</param>
        /// <returns></returns>
        public List<DbParameter> ConvertParameter(List<FastParameter> fastParameters)
        {
            var parameters = new List<DbParameter>();
            foreach (var item in fastParameters)
            {
                parameters.Add(ConvertParameter(item));
            }
            return parameters;
        }
    }
}
