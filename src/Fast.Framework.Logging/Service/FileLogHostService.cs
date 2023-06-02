using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fast.Framework.Logging.Helper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Fast.Framework.Logging.Service
{

    /// <summary>
    /// 文件日志主机服务
    /// </summary>
    internal class FileLogHostService : BackgroundService
    {

        /// <summary>
        /// 日志
        /// </summary>
        private readonly ILogger<FileLogHostService> logger;

        /// <summary>
        /// 配置
        /// </summary>
        private readonly IConfiguration configuration;

        /// <summary>
        /// 构造方法
        /// </summary>
        /// <param name="logger">日志</param>
        /// <param name="configuration">配置</param>
        public FileLogHostService(ILogger<FileLogHostService> logger, IConfiguration configuration)
        {
            this.logger = logger;
            this.configuration = configuration;
        }

        /// <summary>
        /// 执行异步
        /// </summary>
        /// <param name="stoppingToken">停止令牌</param>
        /// <returns></returns>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        LockHelper.mutex.WaitOne();
                        var maxFileCount = Convert.ToInt32(configuration.GetSection("Logging:FileLog:MaxFileCount").Value);
                        foreach (var directory in DirectoryHelper.GetDirectorys())
                        {
                            var directoryInfo = new DirectoryInfo(directory);
                            var fileInfos = directoryInfo.GetFiles();
                            //如果没有文件移除目录
                            if (fileInfos.Length == 0)
                            {
                                DirectoryHelper.Remove(directory);
                            }
                            else
                            {
                                //最大文件个数限制清理
                                if (fileInfos.Length > maxFileCount)
                                {
                                    var removeFileInfo = fileInfos.OrderBy(o => o.CreationTime).ThenBy(o => o.LastWriteTime).SkipLast(maxFileCount).ToList();
                                    foreach (var item in removeFileInfo)
                                    {
                                        FileStreamHelper.Remove(item.FullName);//关闭流再删除
                                        File.Delete(item.FullName);
                                    }
                                }

                                //清理未使用文件流避免长时间占用
                                var currentDate = Convert.ToDateTime(DateTime.Now.ToString("yyyy-MM-dd 00:00:00.000"));
                                var colseFiles = fileInfos.Where(w => FileStreamHelper.ContainsKey(w.FullName) && w.LastWriteTime < currentDate).ToList();
                                if (colseFiles.Any())
                                {
                                    foreach (var item in colseFiles)
                                    {
                                        FileStreamHelper.Remove(item.FullName);//关闭流
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, $"文件日志后台服务发生异常:{ex.Message}");
                    }
                    finally
                    {
                        LockHelper.mutex.ReleaseMutex();
                    }
                    var autolearDelay = Convert.ToInt32(configuration.GetSection("Logging:FileLog:AutolearDelay").Value);
                    await Task.Delay(autolearDelay > 0 ? autolearDelay : 600000, stoppingToken);//默认10分钟清理
                }
            }, stoppingToken);
        }
    }
}
