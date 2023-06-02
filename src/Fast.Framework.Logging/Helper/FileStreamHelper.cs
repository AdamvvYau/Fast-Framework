using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Fast.Framework.Logging.Helper
{

    /// <summary>
    /// 文件流助手
    /// </summary>
    public static class FileStreamHelper
    {

        /// <summary>
        /// 文件流缓存
        /// </summary>
        private static readonly ConcurrentDictionary<string, Lazy<FileStream>> fileStreamCache;

        /// <summary>
        /// 构造方法
        /// </summary>
        static FileStreamHelper()
        {
            fileStreamCache = new ConcurrentDictionary<string, Lazy<FileStream>>();
        }

        /// <summary>
        /// 获取或添加
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static FileStream GetOrAdd(string path)
        {
            return fileStreamCache.GetOrAdd(path, key => new Lazy<FileStream>(() =>
            {
                return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            })).Value;
        }

        /// <summary>
        /// 包含Key
        /// </summary>
        /// <param name="path">路径</param>
        /// <returns></returns>
        public static bool ContainsKey(string path)
        {
            return fileStreamCache.ContainsKey(path);
        }

        /// <summary>
        /// 写入
        /// </summary>
        /// <param name="path">路径</param>
        /// <param name="text">文本</param>
        public static void Write(string path, string text, bool isAppend = false, Encoding encoding = null)
        {
            var stream = GetOrAdd(path);
            if (!isAppend)
            {
                stream.SetLength(0);//丢弃之前内容
            }
            var bytes = encoding == null ? Encoding.UTF8.GetBytes(text) : encoding.GetBytes(text);
            stream.Write(bytes);
            stream.Flush();
            stream.Seek(stream.Length, SeekOrigin.Begin);//重新设置偏移
        }

        /// <summary>
        /// 移除
        /// </summary>
        /// <param name="path">路径</param>
        /// <returns></returns>
        public static void Remove(string path)
        {
            if (fileStreamCache.ContainsKey(path))
            {
                fileStreamCache.TryRemove(path, out var value);
                value.Value.Close();
            }
        }

        /// <summary>
        /// 清除
        /// </summary>
        /// <returns></returns>
        public static void Clear()
        {
            foreach (var item in fileStreamCache)
            {
                item.Value.Value.Close();
            }
            fileStreamCache.Clear();
        }
    }
}
