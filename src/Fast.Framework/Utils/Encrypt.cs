using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Fast.Framework.Utils
{

    /// <summary>
    /// 加密工具类
    /// </summary>
    public static class Encrypt
    {

        /// <summary>
        /// DES 加密
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="key">密钥</param>
        /// <returns></returns>
        public static string DESEncrypt(string str, string key = "!@#$%^&*")
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (key.Length < 8)
            {
                throw new ArgumentException("key length 8");
            }
            var rgbKey = Encoding.UTF8.GetBytes(key.Substring(0, 8));
            var rgbIV = rgbKey;
            var strByte = Encoding.UTF8.GetBytes(str);
            var provider = DES.Create();
            var memoryStream = new MemoryStream();
            var cryptoStream = new CryptoStream(memoryStream, provider.CreateEncryptor(rgbKey, rgbIV), CryptoStreamMode.Write);
            cryptoStream.Write(strByte, 0, strByte.Length);
            cryptoStream.FlushFinalBlock();
            return Convert.ToBase64String(memoryStream.ToArray());
        }

        /// <summary>
        /// DES 解密
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="key">密钥</param>
        /// <returns></returns>
        public static string DESDecrypt(string str, string key = "!@#$%^&*")
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (key.Length < 8)
            {
                throw new ArgumentException("key length 8");
            }
            var rgbKey = Encoding.UTF8.GetBytes(key.Substring(0, 8));
            var rgbIV = rgbKey;
            var strByte = Convert.FromBase64String(str);
            var provider = DES.Create();
            var memoryStream = new MemoryStream();
            var cryptoStream = new CryptoStream(memoryStream, provider.CreateDecryptor(rgbKey, rgbIV), CryptoStreamMode.Write);
            cryptoStream.Write(strByte, 0, strByte.Length);
            cryptoStream.FlushFinalBlock();
            return Encoding.UTF8.GetString(memoryStream.ToArray());
        }

        /// <summary>
        /// MD5 文本加密
        /// </summary>
        /// <param name="str">源字符串</param>
        /// <param name="encoding">字符编码</param>
        /// <returns>密文</returns>
        public static string MD5Encrypt(string str, Encoding encoding = null)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            encoding ??= Encoding.UTF8;
            var provider = MD5.Create();
            var data = provider.ComputeHash(encoding.GetBytes(str));
            var builder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
            {
                builder.Append(data[i].ToString("x2"));
            }
            return builder.ToString();
        }

        /// <summary>
        /// MD5 文件流加密
        /// </summary>
        /// <param name="stream">文件流</param>
        /// <returns>密文</returns>
        public static string MD5Encrypt(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            var provider = MD5.Create();
            var data = provider.ComputeHash(stream);
            var builder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
            {
                builder.Append(data[i].ToString("x2"));
            }
            return builder.ToString();
        }

        /// <summary>
        /// 创建RSA公钥(PKCS#1)和私钥(PKCS#1)
        /// </summary>
        /// <returns>公钥和密钥</returns>
        public static Dictionary<string, string> CreateRSAKey()
        {
            var rsa = RSA.Create();
            return new Dictionary<string, string> { ["public"] = Convert.ToBase64String(rsa.ExportRSAPublicKey()), ["private"] = Convert.ToBase64String(rsa.ExportRSAPrivateKey()) };
        }

        /// <summary>
        /// 写入RSA密钥
        /// </summary>
        /// <param name="keyValues">键值对</param>
        /// <param name="directory">目录</param>
        /// <param name="encoding">编码</param>
        public static void WriteRSAKey(Dictionary<string, string> keyValues, string directory = "", Encoding encoding = null)
        {
            if (string.IsNullOrWhiteSpace(directory))
            {
                directory = AppDomain.CurrentDomain.BaseDirectory;
            }
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            encoding ??= Encoding.UTF8;
            using (var sw = new StreamWriter(Path.Combine(directory, "PublicKey.pem"), false, encoding))
            {
                sw.Write(keyValues["public"]);
            }
            using (var sw = new StreamWriter(Path.Combine(directory, "PrivateKey.pem"), false, encoding))
            {
                sw.Write(keyValues["private"]);
            }
        }

        /// <summary>
        /// RAS 加密
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="publicKey">公钥</param>
        /// <returns></returns>
        public static string RSAEncrypt(string str, string publicKey)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            if (string.IsNullOrWhiteSpace(publicKey))
            {
                throw new ArgumentNullException(nameof(publicKey));
            }
            var rsa = RSA.Create();
            rsa.ImportRSAPublicKey(Convert.FromBase64String(publicKey), out _);

            var bytes = Encoding.UTF8.GetBytes(str);
            var batchSize = (rsa.KeySize / 8) - 11;//批次大小

            using var memoryStream = new MemoryStream();

            for (int i = 0; i < bytes.LongLength; i += batchSize)
            {
                var batchArray = bytes.Skip(i).Take(batchSize).ToArray();
                var encryptBytes = rsa.Encrypt(batchArray, RSAEncryptionPadding.Pkcs1);
                memoryStream.Write(encryptBytes);
            }
            return Convert.ToBase64String(memoryStream.ToArray());
        }

        /// <summary>
        /// RSA 解密
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="privateKey">私钥</param>
        /// <returns></returns>
        public static string RSADecrypt(string str, string privateKey)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            if (string.IsNullOrWhiteSpace(privateKey))
            {
                throw new ArgumentNullException(nameof(privateKey));
            }
            var rsa = RSA.Create();
            rsa.ImportRSAPrivateKey(Convert.FromBase64String(privateKey), out _);

            var bytes = Convert.FromBase64String(str);
            var batchSize = (rsa.KeySize / 8);

            using var memoryStream = new MemoryStream();

            for (int i = 0; i < bytes.LongLength; i += batchSize)
            {
                var batchArray = bytes.Skip(i).Take(batchSize).ToArray();
                var encryptBytes = rsa.Decrypt(batchArray, RSAEncryptionPadding.Pkcs1);
                memoryStream.Write(encryptBytes);
            }
            return Encoding.UTF8.GetString(memoryStream.ToArray());
        }
    }
}
