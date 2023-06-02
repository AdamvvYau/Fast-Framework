using System;
using System.Text;
using System.Security.Claims;
using System.IdentityModel.Tokens.Jwt;
using System.Collections.Generic;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Extensions.Configuration;
using Fast.Framework.Models;
using Fast.Framework.Utils;

namespace Fast.Framework.Web.Test
{

    /// <summary>
    /// Token
    /// </summary>
    public static class Token
    {

        /// <summary>
        /// Jwt 安全令牌处理程序
        /// </summary>
        private static readonly JwtSecurityTokenHandler jwtSecurityTokenHandler;

        /// <summary>
        /// 选项
        /// </summary>
        private static readonly JwtOptions options;

        /// <summary>
        /// Token验证参数
        /// </summary>
        public static readonly TokenValidationParameters tokenValidationParameters;

        /// <summary>
        /// 对称安全密钥
        /// </summary>
        private static readonly SymmetricSecurityKey symmetricSecurityKey;

        /// <summary>
        /// 构造方法
        /// </summary>
        static Token()
        {
            options = JsonConfig.GetInstance().GetSection("JwtOptions").Get<JwtOptions>();
            jwtSecurityTokenHandler = new JwtSecurityTokenHandler();
            symmetricSecurityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(options.SymmetricSecurityKey));

            tokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true,//验证颁发者
                ValidateAudience = true,//验证接收者
                ValidateLifetime = true,//验证过期时间
                ValidateIssuerSigningKey = true, //是否验证签名
                ValidIssuer = options.Issuer,//颁发者
                ValidAudience = options.Audience,//接收者
                IssuerSigningKey = symmetricSecurityKey,//解密密钥
                ClockSkew = TimeSpan.Zero //缓冲时间
            };
        }

        /// <summary>
        /// 创建JwtToken
        /// </summary>
        /// <param name="expirationTime">过期时间</param>
        /// <param name="claims">自定义Claims</param>
        /// <returns></returns>
        public static string CreateJwtToken(DateTime expirationTime, IEnumerable<Claim> claims = null)
        {
            var creds = new SigningCredentials(symmetricSecurityKey, SecurityAlgorithms.HmacSha256);
            var token = new JwtSecurityToken(
                issuer: options.Issuer,
                audience: options.Audience,
                claims: claims,
                notBefore: DateTime.Now,
                expires: expirationTime,
                signingCredentials: creds);
            return jwtSecurityTokenHandler.WriteToken(token);
        }

    }
}
