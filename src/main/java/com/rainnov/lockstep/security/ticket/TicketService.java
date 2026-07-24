package com.rainnov.lockstep.security.ticket;

/**
 * 签发并验证带签名的玩家连接票据。
 */
public interface TicketService {

    /**
     * 为给定声明签发确定性令牌。
     *
     * @param claims 待签名的不可变声明
     * @return 适用于 URL 且不带 Base64 填充的令牌
     */
    String issue(TicketClaims claims);

    /**
     * 验证令牌的结构、签名和时间声明。
     *
     * @param token 待验证的令牌
     * @return 已通过认证的声明
     * @throws TicketValidationException 验证失败时抛出
     */
    TicketClaims validate(String token);
}
