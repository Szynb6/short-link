/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.project.service.impl;

import cn.hutool.core.util.StrUtil;
import com.nageoffer.shortlink.project.common.convention.exception.ClientException;
import com.nageoffer.shortlink.project.config.GotoDomainWhiteListConfiguration;
import com.nageoffer.shortlink.project.service.UrlTitleService;
import com.nageoffer.shortlink.project.toolkit.LinkUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

/**
 * URL 标题接口实现层
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UrlTitleServiceImpl implements UrlTitleService {

    /**
     * 连接/读取超时（毫秒），避免慢目标站挂死 Tomcat 线程
     */
    private static final int TIMEOUT_MS = 2000;

    /**
     * 响应体大小上限（字节），title 在 head 中靠前的位置，512KB 足够，防止超大页面占内存
     */
    private static final int MAX_BODY_SIZE = 512 * 1024;

    /**
     * 最大重定向次数，防止重定向环
     */
    private static final int MAX_REDIRECTS = 3;

    /**
     * User-Agent，避免目标站因缺少 UA 拒绝或返回移动端页面
     */
    private static final String USER_AGENT = "Mozilla/5.0 (compatible; ShortLink-TitleBot/1.0)";

    private final GotoDomainWhiteListConfiguration gotoDomainWhiteListConfiguration;

    @Override
    public String getTitleByUrl(String url) {
        String currentUrl = validateUrl(url);
        try {
            // 手动逐跳跟随重定向：每一跳都重新过协议/白名单/内网地址校验，
            // 否则攻击者可利用白名单站上的开放重定向 302 到内网地址，绕过 SSRF 防护
            for (int redirectCount = 0; redirectCount <= MAX_REDIRECTS; redirectCount++) {
                Connection.Response response = Jsoup.connect(currentUrl)
                        .userAgent(USER_AGENT)
                        .timeout(TIMEOUT_MS)
                        .maxBodySize(MAX_BODY_SIZE)
                        .followRedirects(false)
                        .execute();
                int statusCode = response.statusCode();
                if (statusCode >= 300 && statusCode < 400) {
                    String location = response.header("Location");
                    if (StrUtil.isBlank(location) || redirectCount == MAX_REDIRECTS) {
                        throw new ClientException("目标站点重定向异常");
                    }
                    currentUrl = resolveRedirect(currentUrl, location);
                    continue;
                }
                Document document = response.parse();
                String title = document.title();
                return StrUtil.isBlank(title) ? "该页面无标题" : title;
            }
            throw new ClientException("目标站点重定向次数过多");
        } catch (SocketTimeoutException ex) {
            log.warn("获取页面标题超时，url: {}", url, ex);
            throw new ClientException("获取标题超时，请稍后重试或检查目标站点是否可访问");
        } catch (HttpStatusException ex) {
            log.warn("目标站点返回异常状态码，url: {}, status: {}", url, ex.getStatusCode());
            throw new ClientException("目标站点无法访问（HTTP " + ex.getStatusCode() + "）");
        } catch (UnsupportedMimeTypeException ex) {
            log.warn("目标链接非 HTML 页面，url: {}", url, ex);
            throw new ClientException("目标链接不是网页，无法获取标题");
        } catch (IOException ex) {
            log.warn("获取页面标题失败，url: {}", url, ex);
            throw new ClientException("无法获取标题，请检查链接是否正确或稍后再试");
        }
    }

    /**
     * 解析重定向地址并重新校验。相对路径 Location 基于当前 URL 解析。
     */
    private String resolveRedirect(String baseUrl, String location) {
        URI resolved;
        try {
            resolved = new URI(baseUrl).resolve(location);
        } catch (Exception ex) {
            throw new ClientException("目标站点重定向地址非法");
        }
        return validateUrl(resolved.toString());
    }

    /**
     * 目标链接完整校验：协议白名单 → 域名白名单 → SSRF 内网地址拦截。
     * <p>
     * 关于 DNS 重绑定的说明：校验通过后按域名连接（https 用 IP 直连会因证书 SNI 校验失败），
     * 校验与连接之间存在毫秒级 TOCTOU 窗口。白名单模式下攻击者无法控制白名单域名的解析，
     * 该风险可忽略；生产环境如需彻底消除，可换用支持解析后 IP 固定的 HTTP 客户端或出网代理 ACL。
     */
    private String validateUrl(String url) {
        if (StrUtil.isBlank(url)) {
            throw new ClientException("目标链接不能为空");
        }
        String trimmedUrl = url.trim();
        String lowerUrl = trimmedUrl.toLowerCase();
        // ① 协议白名单：只允许 http/https，禁止 file/dict/gopher/ftp 等 SSRF 常用协议
        if (!lowerUrl.startsWith("http://") && !lowerUrl.startsWith("https://")) {
            throw new ClientException("目标链接协议非法，仅支持 http/https");
        }
        URI uri;
        try {
            uri = new URI(trimmedUrl);
        } catch (Exception ex) {
            throw new ClientException("目标链接格式错误");
        }
        String host = uri.getHost();
        if (StrUtil.isBlank(host)) {
            throw new ClientException("目标链接缺少域名");
        }

        // ② 域名白名单先于 DNS 解析执行：被拒域名无需浪费一次解析
        verificationWhitelist(trimmedUrl);

        // ③ SSRF 防护：解析 IP 后拒绝私网/回环/链路本地等特殊地址
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(host);
        } catch (UnknownHostException ex) {
            throw new ClientException("无法解析目标域名，请确认链接正确");
        }
        if (isInternalAddress(inetAddress)) {
            log.warn("检测到 SSRF 尝试，拒绝访问内网/特殊地址，url: {}, resolvedIp: {}", url, inetAddress.getHostAddress());
            throw new ClientException("非法目标地址");
        }
        return trimmedUrl;
    }

    /**
     * 是否内网/特殊地址：回环、私网、链路本地、任意本地、组播、IPv6 唯一本地（fc00::/7）
     */
    private boolean isInternalAddress(InetAddress inetAddress) {
        if (inetAddress.isLoopbackAddress()
                || inetAddress.isSiteLocalAddress()
                || inetAddress.isLinkLocalAddress()
                || inetAddress.isAnyLocalAddress()
                || inetAddress.isMulticastAddress()) {
            return true;
        }
        return inetAddress instanceof Inet6Address
                && (inetAddress.getAddress()[0] & 0xfe) == 0xfc;
    }

    /**
     * 域名白名单校验：精确匹配或子域匹配（如 zhuanlan.zhihu.com 匹配 zhihu.com），
     * 子域放行是为了不阻断白名单站点的站内子域重定向；与创建短链共用同一份白名单配置。
     * 白名单未启用（enable=false 或未配置）时放行所有合法公网域名。
     */
    private void verificationWhitelist(String originUrl) {
        Boolean enable = gotoDomainWhiteListConfiguration.getEnable();
        if (enable == null || !enable) {
            return;
        }
        String domain = LinkUtil.extractDomain(originUrl);
        if (StrUtil.isBlank(domain)) {
            throw new ClientException("目标链接填写错误");
        }
        List<String> details = gotoDomainWhiteListConfiguration.getDetails();
        boolean allowed = details.stream()
                .anyMatch(item -> domain.equals(item) || domain.endsWith("." + item));
        if (!allowed) {
            throw new ClientException("演示环境为避免恶意攻击，请仅使用以下网站链接：" + gotoDomainWhiteListConfiguration.getNames());
        }
    }
}
