# 访问控制与对外提供

BareS3 对外访问大概有四条路：

- S3 API
- `/pub/{bucket}/*`
- 分享链接 `/s/{id}` 和 `/dl/{id}`
- 绑定到你自己域名上的公共访问


## 桶访问模式

当前桶访问模式有三种：

- `private`
- `public`
- `custom`

### `private`

- 默认要求认证访问
- 对公开文件路由来说，必须提供已有的具有读取权限的 S3 凭证

### `public`

- 整个桶默认公开读
- 适合静态资源、公开下载这类场景

### `custom`

- 你可以按前缀写规则
- 默认动作和规则动作都支持：`public`、`authenticated`、`deny`

### `custom` 的匹配规则

规则是按顺序的  
第一个前缀命中的规则直接生效，并舍弃后面的动作

比如：

```json
{
  "default_action": "authenticated",
  "rules": [
    { "prefix": "images/", "action": "public" },
    { "prefix": "secret/", "action": "deny" }
  ]
}
```

在这个示例中，即代表除了 `images/` 是公开的、`secret/` 是不允许访问的，其他的都需要带鉴权


## 文件服务入口

### `/pub/{bucket}/*`

- 最直接的公开文件路径
- 适合自己拼链接

### `/s/{id}`

- 分享链接查看地址
- 保持原有 inline 访问语义

### `/dl/{id}`

- 分享链接下载地址
- 会尽量走下载附件语义

## 域名绑定

你可以把某个域名直接绑定到某个桶  
绑定项当前包含这些字段：

- `host`
- `bucket`
- `prefix`
- `index_document`
- `spa_fallback`

### 字段释义

- `host` 要绑定的域名
- `bucket` 对应的桶
- `prefix` 可选，把这个域名的请求映射到桶内某个前缀下
- `index_document` 是否启用目录索引文档语义
- `spa_fallback` 是否在找不到对象时回退到索引文档，要求 `index_document` 为真

如果你想整阿里云 OSS 那种直接托管一个静态网站，那这些应该够你用了

## 分享链接是否会绕过桶权限

不会完全绕过  
分享链接本质上是“拿到一个受控访问令牌，再按已认证访问去走桶策略”

这意味着：

- `authenticated` 可以
- `public`        也可以
- `deny`          死也不行

所以如果你有明确不想见光的东西，写 `deny` 就能给你守口如瓶

## S3 凭据

### 权限模型

S3 凭据支持两级权限语义：

- `read_write`
- `read_only`

同时还能限制到指定桶集合  
所以一个比较正常的做法是：

- 应用 A 给一组只读 key
- 应用 B 给一组只写某几个桶的 key

## 预签名链接

### 预签名依赖什么

后台支持基于现有 S3 凭据生成预签名 URL  
这套逻辑会用到：

- `s3_base_url`
- `region`
- 一组满足读写要求的 S3 凭据

所以如果你发现预签名地址长得不对，先查：

- `s3_base_url` 是否填错
- `region` 是否和客户端预期不一致
- 有没有可用的 S3 凭据

下一步建议：

- 对链接和权限有疑问，可以去翻 [FAQ](./faq.md)
- 想看权限用到哪了，看 [存储模型](./storage-model.md)
