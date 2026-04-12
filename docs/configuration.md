# 配置说明

BareS3 的配置分成两类：

- 静态启动配置，放在 `config.yml`
- 运行时配置，放在内部状态库里，通过控制台修改

## 配置文件结构

一个比较接近默认值的配置大概这样：

```yaml
app:
  env: production

paths:
  data_dir: ./data
  log_dir: ./logs
  tmp_dir: ./data/.bares3/tmp

listen:
  admin: 127.0.0.1:19080
  s3: 0.0.0.0:9000
  file: 0.0.0.0:9001

auth:
  console:
    username: admin
    password_hash: "$argon2id$..."
    session_secret: "..."
    session_ttl_minutes: 10080

logging:
  level: info
  format: pretty
  rotate_size_mb: 16
  rotate_keep: 10
```

## 静态配置项

### `app`

- `env` 可选 `production` / `development`  

其实没啥用，但是为了防止我以后干涉你的生产环境行为所以我建议你写 `production`

### `paths`

- `data_dir` 数据目录，桶、对象文件、状态库都在这里面
- `log_dir` 日志目录
- `tmp_dir` 临时目录，用来处理中转写入和 multipart
  
**需要注意的**：  
`tmp_dir` 必须和 `data_dir` 在同一个卷上  
这是为了保证原子移动，毕竟你也不想传一半丢文件吧

### `listen`

- `admin` 管理后台监听地址，默认 `127.0.0.1:19080`
- `s3` S3 服务监听地址，默认 `0.0.0.0:9000`
- `file` 文件服务监听地址，默认 `0.0.0.0:9001`

**需要注意的**：
`admin` 默认只监听本地回环，按理来说应当这样  
当然你可以使用你喜欢的 Web 服务器进行反向代理，参见 [反向代理](./reverse-proxy.md)

### `auth.console`

- `username` 控制台用户名
- `password_hash` 控制台密码的 Argon2id 哈希

   正常人不要自己手搓，直接用 `bares3d init` 或 `bares3d resetpassword`

- `session_secret` 用来签发控制台会话 Cookie 的随机密钥
- `session_ttl_minutes` 控制台会话有效期，单位分钟

### `logging`

- `level` 日志级别，默认 `info`  
  可选: `warn` / `error` / `debug`

- `format` 可选 `pretty` / `json`
- `rotate_size_mb` 单个日志文件轮转大小，单位 MB
- `rotate_keep` 最多保留多少个轮转文件

## 运行时配置

下面这些配置不走 `config.yml`，而是保存在内部状态库里：

- `public_base_url`
- `s3_base_url`
- `region`
- `metadata_layout`
- `max_bytes`
- 公共域名绑定
- 同步开关

你可以在控制台里改它们。  
其中 `tmp_dir` 比较特殊，控制台修改后会回写 `config.yml`，且需要重启 BareS3 后生效

## 运行时配置项

### `public_base_url`

- 文件服务的对外基础地址
- 分享链接、下载链接之类的会基于它生成

### `s3_base_url`

- S3 服务的对外基础地址
- 预签名链接会基于它生成

### `region`

- S3 区域名
- SigV4 校验和一些响应头会用到它

### `metadata_layout`

- 当前实现只支持 `hidden-dir`
- 你可以把它理解成“控制信息放进隐藏控制目录”的布局模式
- 目前只有这一个，别乱改

### `max_bytes`

- 实例总容量上限
- `0` 代表不限制
- 桶级 quota 不能高于实例级 quota

## 配置文件查找规则

- 如果你显式传了 `--config`，那就会用你传的配置文件
- 如果没有，会去寻找找二进制文件同级目录的 `config.yml`


下一步建议：

- 准备正式上线，看 [部署建议](./deployment.md)
- 想看这些配置用在哪了，看 [架构概览](./architecture.md)
