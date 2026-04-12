# 部署建议

本关考验你自主决策能力，仅作建议不做强制要求

## 推荐目录结构

Linux 上建议这么摆：

```bash
root@xcnya:/opt/bares3# ls -la
total 16036
drwxr-xr-x  4 root root     4096 Apr 12 07:41 .
drwxr-xr-x 10 root root     4096 Apr 12 07:08 ..
drwxr-xr-x  4 root root     4096 Apr 12 07:12 data
drwxr-xr-x  2 root root     4096 Apr 13 00:19 logs
-rwxr-xr-x  1 root root 16396472 Apr 12 07:41 bares3d
-rw-------  1 root root      550 Apr 12 07:10 config.yml
```

我比较喜欢全窝在 opt 里，你也可以窝在其他地方，也可以不窝  
不过你们不觉得一个对系统侵入性不强的东西要把自己的东西散落在各个角落很乱吗

这样做的好处：

- 便于管理
- 便于跑路

## 可能的网络暴露方式

### 三个入口分别看

需要把三个入口分开看：

- `admin` 只给自己或内网用，在需要 replication 的场景下再考虑把它暴露出去或者进行反向代理
- `s3` 给 SDK、CLI、应用程序用
- `file` 给公开文件、分享链接、域名绑定用，当然你也可以把这个仅绑定在本地，然后反向代理出去

## systemd 示例

你完全可以自己写一个，以下仅做示例：

```ini
[Unit]
Description=BareS3 Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/bares3
ExecStart=/opt/bares3/bares3d serve --config /opt/bares3/config.yml
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

## 关于反向代理

请参考 [反向代理](./reverse-proxy.md)

## 健康检查与监控

### 默认检查点

BareS3 自带这些检查点：

- 管理端 `GET /healthz`
- 管理端 `GET /readyz`
- 管理端 `GET /metrics`
- S3 端 `GET /healthz`
- S3 端 `GET /readyz`
- 文件端 `GET /healthz`
- 文件端 `GET /readyz`

如果你用容器编排或者外部监控，那这些应该够你用了  
如果你想看返回体长什么样，请参考 [API](./api.md)

## 关于备份

### 最省事的备份方式

很直接，直接把你的一窝打包带走  
比如你是这样窝的

```bash
root@xcnya:/opt/bares3# ls -la
total 16036
drwxr-xr-x  4 root root     4096 Apr 12 07:41 .
drwxr-xr-x 10 root root     4096 Apr 12 07:08 ..
drwxr-xr-x  4 root root     4096 Apr 12 07:12 data
drwxr-xr-x  2 root root     4096 Apr 13 00:19 logs
-rwxr-xr-x  1 root root 16396472 Apr 12 07:41 bares3d
-rw-------  1 root root      550 Apr 12 07:10 config.yml
```

那你直接打包 `/opt/bares3`  
当然你如果只想带文件跑路那你直接打包 `/opt/bares3/data` 或者你的 `data_dir` 就好了  

## 关于升级

### 一个可能的升级流程

一个可能的升级流程：

1. 停服务（`systemctl stop bares3`）
2. 替换二进制文件
3. 赋予文件执行权限（`chmod +x bares3d`）
4. 启动服务（`systemctl start bares3`）  
   如果你懒，可以把 1、4 合为一步  
   （即替换二进制文件且赋予执行权限后执行 `systemctl restart bares3`）
5. 看 `healthz`、`readyz` 和后台页面是否正常

内部状态库带 migration，一般情况下不会大改结构  
但这不等于你可以不备份

## Windows 部署

当然可以  
很显然开发者作为一个忠实的 Windows 用户，肯定考虑了 Windows 路径和一些特殊文件名的问题  
但如果你是要长期稳定托管，Linux 依然是一个不错的选择

下一步建议：

- 想看服务内部调度，看 [架构概览](./architecture.md)
- 想看怎么存的文件，看 [存储模型](./storage-model.md)
