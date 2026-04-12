# 自行编译

如果你只是想快点用，那还是去 Release 拿现成二进制最省事  
既然你看这篇，那你肯定是要用这篇（废话）

## 先说要点

BareS3 不是一个纯 Go 项目  
它是：

- Go 服务端
- React 前端
- 前端构建产物嵌进 Go 二进制

所以你需要这篇教程

## 环境要求

- `Go >= 1.25`
- `Node.js >= 22`
- `pnpm >= 10.10.0`

## 拉仓库

```bash
git clone https://github.com/BLxcwg666/BareS3/
cd BareS3
```

## 安装前端依赖

```bash
pnpm install
```

## 构建流程

### 标准构建命令

```bash
pnpm build:webui
go build -C apps/bares3-server -o ../../bares3d ./cmd/bares3d
```

这两步分别做了：

### `pnpm build:webui`

它会：

1. 构建前端
2. 把前端 `dist` 同步到 `apps/bares3-server/internal/webui/dist`

这一步很重要。  
因为服务端是靠 `go:embed` 把这个目录打进二进制里的。

### `go build ...`

这一步就是正式编译服务端二进制。  
如果上一步没做，或者 `internal/webui/dist` 不存在，那你大概率直接编译不过。

## 只改 Go 代码时

### 后端单独重编译

如果你只是改了后端逻辑，前端根本没动，而且 `apps/bares3-server/internal/webui/dist` 已经存在，那你可以直接：

```bash
go build -C apps/bares3-server -o ../../bares3d ./cmd/bares3d
```

## 改了前端时

### 前端改动后的完整重编译

只要你改了：

- `apps/bares3-frontend/src/*`
- 前端依赖
- 前端构建配置

那你当然得：

```bash
pnpm build:webui
go build -C apps/bares3-server -o ../../bares3d ./cmd/bares3d
```

## 本地开发

### 双终端开发方式

本地开发那你分开跑是没问题的  

终端 1，启动前端：
```bash
pnpm dev:frontend
```

终端 2，启动 BareS3 服务端：

```bash
go run -C apps/bares3-server ./cmd/bares3d serve --config ../../config.yml
```

默认情况下，Vite Dev Server 会把 `/api` 代理到：

- `http://127.0.0.1:19080`

所以你不需要操心 API 代理之类的事

## 如果你想改前端代理目标

### 修改 Vite 代理目标

可以自己设 `VITE_ADMIN_PROXY`  
比如：

```bash
VITE_ADMIN_PROXY=http://127.0.0.1:29080 pnpm dev:frontend
```

这个主要用于你后台没跑在默认 `19080` 的情况

## 产物在哪

按上面的命令，最终二进制会出现在仓库根目录：

- `./bares3d`

如果你想自己改输出路径，直接改 `go build -o` 就行

## 编译完成后

### 至少要确认的事情

你至少要确定

- 能通过编译
- 能正常启动
- 你改的地方正常工作
- 没改炸别的地方

不过如果你要给本项目开 PR，那你要确保

- 上述所有内容
- 没有明显的 UI 错误
- 代码逻辑清晰
- 代码符合项目风格

下一步建议：

- 编译完准备开跑，看 [快速开始](./getting-started.md)
- 编译完准备上线，看 [部署建议](./deployment.md)
