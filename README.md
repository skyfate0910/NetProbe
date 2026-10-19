# NetProbe — TCP/UDP 网络调试助手

NetProbe 是一款面向嵌入式与工业设备联调的 TCP/UDP 网络调试助手，基于 Rust 与 egui 开发，在 Linux 桌面以原生窗口运行，并打包为 deb 一键安装。

支持四类会话：TCP 客户端、TCP 服务器（多客户端接入，可定向或群发）、UDP 单播、UDP 组播（可指定网卡与 TTL）。收发支持文本与十六进制，十六进制输入自动按字节分组；可定时循环发送、发送文件；TCP 客户端带连接超时与断线重连。

本地IP 与网卡以下拉列出本机地址并支持一键刷新，便于多网卡环境锁定链路。接收区支持时间戳、暂停显示、实时存盘与复制，并独立统计收发字节与速率。会话与选项自动保存，重启即恢复。

## 安装

```bash
sudo dpkg -i dist/netprobe_0.1.0_amd64.deb
```

在启动器中搜索 “NetProbe”，或直接执行 `netprobe`。卸载：`sudo dpkg -r netprobe`。

## 从源码构建

```bash
cargo build --release        # 编译
cargo test                   # 传输层测试
bash packaging/build_deb.sh  # 打包 deb
bash scripts/smoke_test.sh   # 自动测试 + 起对端 + 手动验收清单
```

## 配置文件

会话列表与各会话选项保存在 `~/.config/netprobe/config.json`（可用环境变量 `NETPROBE_CONFIG` 覆盖）。
旧名 NetDugger 的配置会在首次启动时自动迁移。

## 说明

本工具是 TCP/UDP 调试助手，不做抓包、串口、Modbus 与广播包。IPv4 only。
