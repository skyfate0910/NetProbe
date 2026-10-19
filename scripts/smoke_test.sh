#!/usr/bin/env bash
# NetDugger 手动验收脚本：先跑自动化测试，再起好对端并给出逐项验收清单
set -u
cd "$(dirname "$0")/.."

echo "== 1. 自动化测试（TCP 客户端/服务器、UDP 单播、组播回环、自动重连）=="
cargo test --quiet || exit 1
echo "   全部通过。"
echo

if ! command -v nc >/dev/null 2>&1; then
    echo "未找到 nc（netcat-openbsd），对打部分请自行准备对端后继续。"
    exit 0
fi

if command -v socat >/dev/null 2>&1; then
    TCP_ECHO="socat TCP-LISTEN:9000,reuseaddr,fork EXEC:cat"
    UDP_ECHO="socat UDP4-RECVFROM:9001,reuseaddr,fork EXEC:cat"
    echo "== 2. 启动回声对端（Ctrl-C 结束）=="
    echo "   $TCP_ECHO"
    echo "   $UDP_ECHO"
    echo "   两台回声端已在后台运行（PID 记录在 /tmp/netdugger_smoke.pid）"
    socat TCP-LISTEN:9000,reuseaddr,fork EXEC:cat &
    echo $! > /tmp/netdugger_smoke.pid
    socat UDP4-RECVFROM:9001,reuseaddr,fork EXEC:cat &
    echo $! >> /tmp/netdugger_smoke.pid
else
    echo "== 2. 未找到 socat，请另开终端手动起对端 =="
    echo "   TCP 接收端（不回发）: nc -l 9000"
    echo "   UDP 接收端（不回发）: nc -u -l 9001"
fi
echo

echo "== 3. 启动 cargo run，按清单逐项验证 =="
cat <<'EOF'
基础收发
[ ] 创建客户端 → TCP 127.0.0.1:9000 → 连接 → 发文本 → 回声显示在接收区，计数/速度变化
[ ] 十六进制发送 01 02 FF → 回声区勾十六进制后显示 01 02 FF
[ ] UDP 单播 → 127.0.0.1:9001 连接 → 互发正常，接收带 [来源地址] 前缀
[ ] 发送新行勾选 → socat/nc 侧每包以 CRLF 结尾
[ ] 时间戳勾选 → 接收区每包前带 [HH:MM:SS]

多客户端 / 组播
[ ] 创建服务器 → TCP :9000 → 启动 → 两个终端 nc 127.0.0.1 9000 → 左列显示 2 个在线
[ ] 发送目标选「全部客户端」→ 两端都收到；选某个客户端 → 只有它收到
[ ] 勾「发送接收到的数据」(echo) → 某 nc 发 X，只有该 nc 收回 X
[ ] UDP 组播 239.255.0.1:5000 → 两台 NetDugger（或另一台机器）互收组播包

自动功能
[ ] 自动发送 100ms → 速度栏发送速度 ≈ 包大小×10 B/s；停止后归零
[ ] TCP 客户端勾自动连接(1s) → Ctrl-C 杀对端再重启 → 自动重连，期间点发送不丢包

持久化
[ ] 改动会话参数/勾选项 → 重启程序 → 会话列表、参数、勾选状态、发送内容全部恢复
[ ] 配置文件在 ~/.config/netdugger/config.json（旧目录下的 netdugger_config.json 会自动迁移）

接收区
[ ] 暂停显示 → 接收区不再追加，但计数继续增长
[ ] 保存到文件（实时）→ 勾选并填路径 → 收到的字节实时写入
[ ] 「保存」按钮 → 导出接收区快照 netdugger_recv_<时间戳>.txt
EOF
echo
echo "启动: cargo run    （验收完成后: kill $(cat /tmp/netdugger_smoke.pid 2>/dev/null) 2>/dev/null）"
