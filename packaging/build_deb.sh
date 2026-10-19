#!/usr/bin/env bash
# 构建 NetProbe 的 deb 安装包（Debian/Ubuntu/Deepin amd64）
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)

PKG=netprobe
VERSION=$(grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
ARCH=amd64
OUT=${ROOT}/dist

echo "== 1/4 编译 release 二进制 =="
cargo build --release
strip -s target/release/${PKG}

STAGE=${OUT}/${PKG}_${VERSION}_${ARCH}
rm -rf "${STAGE}"
mkdir -p "${STAGE}/DEBIAN"
mkdir -p "${STAGE}/usr/bin"
mkdir -p "${STAGE}/usr/share/applications"
mkdir -p "${STAGE}/usr/share/doc/${PKG}"

echo "== 2/4 复制二进制与图标 =="
install -m 0755 target/release/${PKG} "${STAGE}/usr/bin/${PKG}"

for size in 16 32 48 64 128 256 512; do
    src="assets/${PKG}_${size}.png"
    [ -f "$src" ] || continue
    dir="${STAGE}/usr/share/icons/hicolor/${size}x${size}/apps"
    mkdir -p "$dir"
    install -m 0644 "$src" "$dir/${PKG}.png"
done

echo "== 3/4 写入 desktop 与元数据 =="
cat > "${STAGE}/usr/share/applications/${PKG}.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Version=1.0
Name=NetProbe
Name[zh_CN]=NetProbe 网络调试助手
GenericName=TCP/UDP Debug Tool
GenericName[zh_CN]=TCP/UDP 调试工具
Comment=TCP/UDP network debugging assistant (client/server, UDP unicast/multicast)
Comment[zh_CN]=TCP/UDP 网络调试助手：客户端、服务器、UDP 单播与组播
Exec=netprobe
Icon=netprobe
Terminal=false
Categories=Network;
Keywords=TCP;UDP;network;debug;socket;multicast;调试;网络;
StartupNotify=true
StartupWMClass=netprobe
EOF

cat > "${STAGE}/DEBIAN/control" <<EOF
Package: ${PKG}
Version: ${VERSION}
Section: net
Priority: optional
Architecture: ${ARCH}
Maintainer: NetProbe <netprobe@localhost>
Installed-Size: $(du -sk "${STAGE}/usr" | cut -f1)
Depends: libc6 (>= 2.35), libgcc-s1, libx11-6, libxcursor1, libxrandr2, libxi6, libxkbcommon0, libxkbcommon-x11-0, libgl1, libegl1, libwayland-client0, libwayland-cursor0, libwayland-egl1, libxcb1
Description: TCP/UDP network debugging assistant
 TCP/UDP debugging tool with TCP client/server, UDP unicast and UDP
 multicast sessions, hex/text send and receive, periodic auto send,
 auto reconnect, and per-session counters and throughput.
 .
 一个 TCP/UDP 网络调试助手：支持 TCP 客户端、TCP 服务器（多客户端）、
 UDP 单播与组播；文本/十六进制收发、定时自动发送、断线自动重连、
 收发计数与速率统计、接收内容保存到文件。
EOF

cat > "${STAGE}/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
if [ -x /usr/bin/update-desktop-database ]; then
    update-desktop-database -q /usr/share/applications || true
fi
if [ -x /usr/bin/gtk-update-icon-cache ]; then
    gtk-update-icon-cache -q -t -f /usr/share/icons/hicolor || true
fi
exit 0
EOF
chmod 0755 "${STAGE}/DEBIAN/postinst"

cat > "${STAGE}/DEBIAN/postrm" <<'EOF'
#!/bin/sh
set -e
if [ -x /usr/bin/update-desktop-database ]; then
    update-desktop-database -q /usr/share/applications || true
fi
if [ -x /usr/bin/gtk-update-icon-cache ]; then
    gtk-update-icon-cache -q -t -f /usr/share/icons/hicolor || true
fi
exit 0
EOF
chmod 0755 "${STAGE}/DEBIAN/postrm"

if [ -f README.md ]; then
    install -m 0644 README.md "${STAGE}/usr/share/doc/${PKG}/README.md"
fi

echo "== 4/4 生成 deb =="
dpkg-deb --build --root-owner-group "${STAGE}" "${OUT}/${PKG}_${VERSION}_${ARCH}.deb"
dpkg-deb --info "${OUT}/${PKG}_${VERSION}_${ARCH}.deb"
echo
echo "包已生成: ${OUT}/${PKG}_${VERSION}_${ARCH}.deb"
echo "安装:  sudo dpkg -i ${OUT}/${PKG}_${VERSION}_${ARCH}.deb"
echo "卸载:  sudo dpkg -r ${PKG}"
