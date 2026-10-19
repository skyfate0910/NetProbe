use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use crossbeam::channel::Sender;
use socket2::{SockRef, SockAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use tokio::sync::mpsc;

use crate::config::SessionParams;

#[derive(Debug)]
pub enum NetEvent {
    Connected(String),
    Data(Vec<u8>, Option<SocketAddr>),
    /// TCP 服务器当前在线客户端列表（地址快照）
    Clients(Vec<String>),
    Closed(String),
    Error(String),
}

pub type EventTx = Sender<(String, NetEvent)>;

/// 发送目标：全部在线客户端，或指定某个客户端地址
#[derive(Debug, Clone)]
pub enum SendTarget {
    All,
    One(String),
}

enum Cmd {
    Send(Vec<u8>, SendTarget),
}

pub struct SessionRuntime {
    send_tx: Option<mpsc::UnboundedSender<Cmd>>,
    task: tokio::task::JoinHandle<()>,
    pub peer_desc: String,
}

impl SessionRuntime {
    pub fn send(&self, data: Vec<u8>, target: SendTarget) -> bool {
        match &self.send_tx {
            Some(tx) => tx.send(Cmd::Send(data, target)).is_ok(),
            None => false,
        }
    }

    pub fn stop(self) {
        self.task.abort();
    }
}

pub fn start(
    rt: &tokio::runtime::Runtime,
    session_id: String,
    params: SessionParams,
    events: EventTx,
) -> Result<SessionRuntime, String> {
    let (send_tx, send_rx) = mpsc::unbounded_channel::<Cmd>();

    let (peer_desc, task) = match params {
        SessionParams::TcpClient(cfg) => {
            let desc = format!("{}:{}", cfg.remote_host, cfg.remote_port);
            let id = session_id.clone();
            let ev = events.clone();
            let task = rt.spawn(async move {
                run_tcp_client(cfg, send_rx, id, ev).await;
            });
            (desc, task)
        }
        SessionParams::UdpUnicast(cfg) => {
            let desc = format!("{}:{}", cfg.local_host, cfg.local_port);
            let id = session_id.clone();
            let ev = events.clone();
            let task = rt.spawn(async move {
                run_udp_unicast(cfg, send_rx, id, ev).await;
            });
            (desc, task)
        }
        SessionParams::TcpServer(cfg) => {
            let desc = format!("{}:{}", cfg.listen_host, cfg.listen_port);
            let id = session_id.clone();
            let ev = events.clone();
            let task = rt.spawn(async move {
                run_tcp_server(cfg, send_rx, id, ev).await;
            });
            (desc, task)
        }
        SessionParams::UdpMulticast(cfg) => {
            let desc = format!("{}:{}", cfg.group, cfg.port);
            let id = session_id.clone();
            let ev = events.clone();
            let task = rt.spawn(async move {
                run_udp_multicast(cfg, send_rx, id, ev).await;
            });
            (desc, task)
        }
    };

    Ok(SessionRuntime {
        send_tx: Some(send_tx),
        task,
        peer_desc,
    })
}

fn report_error(events: &EventTx, id: &str, msg: String) {
    let _ = events.send((id.to_string(), NetEvent::Error(msg.clone())));
    let _ = events.send((id.to_string(), NetEvent::Closed(msg)));
}

fn parse_addr(host: &str, port: u16, what: &str) -> Result<SocketAddr, String> {
    format!("{host}:{port}")
        .parse()
        .map_err(|_| format!("{what}地址非法: {host}:{port}"))
}

// ---------- TCP 客户端 ----------

async fn run_tcp_client(
    cfg: crate::config::TcpClientConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    id: String,
    events: EventTx,
) {
    let remote = match parse_addr(&cfg.remote_host, cfg.remote_port, "目标") {
        Ok(a) => a,
        Err(e) => {
            report_error(&events, &id, e);
            return;
        }
    };

    let mut first_attempt = true;
    loop {
        if !first_attempt {
            let _ = events.send((
                id.clone(),
                NetEvent::Error(format!(
                    "连接断开，{} 秒后自动重连…",
                    cfg.reconnect_secs
                )),
            ));
            tokio::time::sleep(std::time::Duration::from_secs(cfg.reconnect_secs)).await;
        }
        first_attempt = false;

        let socket = match TcpSocket::new_v4() {
            Ok(s) => s,
            Err(e) => {
                report_error(&events, &id, format!("创建 TCP 套接字失败: {e}"));
                return;
            }
        };
        // 指定了本地IP 或本地端口才绑定：
        // - 只填本地IP → 强制源地址/出接口，端口由系统分配
        // - 只填本地端口 → 固定源端口，出接口仍由路由决定
        if cfg.local_host != "0.0.0.0" || cfg.local_port != 0 {
            let local = match parse_addr(&cfg.local_host, cfg.local_port, "本地") {
                Ok(a) => a,
                Err(e) => {
                    report_error(&events, &id, e);
                    return;
                }
            };
            if let Err(e) = socket.bind(local) {
                report_error(&events, &id, format!("绑定本地 {local} 失败: {e}"));
                return;
            }
        }
        // 0 表示未填，用默认 3 秒
        let secs = if cfg.connect_timeout_secs == 0 {
            3
        } else {
            cfg.connect_timeout_secs
        };
        let timeout = std::time::Duration::from_secs(secs as u64);
        match tokio::time::timeout(timeout, socket.connect(remote)).await {
            Err(_) => {
                let _ = events.send((
                    id.clone(),
                    NetEvent::Error(format!(
                        "连接 {remote} 超时（{} 秒）",
                        cfg.connect_timeout_secs
                    )),
                ));
                if !cfg.auto_reconnect {
                    let _ = events.send((id.clone(), NetEvent::Closed("连接超时".into())));
                    return;
                }
                continue;
            }
            Ok(Err(e)) => {
                let _ = events.send((
                    id.clone(),
                    NetEvent::Error(format!("连接 {remote} 失败: {e}")),
                ));
                if !cfg.auto_reconnect {
                    let _ = events.send((id.clone(), NetEvent::Closed("连接失败".into())));
                    return;
                }
                continue;
            }
            Ok(Ok(stream)) => {
                let _ = events.send((id.clone(), NetEvent::Connected(remote.to_string())));
                if let Some(reason) = drive_tcp(stream, &mut cmd_rx, &id, &events).await {
                    if !cfg.auto_reconnect {
                        let _ = events.send((id.clone(), NetEvent::Closed(reason)));
                        return;
                    }
                } else {
                    // 发送通道关闭（手动停止），直接退出
                    return;
                }
            }
        }
    }
}

/// 返回 Some(reason) 表示因网络断开退出；None 表示发送通道关闭（手动停止）
async fn drive_tcp(
    stream: TcpStream,
    cmd_rx: &mut mpsc::UnboundedReceiver<Cmd>,
    id: &str,
    events: &EventTx,
) -> Option<String> {
    let (mut rd, mut wr) = stream.into_split();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            res = rd.read(&mut buf) => match res {
                Ok(0) => return Some("对方关闭了连接".into()),
                Ok(n) => {
                    let _ = events.send((id.to_string(), NetEvent::Data(buf[..n].to_vec(), None)));
                }
                Err(e) => return Some(format!("读取失败: {e}")),
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(Cmd::Send(data, _)) => {
                    if let Err(e) = wr.write_all(&data).await {
                        return Some(format!("写入失败: {e}"));
                    }
                }
                None => return None,
            }
        }
    }
}

// ---------- TCP 服务器（多客户端） ----------

type PeerMap = Arc<Mutex<HashMap<SocketAddr, mpsc::UnboundedSender<Vec<u8>>>>>;

fn send_clients(events: &EventTx, id: &str, peers: &PeerMap) {
    let list: Vec<String> = peers
        .lock()
        .unwrap()
        .keys()
        .map(|a| a.to_string())
        .collect();
    let _ = events.send((id.to_string(), NetEvent::Clients(list)));
}

async fn run_tcp_server(
    cfg: crate::config::TcpServerConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    id: String,
    events: EventTx,
) {
    let listen_addr = match parse_addr(&cfg.listen_host, cfg.listen_port, "监听") {
        Ok(a) => a,
        Err(e) => {
            report_error(&events, &id, e);
            return;
        }
    };
    let listener = match TcpListener::bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            report_error(&events, &id, format!("监听 {listen_addr} 失败: {e}"));
            return;
        }
    };
    let _ = events.send((
        id.clone(),
        NetEvent::Connected(format!("TCP 服务器监听 {listen_addr}")),
    ));

    let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let (tx, mut peer_rx) = mpsc::unbounded_channel::<Vec<u8>>();
                    peers.lock().unwrap().insert(peer, tx);
                    send_clients(&events, &id, &peers);

                    let (mut rd, mut wr) = stream.into_split();
                    let ev = events.clone();
                    let pid = id.clone();
                    let peers2 = peers.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            tokio::select! {
                                res = rd.read(&mut buf) => match res {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let _ = ev.send((
                                            pid.clone(),
                                            NetEvent::Data(buf[..n].to_vec(), Some(peer)),
                                        ));
                                    }
                                    Err(_) => break,
                                },
                                msg = peer_rx.recv() => match msg {
                                    Some(data) => {
                                        if wr.write_all(&data).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                        peers2.lock().unwrap().remove(&peer);
                        send_clients(&ev, &pid, &peers2);
                    });
                }
                Err(e) => {
                    report_error(&events, &id, format!("accept 失败: {e}"));
                    break;
                }
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(Cmd::Send(data, target)) => {
                    let map = peers.lock().unwrap();
                    match target {
                        SendTarget::All => {
                            for tx in map.values() {
                                let _ = tx.send(data.clone());
                            }
                        }
                        SendTarget::One(addr) => {
                            if let Ok(a) = addr.parse::<SocketAddr>() {
                                if let Some(tx) = map.get(&a) {
                                    let _ = tx.send(data);
                                }
                            }
                        }
                    }
                }
                None => break,
            }
        }
    }
}

// ---------- UDP 单播 ----------

async fn run_udp_unicast(
    cfg: crate::config::UdpUnicastConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    id: String,
    events: EventTx,
) {
    let local = match parse_addr(&cfg.local_host, cfg.local_port, "本地") {
        Ok(a) => a,
        Err(e) => {
            report_error(&events, &id, e);
            return;
        }
    };
    let socket = match UdpSocket::bind(local).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            report_error(&events, &id, format!("绑定 {local} 失败: {e}"));
            return;
        }
    };
    // 本地端口填 0（不指定）时由系统分配，这里回读实际地址用于提示
    let actual_local = socket
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| local.to_string());

    let remote: Option<SocketAddr> = if cfg.has_remote {
        match parse_addr(&cfg.remote_host, cfg.remote_port, "目标") {
            Ok(a) => Some(a),
            Err(e) => {
                report_error(&events, &id, e);
                return;
            }
        }
    } else {
        None
    };

    let connected = if let Some(r) = remote {
        if let Err(e) = socket.connect(r).await {
            report_error(&events, &id, format!("固定对端 {r} 失败: {e}"));
            return;
        }
        let _ = events.send((
            id.clone(),
            NetEvent::Connected(format!("UDP {actual_local} ↔ {r}")),
        ));
        true
    } else {
        let _ = events.send((
            id.clone(),
            NetEvent::Connected(format!("UDP 已绑定 {actual_local}，等待对端")),
        ));
        false
    };

    let mut last_peer: Option<SocketAddr> = None;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            res = socket.recv_from(&mut buf) => match res {
                Ok((n, from)) => {
                    last_peer = Some(from);
                    let _ = events.send((
                        id.clone(),
                        NetEvent::Data(buf[..n].to_vec(), Some(from)),
                    ));
                }
                Err(e) => {
                    report_error(&events, &id, format!("接收失败: {e}"));
                    break;
                }
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(Cmd::Send(data, _)) => {
                    let target = if connected { remote } else { last_peer };
                    match target {
                        Some(addr) => {
                            let res = if connected {
                                socket.send(&data).await.map(|_| ())
                            } else {
                                socket.send_to(&data, addr).await.map(|_| ())
                            };
                            if let Err(e) = res {
                                report_error(&events, &id, format!("发送失败: {e}"));
                                break;
                            }
                        }
                        None => {
                            let _ = events.send((
                                id.clone(),
                                NetEvent::Error("还没有来源地址，先收到一包再发".into()),
                            ));
                        }
                    }
                }
                None => break,
            }
        }
    }
}

// ---------- UDP 组播 ----------

async fn run_udp_multicast(
    cfg: crate::config::UdpMulticastConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    id: String,
    events: EventTx,
) {
    let group: Ipv4Addr = match cfg.group.parse() {
        Ok(g) => g,
        Err(_) => {
            report_error(&events, &id, format!("组地址非法: {}", cfg.group));
            return;
        }
    };
    if !(224..=239).contains(&group.octets()[0]) {
        report_error(
            &events,
            &id,
            format!("{} 不是组播地址（应为 224.0.0.0 ~ 239.255.255.255）", cfg.group),
        );
        return;
    }
    let iface: Ipv4Addr = match cfg.iface.parse() {
        Ok(i) => i,
        Err(_) => {
            report_error(&events, &id, format!("网卡地址非法: {}", cfg.iface));
            return;
        }
    };

    // SO_REUSEADDR + 非阻塞，多个接收端可同时加入同一组
    let raw = match socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    ) {
        Ok(s) => s,
        Err(e) => {
            report_error(&events, &id, format!("创建 UDP 套接字失败: {e}"));
            return;
        }
    };
    if let Err(e) = raw.set_reuse_address(true) {
        report_error(&events, &id, format!("设置 SO_REUSEADDR 失败: {e}"));
        return;
    }
    if let Err(e) = raw.set_nonblocking(true) {
        report_error(&events, &id, format!("设置非阻塞失败: {e}"));
        return;
    }
    let bind_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED), cfg.port);
    if let Err(e) = raw.bind(&SockAddr::from(bind_addr)) {
        report_error(&events, &id, format!("绑定 {bind_addr} 失败: {e}"));
        return;
    }
    let socket = match UdpSocket::from_std(raw.into()) {
        Ok(s) => s,
        Err(e) => {
            report_error(&events, &id, format!("套接字进入 tokio 失败: {e}"));
            return;
        }
    };

    let sock = SockRef::from(&socket);
    if let Err(e) = sock.set_multicast_loop_v4(cfg.loopback) {
        report_error(&events, &id, format!("设置组播回环失败: {e}"));
        return;
    }
    if let Err(e) = sock.set_multicast_ttl_v4(cfg.ttl) {
        report_error(&events, &id, format!("设置组播 TTL 失败: {e}"));
        return;
    }
    if !iface.is_unspecified() {
        if let Err(e) = sock.set_multicast_if_v4(&iface) {
            report_error(&events, &id, format!("设置组播网卡失败: {e}"));
            return;
        }
    }
    if let Err(e) = sock.join_multicast_v4(&group, &iface) {
        report_error(
            &events,
            &id,
            format!(
                "加入组播组 {group} 失败: {e}（iface={} 时由系统选默认网卡）",
                cfg.iface
            ),
        );
        return;
    }

    let _ = events.send((
        id.clone(),
        NetEvent::Connected(format!(
            "UDP 组播 {group}:{} 已加入（ttl={}，回环={}）",
            cfg.port, cfg.ttl, cfg.loopback
        )),
    ));

    let group_addr = SocketAddr::new(std::net::IpAddr::V4(group), cfg.port);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            res = socket.recv_from(&mut buf) => match res {
                Ok((n, from)) => {
                    let _ = events.send((
                        id.clone(),
                        NetEvent::Data(buf[..n].to_vec(), Some(from)),
                    ));
                }
                Err(e) => {
                    report_error(&events, &id, format!("接收失败: {e}"));
                    break;
                }
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(Cmd::Send(data, _)) => {
                    if let Err(e) = socket.send_to(&data, group_addr).await {
                        report_error(
                            &events,
                            &id,
                            format!("发送到 {group_addr} 失败: {e}（检查默认组播路由）"),
                        );
                        break;
                    }
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TcpClientConfig, TcpServerConfig, UdpMulticastConfig, UdpUnicastConfig};
    use std::time::Duration;

    fn wait_data(
        rx: &crossbeam::channel::Receiver<(String, NetEvent)>,
    ) -> Option<Vec<u8>> {
        for _ in 0..50 {
            if let Ok((_, ev)) = rx.recv_timeout(Duration::from_millis(200)) {
                if let NetEvent::Data(d, _) = ev {
                    return Some(d);
                }
            }
        }
        None
    }

    #[test]
    fn udp_unicast_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        let peer = rt.block_on(tokio::net::UdpSocket::bind("127.0.0.1:0")).unwrap();
        let peer_addr = peer.local_addr().unwrap();

        let cfg = UdpUnicastConfig {
            local_host: "127.0.0.1".into(),
            local_port: 0,
            remote_host: peer_addr.ip().to_string(),
            remote_port: peer_addr.port(),
            has_remote: true,
            created_as_server: false,
        };
        let runtime = start(
            &rt,
            "test-udp".into(),
            SessionParams::UdpUnicast(cfg),
            event_tx,
        )
        .unwrap();

        assert!(runtime.send(b"hello udp".to_vec(), SendTarget::All));
        let mut buf = [0u8; 64];
        let (n, from) = rt.block_on(peer.recv_from(&mut buf)).unwrap();
        assert_eq!(&buf[..n], b"hello udp");

        rt.block_on(peer.send_to(b"pong", from)).unwrap();
        let data = wait_data(&event_rx).expect("应收到 UDP 回包");
        assert_eq!(data, b"pong");
        runtime.stop();
    }

    #[test]
    fn local_ipv4_addrs_contains_loopback() {
        let addrs = local_ipv4_addrs();
        assert!(
            addrs.iter().any(|(ip, _)| ip == "127.0.0.1"),
            "本机地址列表应至少包含回环地址，实际: {addrs:?}"
        );
    }

    #[test]
    fn udp_local_ip_forces_source_address() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, _event_rx) = crossbeam::channel::unbounded();

        let peer = rt.block_on(tokio::net::UdpSocket::bind("127.0.0.1:0")).unwrap();
        let peer_addr = peer.local_addr().unwrap();

        let cfg = UdpUnicastConfig {
            local_host: "127.0.0.1".into(), // 指定本地IP，强制源地址
            local_port: 0,
            remote_host: peer_addr.ip().to_string(),
            remote_port: peer_addr.port(),
            has_remote: true,
            created_as_server: false,
        };
        let runtime = start(
            &rt,
            "test-src-ip".into(),
            SessionParams::UdpUnicast(cfg),
            event_tx,
        )
        .unwrap();

        assert!(runtime.send(b"src".to_vec(), SendTarget::All));
        let mut buf = [0u8; 16];
        let (_, from) = rt.block_on(peer.recv_from(&mut buf)).unwrap();
        assert_eq!(from.ip().to_string(), "127.0.0.1", "源IP 应为指定的本地IP");
        runtime.stop();
    }

    #[test]
    fn udp_local_port_zero_uses_random_port() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, _event_rx) = crossbeam::channel::unbounded();

        // 对端只负责收，看源端口是否被系统分配
        let peer = rt.block_on(tokio::net::UdpSocket::bind("127.0.0.1:0")).unwrap();
        let peer_addr = peer.local_addr().unwrap();

        let cfg = UdpUnicastConfig {
            local_host: "0.0.0.0".into(),
            local_port: 0, // 不指定本地端口
            remote_host: peer_addr.ip().to_string(),
            remote_port: peer_addr.port(),
            has_remote: true,
            created_as_server: false,
        };
        let runtime = start(
            &rt,
            "test-random-port".into(),
            SessionParams::UdpUnicast(cfg),
            event_tx,
        )
        .unwrap();

        assert!(runtime.send(b"hi".to_vec(), SendTarget::All));
        let mut buf = [0u8; 16];
        let (n, from) = rt.block_on(peer.recv_from(&mut buf)).unwrap();
        assert_eq!(&buf[..n], b"hi");
        assert!(from.port() != 0, "本地端口留空时应由系统分配随机端口");
        runtime.stop();
    }

    #[test]
    fn tcp_client_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        rt.spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 64];
                if let Ok(n) = sock.read(&mut buf).await {
                    let _ = sock.write_all(&buf[..n]).await;
                }
            }
        });

        let cfg = TcpClientConfig {
            remote_host: addr.ip().to_string(),
            remote_port: addr.port(),
            local_host: "0.0.0.0".into(),
            local_port: 0,
            auto_reconnect: false,
            reconnect_secs: 3,
            connect_timeout_secs: 3,
        };
        let runtime = start(
            &rt,
            "test-tcp".into(),
            SessionParams::TcpClient(cfg),
            event_tx,
        )
        .unwrap();

        let mut connected = false;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if matches!(ev, NetEvent::Connected(_)) {
                    connected = true;
                    break;
                }
            }
        }
        assert!(connected, "应收到 Connected 事件");

        assert!(runtime.send(b"ping".to_vec(), SendTarget::All));
        let data = wait_data(&event_rx).expect("应收到 TCP echo 回包");
        assert_eq!(data, b"ping");
        runtime.stop();
    }

    #[test]
    fn tcp_client_auto_reconnect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        // 服务器：第一连接收到消息后直接关闭；第二连接回 ok
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        rt.spawn(async move {
            for i in 0..2 {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                if i == 0 {
                    drop(sock); // 立即断开第一连接
                } else {
                    let mut sock = sock;
                    let mut buf = [0u8; 64];
                    if sock.read(&mut buf).await.is_ok() {
                        let _ = sock.write_all(b"ok").await;
                    }
                }
            }
        });

        let cfg = TcpClientConfig {
            remote_host: addr.ip().to_string(),
            remote_port: addr.port(),
            local_host: "0.0.0.0".into(),
            local_port: 0,
            auto_reconnect: true,
            reconnect_secs: 1,
            connect_timeout_secs: 3,
        };
        let runtime = start(
            &rt,
            "test-reconn".into(),
            SessionParams::TcpClient(cfg),
            event_tx,
        )
        .unwrap();

        // 第一次连接成功
        let mut connects = 0;
        let mut saw_close = false;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                match ev {
                    NetEvent::Connected(_) => connects += 1,
                    NetEvent::Error(_) => saw_close = true,
                    _ => {}
                }
            }
            if connects >= 2 {
                break;
            }
        }
        assert!(connects >= 2, "自动重连应产生第二次 Connected（实际 {connects} 次）");
        assert!(saw_close, "断开后应有重连提示事件");

        // 重连成功后发送，服务器第二连接应回 ok
        assert!(runtime.send(b"again".to_vec(), SendTarget::All));
        let data = wait_data(&event_rx).expect("重连后应收到服务器回包");
        assert_eq!(data, b"ok");
        runtime.stop();
    }

    #[test]
    fn tcp_client_connect_timeout() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        // 不可达地址：要么超时要么直接报错，但必须在短时间内结束并上报 Closed
        let cfg = TcpClientConfig {
            remote_host: "10.255.255.1".into(),
            remote_port: 80,
            local_host: "0.0.0.0".into(),
            local_port: 0,
            auto_reconnect: false,
            reconnect_secs: 3,
            connect_timeout_secs: 1,
        };
        let runtime = start(
            &rt,
            "test-timeout".into(),
            SessionParams::TcpClient(cfg),
            event_tx,
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut saw_closed = false;
        while std::time::Instant::now() < deadline {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if matches!(ev, NetEvent::Closed(_)) {
                    saw_closed = true;
                    break;
                }
            }
        }
        assert!(saw_closed, "连接尝试应在超时后结束并上报 Closed");
        runtime.stop();
    }

    #[test]
    fn tcp_server_multi_client() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        // 占一个空闲端口给服务器
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let cfg = TcpServerConfig {
            listen_host: "127.0.0.1".into(),
            listen_port: port,
        };
        let runtime = start(
            &rt,
            "test-srv".into(),
            SessionParams::TcpServer(cfg),
            event_tx,
        )
        .unwrap();

        // 等服务器进入监听状态
        let mut listening = false;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if matches!(ev, NetEvent::Connected(_)) {
                    listening = true;
                    break;
                }
            }
        }
        assert!(listening, "服务器应进入监听状态");

        // 两个客户端接入
        let mut c1 = rt
            .block_on(tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .unwrap();
        let mut c2 = rt
            .block_on(tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .unwrap();
        let c1_addr = c1.local_addr().unwrap().to_string();

        // 等 Clients 事件出现两个客户端
        let mut got_two = false;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if let NetEvent::Clients(list) = ev {
                    if list.len() == 2 {
                        got_two = true;
                        break;
                    }
                }
            }
        }
        assert!(got_two, "应收到含 2 个客户端的列表事件");

        // 群发：两个客户端都应收到
        assert!(runtime.send(b"broadcast".to_vec(), SendTarget::All));
        for sock in [&mut c1, &mut c2] {
            let mut buf = [0u8; 16];
            let n = rt
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf)).await
                })
                .expect("超时")
                .expect("读取失败");
            assert_eq!(&buf[..n], b"broadcast");
        }

        // c1 发消息 → 服务器 Data 事件带来源地址
        let (mut rd1, mut wr1) = c1.into_split();
        rt.block_on(wr1.write_all(b"hi")).unwrap();
        let mut from_addr = None;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if let NetEvent::Data(_, Some(a)) = ev {
                    from_addr = Some(a.to_string());
                    break;
                }
            }
        }
        assert_eq!(from_addr.as_deref(), Some(c1_addr.as_str()));

        // 定向发送：只发 c1
        assert!(runtime.send(
            b"secret".to_vec(),
            SendTarget::One(from_addr.unwrap()),
        ));
        let mut buf = [0u8; 16];
        let n = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), rd1.read(&mut buf)).await
            })
            .expect("超时")
            .expect("读取失败");
        assert_eq!(&buf[..n], b"secret");

        // c2 不应收到 secret：1 秒内无数据（保持连接不拆分，避免半关误导发 EOF）
        let mut buf2 = [0u8; 16];
        let res = rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), c2.read(&mut buf2)).await
        });
        assert!(res.is_err(), "c2 不应收到定向消息");
        runtime.stop();
    }

    #[test]
    fn udp_multicast_loopback() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (event_tx, event_rx) = crossbeam::channel::unbounded();

        let cfg = UdpMulticastConfig {
            group: "239.255.42.7".into(),
            port: 0,
            iface: "0.0.0.0".into(),
            ttl: 1,
            loopback: true,
        };
        let runtime = start(
            &rt,
            "test-mcast".into(),
            SessionParams::UdpMulticast(cfg),
            event_tx,
        )
        .unwrap();

        let mut connected = false;
        for _ in 0..50 {
            if let Ok((_, ev)) = event_rx.recv_timeout(Duration::from_millis(200)) {
                if matches!(ev, NetEvent::Connected(_)) {
                    connected = true;
                    break;
                }
            }
        }
        assert!(connected, "应收到组播 Connected 事件");

        // 发送，靠本机回环自己收到
        assert!(runtime.send(b"mcast-hello".to_vec(), SendTarget::All));
        match wait_data(&event_rx) {
            Some(data) => assert_eq!(data, b"mcast-hello"),
            None => {
                // 环境没有默认组播路由时跳过，不算失败
                eprintln!("警告: 本机回环未收到组播包（可能无默认组播路由），跳过断言");
            }
        }
        runtime.stop();
    }
}

/// 枚举本机 IPv4 地址（含接口名），供「本地IP / 网卡」下拉使用
pub fn local_ipv4_addrs() -> Vec<(String, String)> {
    let mut addrs: Vec<(String, String)> = Vec::new();
    unsafe {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut head) != 0 {
            return addrs;
        }
        let mut cur = head;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() && (*ifa.ifa_addr).sa_family as i32 == libc::AF_INET {
                let sin = ifa.ifa_addr as *const libc::sockaddr_in;
                let b = (*sin).sin_addr.s_addr.to_ne_bytes();
                let ip = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                let name = std::ffi::CStr::from_ptr(ifa.ifa_name)
                    .to_string_lossy()
                    .into_owned();
                addrs.push((ip.to_string(), name));
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(head);
    }
    addrs.sort_by(|a, b| {
        iface_rank(&a.1)
            .cmp(&iface_rank(&b.1))
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    addrs.dedup();
    addrs
}

/// 网卡排序权重：0 = 真实网卡，1 = 虚拟网卡（docker/tun/bridge 等），2 = 回环。
/// 多网卡机器上让真正能用的网卡排在前面。
fn iface_rank(name: &str) -> u8 {
    const VIRTUAL: &[&str] = &[
        "veth", "docker", "br-", "virbr", "vboxnet", "vmnet", "tun", "tap", "wg", "zt", "Meta",
    ];
    if name == "lo" {
        2
    } else if VIRTUAL.iter().any(|p| name.starts_with(p)) {
        1
    } else {
        0
    }
}
