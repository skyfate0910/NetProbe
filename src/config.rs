use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionKind {
    TcpClient,
    TcpServer,
    UdpUnicast,
    UdpMulticast,
}

impl SessionKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::TcpClient => "TCP 客户端",
            Self::TcpServer => "TCP 服务器",
            Self::UdpUnicast => "UDP 单播",
            Self::UdpMulticast => "UDP 组播",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpClientConfig {
    pub remote_host: String,
    pub remote_port: u16,
    /// 本地IP（源地址）；0.0.0.0 表示由路由决定。指定后强制使用该网卡
    #[serde(default = "default_any_host")]
    pub local_host: String,
    /// 本地端口；0 表示不指定，由系统分配随机空闲端口
    pub local_port: u16,
    pub auto_reconnect: bool,
    pub reconnect_secs: u64,
    /// 连接超时：超过则中断本次连接尝试
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u32,
}

fn default_connect_timeout() -> u32 {
    3
}

fn default_any_host() -> String {
    "0.0.0.0".into()
}

impl Default for TcpClientConfig {
    fn default() -> Self {
        Self {
            remote_host: "127.0.0.1".into(),
            remote_port: 7,
            local_host: "0.0.0.0".into(),
            local_port: 0,
            auto_reconnect: false,
            reconnect_secs: 3,
            connect_timeout_secs: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpServerConfig {
    pub listen_host: String,
    pub listen_port: u16,
}

impl Default for TcpServerConfig {
    fn default() -> Self {
        Self {
            listen_host: "0.0.0.0".into(),
            listen_port: 9000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpUnicastConfig {
    pub local_host: String,
    pub local_port: u16,
    pub remote_host: String,
    pub remote_port: u16,
    pub has_remote: bool,
    /// 是否以「创建服务器」方式创建：决定属性栏分组，勾掉指定对端不会移动条目
    #[serde(default)]
    pub created_as_server: bool,
}

impl Default for UdpUnicastConfig {
    fn default() -> Self {
        Self {
            local_host: "0.0.0.0".into(),
            local_port: 9001,
            remote_host: "127.0.0.1".into(),
            remote_port: 9001,
            has_remote: true,
            created_as_server: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpMulticastConfig {
    pub group: String,
    pub port: u16,
    pub iface: String,
    pub ttl: u32,
    pub loopback: bool,
}

impl Default for UdpMulticastConfig {
    fn default() -> Self {
        Self {
            group: "239.255.0.1".into(),
            port: 5000,
            iface: "0.0.0.0".into(),
            ttl: 1,
            loopback: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionParams {
    TcpClient(TcpClientConfig),
    TcpServer(TcpServerConfig),
    UdpUnicast(UdpUnicastConfig),
    UdpMulticast(UdpMulticastConfig),
}

impl SessionParams {
    pub fn kind(&self) -> SessionKind {
        match self {
            Self::TcpClient(_) => SessionKind::TcpClient,
            Self::TcpServer(_) => SessionKind::TcpServer,
            Self::UdpUnicast(_) => SessionKind::UdpUnicast,
            Self::UdpMulticast(_) => SessionKind::UdpMulticast,
        }
    }

    pub fn summary(&self) -> String {
        match self {
            Self::TcpClient(c) => format!("{}:{}", c.remote_host, c.remote_port),
            Self::TcpServer(c) => format!("{}:{}", c.listen_host, c.listen_port),
            Self::UdpUnicast(c) => {
                if c.has_remote {
                    format!(
                        "{}:{} → {}:{}",
                        c.local_host, c.local_port, c.remote_host, c.remote_port
                    )
                } else {
                    format!("{}:{}", c.local_host, c.local_port)
                }
            }
            Self::UdpMulticast(c) => format!("{}:{} ttl={}", c.group, c.port, c.ttl),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    pub id: String,
    pub name: String,
    pub params: SessionParams,
    #[serde(default)]
    pub prefs: UiPrefs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub sessions: Vec<SessionConfig>,
    pub selected: Option<String>,
    pub theme: String,
    pub font_size: f32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            sessions: Vec::new(),
            selected: None,
            theme: "light".into(),
            font_size: 14.0,
        }
    }
}

impl AppConfig {
    pub fn load() -> Self {
        let path = config_path();
        if let Ok(text) = std::fs::read_to_string(&path) {
            if let Ok(config) = serde_json::from_str::<AppConfig>(&text) {
                return config;
            }
        }
        // 新位置不存在或损坏时，尝试迁移旧名字的配置（不丢已保存的会话列表）
        for legacy in legacy_config_paths() {
            if legacy == path {
                continue;
            }
            if let Ok(text) = std::fs::read_to_string(&legacy)
                && let Ok(config) = serde_json::from_str::<AppConfig>(&text)
            {
                return config;
            }
        }
        Self::default()
    }

    pub fn save(&self) {
        let path = config_path();
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write(path, json);
        }
    }
}

// ---------- 收发区选项持久化 ----------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiPrefs {
    pub hex_send: bool,
    pub hex_recv: bool,
    pub auto_send: bool,
    pub interval_ms: u32,
    pub echo: bool,
    pub show_timestamp: bool,
    /// 旧配置：勾选「发送新行」时同时追加 0D 0A。新配置拆成下面两项。
    #[serde(default)]
    pub append_newline: bool,
    #[serde(default)]
    pub append_cr: bool,
    #[serde(default)]
    pub append_lf: bool,
    pub save_enabled: bool,
    pub save_path: String,
    pub send_text: String,
}

impl Default for UiPrefs {
    fn default() -> Self {
        Self {
            hex_send: false,
            hex_recv: false,
            auto_send: false,
            interval_ms: 100,
            echo: false,
            show_timestamp: false,
            append_newline: false,
            append_cr: false,
            append_lf: false,
            save_enabled: false,
            save_path: String::new(),
            send_text: String::new(),
        }
    }
}

/// 配置文件位置：优先 NETPROBE_CONFIG 环境变量，其次 ~/.config/netprobe/config.json
pub fn config_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("NETPROBE_CONFIG") {
        return p.into();
    }
    if let Ok(home) = std::env::var("HOME") {
        return format!("{home}/.config/netprobe/config.json").into();
    }
    "netprobe_config.json".into()
}

/// 旧名字（NetDugger）时期的配置位置，仅用于一次性迁移
fn legacy_config_paths() -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        paths.push(format!("{home}/.config/netdugger/config.json").into());
    }
    paths.push("netdugger_config.json".into());
    paths
}
