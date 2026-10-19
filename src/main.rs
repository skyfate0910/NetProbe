mod codec;
mod config;
mod session;

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use codec::{format_hex, group_hex, parse_hex};
use egui::epaint::text::cursor::CCursor;
use egui::text_selection::CCursorRange;
use config::{
    AppConfig, SessionConfig, SessionKind, SessionParams, TcpClientConfig, TcpServerConfig,
    UdpMulticastConfig, UiPrefs, UdpUnicastConfig,
};
use crossbeam::channel::{Receiver, Sender};
use eframe::egui::{self, Align2, Color32, RichText, ScrollArea, TextEdit};
use session::{
    start as start_session, NetEvent, SendTarget, SessionRuntime,
};

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1400.0, 900.0])
            .with_min_inner_size([900.0, 600.0])
            .with_title("NetProbe — TCP/UDP 调试助手")
            .with_app_id("netprobe")
            .with_icon(egui::IconData {
                rgba: include_bytes!("../assets/icon_256.rgba").to_vec(),
                width: 256,
                height: 256,
            }),
        // 不记住上次窗口大小，每次都按 1400×900 打开
        persist_window: false,
        ..Default::default()
    };
    eframe::run_native(
        "NetProbe",
        options,
        Box::new(|cc| Ok(Box::new(NetProbeApp::new(cc)))),
    )
}

// ---------- 会话运行时的界面状态 ----------

struct SessionUi {
    send_text: String,
    recv_text: String,
    hex_send: bool,
    hex_recv: bool,
    auto_send: bool,
    interval_ms: u32,
    echo: bool,
    paused: bool,
    save_enabled: bool,
    save_path: String,
    writer: Option<std::io::BufWriter<std::fs::File>>,
    tx_bytes: u64,
    rx_bytes: u64,
    tx_rate: u64,
    rx_rate: u64,
    rate_start: Instant,
    tx_recent: u64,
    rx_recent: u64,
    last_auto: Instant,
    /// TCP 服务器：在线客户端地址列表
    clients: Vec<String>,
    /// 发送目标：0 = 全部，其余为 clients 下标 + 1
    client_sel: usize,
    /// 接收区显示时间戳
    show_timestamp: bool,
    /// 发送后追加回车 0D
    append_cr: bool,
    /// 发送后追加换行 0A
    append_lf: bool,
    /// 发送文件弹框状态
    file_open: bool,
    file_path: String,
    /// 发送区文本框高度，拖动分隔条调整
    send_area_h: f32,
}

impl Default for SessionUi {
    fn default() -> Self {
        Self {
            send_text: String::new(),
            recv_text: String::new(),
            hex_send: false,
            hex_recv: false,
            auto_send: false,
            interval_ms: 100,
            echo: false,
            paused: false,
            save_enabled: false,
            save_path: String::new(),
            writer: None,
            tx_bytes: 0,
            rx_bytes: 0,
            tx_rate: 0,
            rx_rate: 0,
            rate_start: Instant::now(),
            tx_recent: 0,
            rx_recent: 0,
            last_auto: Instant::now(),
            clients: Vec::new(),
            client_sel: 0,
            show_timestamp: false,
            append_cr: false,
            append_lf: false,
            file_open: false,
            file_path: String::new(),
            send_area_h: 150.0,
        }
    }
}

// ---------- 创建对话框 ----------

#[derive(Clone, Copy, PartialEq)]
enum CreateMode {
    Client,
    Server,
}

#[derive(Clone)]
struct CreateDialog {
    mode: Option<CreateMode>,
    kind: usize, // 客户端: 0=TCP 1=UDP单播 2=UDP组播；服务器: 0=TCP 1=UDP
    remote_host: String,
    remote_port: u16,
    /// 本地IP（源地址），0.0.0.0 表示任意网卡
    local_host: String,
    /// 本地端口；0 表示不指定，使用随机空闲端口
    local_port: u16,
    listen_host: String,
    listen_port: u16,
    group: String,
    group_port: u16,
    iface: String,
    ttl: u32,
    mcast_loop: bool,
}

impl Default for CreateDialog {
    fn default() -> Self {
        Self {
            mode: None,
            kind: 0,
            remote_host: "127.0.0.1".into(),
            remote_port: 7,
            local_host: "0.0.0.0".into(),
            local_port: 0,
            listen_host: "0.0.0.0".into(),
            listen_port: 9000,
            group: "239.255.0.1".into(),
            group_port: 5000,
            iface: "0.0.0.0".into(),
            ttl: 1,
            mcast_loop: true,
        }
    }
}

// ---------- UI 触发的动作 ----------

#[derive(Clone)]
enum Action {
    Connect,
    Disconnect,
    SendNow,
}

fn group_of(params: &SessionParams) -> &'static str {
    match params {
        SessionParams::TcpServer(_) => "服务器模式",
        SessionParams::UdpUnicast(c) if c.created_as_server => "服务器模式",
        _ => "客户端模式",
    }
}

// ---------- 应用主体 ----------

struct NetProbeApp {
    config: AppConfig,
    tokio_rt: tokio::runtime::Runtime,
    event_tx: Sender<(String, NetEvent)>,
    event_rx: Receiver<(String, NetEvent)>,
    runtimes: HashMap<String, SessionRuntime>,
    ui: HashMap<String, SessionUi>,
    status: String,
    dialog: CreateDialog,
    started_at: Instant,
    /// 本机 IPv4 地址缓存（本地IP/网卡下拉用），定时刷新
    ip_addrs: Vec<(String, String)>,
    ip_addrs_at: Instant,
    /// 左侧分组展开状态：0=客户端模式，1=服务器模式（false=展开）
    groups_collapsed: [bool; 2],
}

impl NetProbeApp {
    fn install_cjk_fonts(ctx: &egui::Context) {
        const CANDIDATES: &[&str] = &[
            "/usr/share/fonts/opentype/source-han-cjk/SourceHanSansSC-Medium.otf",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        ];
        for path in CANDIDATES {
            let Ok(data) = std::fs::read(path) else {
                continue;
            };
            let mut fonts = egui::FontDefinitions::default();
            fonts
                .font_data
                .insert("cjk".into(), egui::FontData::from_owned(data));
            for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
                fonts.families.entry(family).or_default().push("cjk".into());
            }
            ctx.set_fonts(fonts);
            return;
        }
        eprintln!("NetProbe: 未找到 CJK 字体，中文将显示为方框");
    }

    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        Self::install_cjk_fonts(&cc.egui_ctx);
        let mut style = (*cc.egui_ctx.style()).clone();
        style
            .text_styles
            .insert(egui::TextStyle::Heading, egui::FontId::proportional(17.0));
        style
            .text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(14.0));
        style
            .text_styles
            .insert(egui::TextStyle::Button, egui::FontId::proportional(14.0));
        style
            .text_styles
            .insert(egui::TextStyle::Small, egui::FontId::proportional(12.5));
        style.text_styles.insert(
            egui::TextStyle::Monospace,
            egui::FontId::monospace(13.0),
        );
        // 行间距与控件内边距：比默认稍宽松，观感更舒适
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        style.spacing.button_padding = egui::vec2(6.0, 4.0);
        // 控件最小高度：下拉框/数字框/按钮整体加高
        style.spacing.interact_size = egui::vec2(40.0, 28.0);
        cc.egui_ctx.set_style(style);
        // 白色背景（浅色主题）。set_visuals 会整体替换 visuals，边框和输入框底色必须在这之后设
        cc.egui_ctx.set_visuals(egui::Visuals::light());
        cc.egui_ctx.style_mut(|style| {
            style.visuals.extreme_bg_color = Color32::WHITE;
            style.visuals.widgets.inactive.weak_bg_fill = Color32::WHITE;
            style.visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(245, 248, 252);
            style.visuals.widgets.inactive.bg_stroke =
                egui::Stroke::new(1.0_f32, Color32::from_rgb(185, 195, 210));
            style.visuals.widgets.hovered.bg_stroke =
                egui::Stroke::new(1.0_f32, Color32::from_rgb(120, 150, 210));
        });

        let (event_tx, event_rx) = crossbeam::channel::unbounded();
        let config = AppConfig::load();
        // 从持久化选项恢复每个会话的收发区状态
        let mut ui = HashMap::new();
        for session in &config.sessions {
            let mut st = SessionUi::default();
            let p = &session.prefs;
            st.hex_send = p.hex_send;
            st.hex_recv = p.hex_recv;
            st.auto_send = p.auto_send;
            st.interval_ms = if p.interval_ms == 0 { 100 } else { p.interval_ms };
            st.echo = p.echo;
            st.show_timestamp = p.show_timestamp;
            // 旧的「发送新行」等价于同时追加 0D 和 0A
            if p.append_newline && !p.append_cr && !p.append_lf {
                st.append_cr = true;
                st.append_lf = true;
            } else {
                st.append_cr = p.append_cr;
                st.append_lf = p.append_lf;
            }
            st.save_enabled = p.save_enabled;
            st.save_path = p.save_path.clone();
            st.send_text = p.send_text.clone();
            ui.insert(session.id.clone(), st);
        }
        Self {
            config,
            tokio_rt: tokio::runtime::Runtime::new().expect("无法启动 tokio 运行时"),
            event_tx,
            event_rx,
            runtimes: HashMap::new(),
            ui,
            status: "就绪。用顶栏按钮创建会话。".into(),
            dialog: CreateDialog::default(),
            started_at: Instant::now(),
            ip_addrs: session::local_ipv4_addrs(),
            ip_addrs_at: Instant::now(),
            groups_collapsed: [false, false],
        }
    }

    fn add_session(&mut self, params: SessionParams) {
        let kind = params.kind();
        let n = self
            .config
            .sessions
            .iter()
            .filter(|s| s.params.kind() == kind)
            .count()
            + 1;
        let id = format!("{kind:?}-{}", self.started_at.elapsed().as_nanos());
        self.config.sessions.push(SessionConfig {
            id: id.clone(),
            name: format!("{} {n}", kind.label()),
            params,
            prefs: UiPrefs::default(),
        });
        self.config.selected = Some(id.clone());
        self.ui.insert(id, SessionUi::default());
        self.config.save();
    }

    fn handle_event(&mut self, id: String, event: NetEvent) {
        let st = self.ui.entry(id.clone()).or_default();
        match event {
            NetEvent::Connected(desc) => {
                self.status = format!("[{id}] 已连接 {desc}");
                st.recv_text.push_str(&format!("// 已连接 {desc}\n"));
            }
            NetEvent::Clients(list) => {
                st.clients = list;
                if st.client_sel > st.clients.len() {
                    st.client_sel = 0;
                }
            }
            NetEvent::Closed(msg) => {
                self.status = format!("[{id}] 已断开 {msg}");
                st.recv_text.push_str(&format!("// 已断开 {msg}\n"));
                if let Some(rt) = self.runtimes.remove(&id) {
                    rt.stop();
                }
            }
            NetEvent::Error(msg) => {
                self.status = format!("[{id}] {msg}");
                st.recv_text.push_str(&format!("// {msg}\n"));
            }
            NetEvent::Data(data, from) => {
                st.rx_bytes += data.len() as u64;
                st.rx_recent += data.len() as u64;
                if st.save_enabled && !st.save_path.is_empty() {
                    if st.writer.is_none() {
                        match std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&st.save_path)
                        {
                            Ok(f) => st.writer = Some(std::io::BufWriter::new(f)),
                            Err(e) => {
                                self.status = format!("[{id}] 打开保存文件失败: {e}");
                                st.save_enabled = false;
                            }
                        }
                    }
                    if let Some(w) = &mut st.writer {
                        let _ = w.write_all(&data);
                        let _ = w.flush();
                    }
                }
                if !st.paused {
                    let prefix = if st.show_timestamp {
                        format!("[{}] ", chrono::Local::now().format("%H:%M:%S"))
                    } else {
                        String::new()
                    };
                    if st.hex_recv {
                        st.recv_text
                            .push_str(&format!("{prefix}{}\n", format_hex(&data)));
                    } else {
                        st.recv_text
                            .push_str(&format!("{prefix}{}", String::from_utf8_lossy(&data)));
                    }
                    if st.recv_text.len() > 200_000 {
                        if let Some(pos) = st.recv_text.find('\n') {
                            let _ = st.recv_text.drain(..pos + 1);
                        }
                    }
                }
                if st.echo {
                    if let Some(rt) = self.runtimes.get(&id) {
                        let n = data.len() as u64;
                        // 回发给来源地址；无来源（TCP 客户端）则按全部/当前目标
                        let target = match from {
                            Some(a) => SendTarget::One(a.to_string()),
                            None => SendTarget::All,
                        };
                        if rt.send(data, target) {
                            st.tx_bytes += n;
                            st.tx_recent += n;
                        }
                    }
                }
            }
        }
    }

    fn send_payload(&mut self, id: &str) {
        let Some(st) = self.ui.get_mut(id) else {
            return;
        };
        if st.send_text.is_empty() {
            return;
        }
        let mut data = if st.hex_send {
            match parse_hex(&st.send_text) {
                Ok(d) => d,
                Err(e) => {
                    self.status = format!("[{id}] 十六进制错误: {e}");
                    return;
                }
            }
        } else {
            st.send_text.as_bytes().to_vec()
        };
        if st.append_cr {
            data.push(0x0D);
        }
        if st.append_lf {
            data.push(0x0A);
        }
        self.send_bytes(id, data);
    }

    /// 发送原始字节：TCP 整包发；UDP 按便于分片的块发
    fn send_bytes(&mut self, id: &str, data: Vec<u8>) {
        let Some(st) = self.ui.get_mut(id) else {
            return;
        };
        let target = if st.client_sel == 0 || st.clients.is_empty() {
            SendTarget::All
        } else {
            SendTarget::One(st.clients[st.client_sel - 1].clone())
        };
        let Some(rt) = self.runtimes.get(id) else {
            self.status = format!("[{id}] 未连接，无法发送");
            return;
        };
        let kind = self
            .config
            .sessions
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.params.kind());
        let chunks: Vec<&[u8]> = match kind {
            Some(SessionKind::UdpUnicast) | Some(SessionKind::UdpMulticast) => {
                // UDP 载荷超 MTU 会分片，按 1400 切块
                data.chunks(1400).collect()
            }
            _ => vec![data.as_slice()],
        };
        let total: u64 = data.len() as u64;
        let mut ok = true;
        for chunk in chunks {
            if !rt.send(chunk.to_vec(), target.clone()) {
                ok = false;
                break;
            }
        }
        if ok {
            st.tx_bytes += total;
            st.tx_recent += total;
            self.status = format!("[{id}] 已发送 {total} 字节");
        } else {
            self.status = format!("[{id}] 发送失败，连接可能已关闭");
        }
    }

    /// 重新读取本机网卡地址（启动时、定时、点「刷新」时调用）
    fn refresh_ip_addrs(&mut self) {
        self.ip_addrs = session::local_ipv4_addrs();
        self.ip_addrs_at = Instant::now();
    }

    fn connect_one(&mut self, id: &str) -> bool {
        if self.runtimes.contains_key(id) {
            return false;
        }
        let Some(session) = self.config.sessions.iter().find(|s| s.id == id) else {
            return false;
        };
        let params = session.params.clone();
        let kind = session.params.kind();
        match start_session(&self.tokio_rt, id.to_string(), params, self.event_tx.clone()) {
            Ok(rt) => {
                self.status = match kind {
                    SessionKind::TcpServer => format!("[{id}] 正在监听 {}…", rt.peer_desc),
                    SessionKind::UdpMulticast => format!("[{id}] 正在加入组播 {}…", rt.peer_desc),
                    _ => format!("[{id}] 正在连接 {}…", rt.peer_desc),
                };
                self.runtimes.insert(id.to_string(), rt);
                true
            }
            Err(e) => {
                self.status = format!("[{id}] {e}");
                false
            }
        }
    }

    fn connect_selected(&mut self) {
        if let Some(id) = self.config.selected.clone() {
            self.connect_one(&id);
        }
    }

    fn disconnect(&mut self, id: &str) {
        if let Some(rt) = self.runtimes.remove(id) {
            rt.stop();
            self.status = format!("[{id}] 已手动断开");
        }
    }

    fn save_recv_snapshot(&mut self, id: &str) {
        let Some(st) = self.ui.get_mut(id) else {
            return;
        };
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let path = format!("netprobe_recv_{secs}.txt");
        match std::fs::write(&path, &st.recv_text) {
            Ok(()) => self.status = format!("[{id}] 已保存到 {path}"),
            Err(e) => self.status = format!("[{id}] 保存失败: {e}"),
        }
    }
}

impl eframe::App for NetProbeApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // 网卡地址列表：仅在有选择器可见时按 3 秒节奏刷新（空闲时零开销）；
        // 每次读取用 getifaddrs 系统调用（实测 ~100µs），不启子进程
        let pickers_visible = self.config.selected.is_some() || self.dialog.mode.is_some();
        if pickers_visible && self.ip_addrs_at.elapsed() > Duration::from_secs(3) {
            self.refresh_ip_addrs();
        }
        let ip_addrs = self.ip_addrs.clone();

        // 事件 + 自动发送 + 速率
        while let Ok((id, ev)) = self.event_rx.try_recv() {
            self.handle_event(id, ev);
        }
        let due: Vec<String> = self
            .ui
            .iter()
            .filter(|(id, st)| {
                st.auto_send
                    && self.runtimes.contains_key(*id)
                    && st.last_auto.elapsed() >= Duration::from_millis(st.interval_ms as u64)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            if let Some(st) = self.ui.get_mut(&id) {
                st.last_auto = Instant::now();
            }
            self.send_payload(&id);
        }
        for st in self.ui.values_mut() {
            if st.rate_start.elapsed() >= Duration::from_secs(1) {
                st.tx_rate = st.tx_recent;
                st.rx_rate = st.rx_recent;
                st.tx_recent = 0;
                st.rx_recent = 0;
                st.rate_start = Instant::now();
            }
        }
        ctx.request_repaint_after(Duration::from_millis(100));

        let mut actions: Vec<Action> = Vec::new();
        let mut delete_selected = false;
        let mut disconnect_all = false;
        let mut connect_all = false;
        let mut save_snapshot = false;
        let mut refresh_ips = false;

        // 顶栏
        let mut top_frame = egui::Frame::side_top_panel(&ctx.style());
        top_frame.inner_margin.right = 16.0;
        egui::TopBottomPanel::top("top").frame(top_frame).show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("NetProbe").strong().size(16.0));
                ui.separator();
                if blue_button(ui, "创建客户端") {
                    self.dialog = CreateDialog {
                        mode: Some(CreateMode::Client),
                        kind: 0,
                        ..CreateDialog::default()
                    };
                }
                if blue_button(ui, "创建服务器") {
                    self.dialog = CreateDialog {
                        mode: Some(CreateMode::Server),
                        kind: 0,
                        ..CreateDialog::default()
                    };
                }
                ui.separator();
                let any_pending = self
                    .config
                    .sessions
                    .iter()
                    .any(|s| !self.runtimes.contains_key(&s.id));
                ui.add_enabled_ui(any_pending, |ui| {
                    if blue_button(ui, "全部连接") {
                        connect_all = true;
                    }
                });
                ui.add_enabled_ui(!self.runtimes.is_empty(), |ui| {
                    if blue_button(ui, "全部断开") {
                        disconnect_all = true;
                    }
                });
                let has_sel = self.config.selected.is_some();
                ui.add_enabled_ui(has_sel, |ui| {
                    if delete_button(ui, "删除") {
                        delete_selected = true;
                    }
                });
            });
            ui.add_space(4.0);
        });

        // 状态栏
        let selected_id = self.config.selected.clone();
        let (tx_rate, rx_rate) = selected_id
            .as_ref()
            .and_then(|id| self.ui.get(id))
            .map(|st| (st.tx_rate, st.rx_rate))
            .unwrap_or((0, 0));
        let status_line = self.status.clone();
        let mut status_frame = egui::Frame::side_top_panel(&ctx.style());
        status_frame.inner_margin.right = 16.0;
        egui::TopBottomPanel::bottom("status")
            .frame(status_frame)
            .show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(&status_line);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!("接收速度 (B/s): {rx_rate}"));
                    ui.separator();
                    ui.label(format!("发送速度 (B/s): {tx_rate}"));
                });
            });
        });

        // 左侧属性栏
        let running: Vec<String> = self.runtimes.keys().cloned().collect();
        let snapshot: Vec<(String, String, String, bool)> = self
            .config
            .sessions
            .iter()
            .map(|s| {
                (
                    s.id.clone(),
                    s.name.clone(),
                    s.params.summary(),
                    running.contains(&s.id),
                )
            })
            .collect();
        egui::SidePanel::left("sessions")
            .resizable(false)
            .exact_width(335.0)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                for (gi, group) in ["客户端模式", "服务器模式"].into_iter().enumerate() {
                    ui.add_space(2.0);
                    // 分组标题：整行可点，点任意处收起/展开
                    if group_header(ui, group, self.groups_collapsed[gi]) {
                        self.groups_collapsed[gi] = !self.groups_collapsed[gi];
                    }
                    if self.groups_collapsed[gi] {
                        continue;
                    }
                    for (id, name, summary, is_running) in &snapshot {
                        if group_of(&self.config.sessions.iter().find(|s| &s.id == id).unwrap().params) != group {
                            continue;
                        }
                        let selected = self.config.selected.as_deref() == Some(id.as_str());
                        // 整行自定义绘制：底色左右充满整个属性栏
                        let width = ui.available_width();
                        let (rect, response) =
                            ui.allocate_exact_size(egui::vec2(width, 42.0), egui::Sense::click());
                        let hovered = response.hovered();
                        let bg = if selected {
                            Color32::from_rgb(219, 233, 251)
                        } else if hovered {
                            Color32::from_rgb(239, 244, 250)
                        } else {
                            Color32::TRANSPARENT
                        };
                        if bg != Color32::TRANSPARENT {
                            ui.painter().rect_filled(rect, 4.0, bg);
                        }
                        // 状态点：已连接实心绿，未连接实心灰
                        let dot_color = if *is_running {
                            Color32::from_rgb(30, 155, 60)
                        } else {
                            Color32::from_rgb(160, 160, 160)
                        };
                        let dot_center = egui::pos2(rect.left() + 14.0, rect.top() + 14.0);
                        ui.painter()
                            .circle(dot_center, 4.5, dot_color, egui::Stroke::NONE);
                        // 名称（深色）+ 摘要（灰色）
                        let name_color = Color32::from_rgb(40, 40, 40);
                        let name_galley = ui.painter().layout_no_wrap(
                            name.clone(),
                            egui::FontId::proportional(13.5),
                            name_color,
                        );
                        ui.painter()
                            .galley(egui::pos2(rect.left() + 26.0, rect.top() + 4.0), name_galley, name_color);
                        let sum_color = Color32::from_rgb(125, 125, 125);
                        let sum_galley = ui.painter().layout_no_wrap(
                            summary.clone(),
                            egui::FontId::proportional(12.0),
                            sum_color,
                        );
                        ui.painter().galley(
                            egui::pos2(rect.left() + 26.0, rect.top() + 22.0),
                            sum_galley,
                            sum_color,
                        );
                        if response.clicked() {
                            self.config.selected = Some(id.clone());
                            self.config.save();
                        }
                    }
                }
            });

        // 中央会话页
        let mut center_frame = egui::Frame::central_panel(&ctx.style());
        center_frame.inner_margin.right = 16.0;
        center_frame.inner_margin.bottom = 16.0;
        egui::CentralPanel::default()
            .frame(center_frame)
            .show(ctx, |ui| {
            let Some(id) = selected_id.clone() else {
                ui.centered_and_justified(|ui| {
                    ui.label(RichText::new("选择或新建一个会话").size(16.0).weak());
                });
                return;
            };
            let Some(pos) = self.config.sessions.iter().position(|s| s.id == id) else {
                return;
            };
            let is_running = self.runtimes.contains_key(&id);
            let kind = self.config.sessions[pos].params.kind();

            let session = &mut self.config.sessions[pos];
            let st = self.ui.entry(id.clone()).or_default();

            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new(&session.name).strong().size(15.0));
                ui.label(
                    RichText::new(format!("【{}】", kind.label()))
                        .color(Color32::from_rgb(40, 110, 220))
                        .small(),
                );
                if is_running {
                    ui.label(RichText::new("● 运行中").color(Color32::from_rgb(20, 140, 60)).small());
                }
            });
            ui.separator();

            let mut dirty = false;
            ui.horizontal_top(|ui| {
                // 左列：参数。标题通栏，内容左右各 12px
                ui.allocate_ui_with_layout(
                    egui::vec2(300.0, ui.available_height()),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                    section_title(ui, "基本信息");
                    egui::Frame::default()
                        .inner_margin(egui::Margin {
                            left: 12.0,
                            right: 12.0,
                            top: 8.0,
                            bottom: 4.0,
                        })
                        .show(ui, |ui| {
                    ui.set_min_width(276.0);
                    match &mut session.params {
                        SessionParams::TcpClient(c) => {
                            ui.label("目标IP");
                            dirty |= ip_text_edit(ui, &mut c.remote_host).changed();
                            ui.label("目标端口");
                            dirty |= edit_port(ui, &mut c.remote_port);
                            refresh_ips |= ip_field(
                            ui,
                            "tc-localip",
                            "本地IP",
                            &mut c.local_host,
                            &ip_addrs,
                            &mut dirty,
                        );
                            ui.label("本地端口");
                            dirty |= edit_port_hint(
                                ui,
                                &mut c.local_port,
                                ui.available_width(),
                                "使用随机端口",
                            );
                            ui.label("超时时间(秒)");
                            dirty |= edit_secs(ui, &mut c.connect_timeout_secs);
                            ui.horizontal(|ui| {
                                dirty |= ui.checkbox(&mut c.auto_reconnect, "自动连接").changed();
                                ui.label("每隔");
                                dirty |= ui
                                    .add(
                                        egui::DragValue::new(&mut c.reconnect_secs)
                                            .range(1..=3600)
                                            .suffix(" s"),
                                    )
                                    .changed();
                            });
                            ui.label(
                                RichText::new("断开后自动重连，间隔可调")
                                    .weak()
                                    .small(),
                            );
                        }
                        SessionParams::TcpServer(c) => {
                            refresh_ips |= ip_field(
                            ui,
                            "ts-listen",
                            "监听地址",
                            &mut c.listen_host,
                            &ip_addrs,
                            &mut dirty,
                        );
                            ui.label("监听端口");
                            dirty |= edit_port(ui, &mut c.listen_port);
                            ui.add_space(4.0);
                            ui.label(RichText::new("发送目标").strong());
                            let mut items: Vec<String> = vec!["全部客户端".into()];
                            items.extend(st.clients.iter().cloned());
                            let sel_text = &items[st.client_sel.min(items.len() - 1)];
                            egui::ComboBox::from_id_salt("srv_target")
                                .selected_text(sel_text.clone())
                                .width(ui.available_width())
                                .show_ui(ui, |ui| {
                                    for (i, item) in items.iter().enumerate() {
                                        ui.selectable_value(&mut st.client_sel, i, item.clone());
                                    }
                                });
                            ui.label(
                                RichText::new(format!("在线客户端: {}", st.clients.len())).weak().small(),
                            );
                        }
                        SessionParams::UdpUnicast(c) => {
                            refresh_ips |= ip_field(
                            ui,
                            "uc-localip",
                            "本地IP",
                            &mut c.local_host,
                            &ip_addrs,
                            &mut dirty,
                        );
                            ui.label("本地端口");
                            dirty |= edit_port_hint(
                                ui,
                                &mut c.local_port,
                                ui.available_width(),
                                "使用随机端口",
                            );
                            dirty |= ui.checkbox(&mut c.has_remote, "指定对端").changed();
                            ui.add_enabled_ui(c.has_remote, |ui| {
                                ui.label("目标IP");
                                dirty |= ip_text_edit(ui, &mut c.remote_host).changed();
                                ui.label("目标端口");
                                dirty |= edit_port(ui, &mut c.remote_port);
                            });
                        }
                        SessionParams::UdpMulticast(c) => {
                            ui.label("组地址");
                            dirty |= ip_text_edit(ui, &mut c.group).changed();
                            ui.label("组端口");
                            dirty |= edit_port(ui, &mut c.port);
                            refresh_ips |= ip_field(
                            ui,
                            "mc-iface",
                            "网卡",
                            &mut c.iface,
                            &ip_addrs,
                            &mut dirty,
                        );
                            ui.horizontal(|ui| {
                                ui.label("TTL");
                                dirty |= ui
                                    .add(egui::DragValue::new(&mut c.ttl).range(0..=255))
                                    .changed();
                                dirty |= ui.checkbox(&mut c.loopback, "本机回环").changed();
                            });
                        }
                    }
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        let connect_text = match (kind, is_running) {
                            (SessionKind::TcpServer, false) => "启动",
                            (SessionKind::TcpServer, true) => "停止",
                            (_, true) => "断开",
                            (_, false) => "连接",
                        };
                        if is_running {
                            // 运行中：停止/断开按钮用红色
                            if delete_button(ui, connect_text) {
                                actions.push(Action::Disconnect);
                            }
                        } else if blue_button(ui, connect_text) {
                            actions.push(Action::Connect);
                        }
                    });
                        });
                    ui.add_space(8.0);
                    section_title(ui, "计数信息");
                    egui::Frame::default()
                        .inner_margin(egui::Margin {
                            left: 12.0,
                            right: 12.0,
                            top: 8.0,
                            bottom: 8.0,
                        })
                        .show(ui, |ui| {
                    ui.set_min_width(276.0);
                    ui.horizontal(|ui| {
                        ui.label("发送");
                        readonly_box(ui, &st.tx_bytes.to_string(), 90.0);
                        ui.label("接收");
                        readonly_box(ui, &st.rx_bytes.to_string(), 90.0);
                    });
                    if blue_button(ui, "清空计数") {
                        st.tx_bytes = 0;
                        st.rx_bytes = 0;
                        st.tx_rate = 0;
                        st.rx_rate = 0;
                        st.tx_recent = 0;
                        st.rx_recent = 0;
                    }
                        });
                    },
                );

                ui.separator();

                // 右列：上面接收区，下面发送区，中间可拖动。文本框都不小于 3 行
                ui.vertical(|ui| {
                    let total = ui.available_height();
                    let row_h = ui.fonts(|f| f.row_height(&egui::FontId::monospace(13.0)));
                    let min_text = row_h * 3.0;
                    let recv_chrome = 108.0;
                    let send_chrome = 100.0;
                    let split = 8.0;
                    let budget = (total - split - recv_chrome - send_chrome).max(min_text * 2.0);
                    let min_each = budget.min(min_text);
                    let max_send = (budget - min_each).max(min_each);
                    st.send_area_h = st.send_area_h.clamp(min_each, max_send);
                    let send_text_h = st.send_area_h;
                    let recv_block = total - split - send_chrome - send_text_h;

                    ui.allocate_ui_with_layout(
                        egui::vec2(ui.available_width(), recv_block),
                        egui::Layout::top_down(egui::Align::Min),
                        |ui| {
                            egui::Frame::group(ui.style()).show(ui, |ui| {
                                ui.set_width(ui.available_width());
                                ui.label(RichText::new("接收区").strong());
                                ui.horizontal(|ui| {
                                    let pause_text = if st.paused {
                                        "继续显示"
                                    } else {
                                        "暂停显示"
                                    };
                                    if blue_button(ui, pause_text) {
                                        st.paused = !st.paused;
                                    }
                                    if blue_button(ui, "保存") {
                                        save_snapshot = true;
                                    }
                                    ui.checkbox(&mut st.hex_recv, "十六进制");
                                    ui.checkbox(&mut st.show_timestamp, "时间戳");
                                    ui.with_layout(
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            if blue_button(ui, "清空") {
                                                st.recv_text.clear();
                                            }
                                        },
                                    );
                                });
                                ui.horizontal(|ui| {
                                    ui.checkbox(&mut st.save_enabled, "保存到文件（实时）");
                                    if st.save_enabled {
                                        ui.add(
                                            TextEdit::singleline(&mut st.save_path)
                                                .desired_width(220.0)
                                                .hint_text("文件路径"),
                                        );
                                    }
                                });
                                let text_h = ui.available_height().max(min_text);
                                let mut shown = st.recv_text.clone();
                                ScrollArea::vertical().id_salt("recv").show(ui, |ui| {
                                    ui.add(
                                        TextEdit::multiline(&mut shown)
                                            .desired_width(f32::INFINITY)
                                            .min_size(egui::vec2(0.0, text_h))
                                            .font(egui::TextStyle::Monospace),
                                    );
                                });
                            });
                        },
                    );

                    let (bar, bar_resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), split),
                        egui::Sense::click_and_drag(),
                    );
                    if bar_resp.hovered() || bar_resp.dragged() {
                        ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeVertical);
                    }
                    if bar_resp.dragged() {
                        st.send_area_h = (st.send_area_h - bar_resp.drag_delta().y)
                            .clamp(min_each, max_send);
                    }
                    let bar_color = if bar_resp.hovered() || bar_resp.dragged() {
                        Color32::from_rgb(120, 150, 210)
                    } else {
                        Color32::from_rgb(190, 198, 210)
                    };
                    ui.painter().hline(
                        bar.x_range(),
                        bar.center().y,
                        egui::Stroke::new(2.0_f32, bar_color),
                    );

                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.label(RichText::new("发送区").strong());
                        ui.horizontal(|ui| {
                            if ui.checkbox(&mut st.auto_send, "自动发送").changed() {
                                st.last_auto = Instant::now();
                            }
                            ui.label("每隔");
                            ui.add(
                                egui::DragValue::new(&mut st.interval_ms)
                                    .range(10..=60_000)
                                    .suffix(" ms"),
                            );
                            if blue_button(ui, "发送") {
                                actions.push(Action::SendNow);
                            }
                            if blue_button(ui, "停止") {
                                st.auto_send = false;
                            }
                        });
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut st.hex_send, "十六进制");
                            ui.checkbox(&mut st.append_cr, "追加回车符");
                            ui.checkbox(&mut st.append_lf, "追加换行符");
                            ui.checkbox(&mut st.echo, "发送接收到的数据");
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if blue_button(ui, "清空") {
                                        st.send_text.clear();
                                    }
                                    if blue_button(ui, "发送文件") {
                                        st.file_open = true;
                                    }
                                },
                            );
                        });
                        let width = ui.available_width();
                        let row_h = ui.fonts(|f| f.row_height(&egui::FontId::monospace(13.0)));
                        let line_count = st.send_text.split('\n').count().max(1);
                        let content_h = line_count as f32 * row_h + 12.0;
                        if content_h <= send_text_h {
                            let resp = ui.add(
                                TextEdit::multiline(&mut st.send_text)
                                    .desired_width(width)
                                    .desired_rows(1)
                                    .min_size(egui::vec2(width, send_text_h))
                                    .font(egui::TextStyle::Monospace)
                                    .hint_text("要发送的文本，或 01 02 FF"),
                            );
                            if resp.changed() && st.hex_send {
                                let spaced = group_hex(&st.send_text);
                                if spaced != st.send_text {
                                    remap_hex_cursor(ui.ctx(), resp.id, &st.send_text);
                                    st.send_text = spaced;
                                }
                            }
                        } else {
                            ScrollArea::vertical()
                                .id_salt("send")
                                .max_height(send_text_h)
                                .auto_shrink([false, false])
                                .show(ui, |ui| {
                                    let resp = ui.add(
                                        TextEdit::multiline(&mut st.send_text)
                                            .desired_width(width)
                                            .desired_rows(line_count)
                                            .font(egui::TextStyle::Monospace)
                                            .hint_text("要发送的文本，或 01 02 FF"),
                                    );
                                    if resp.changed() && st.hex_send {
                                        let spaced = group_hex(&st.send_text);
                                        if spaced != st.send_text {
                                            remap_hex_cursor(ui.ctx(), resp.id, &st.send_text);
                                            st.send_text = spaced;
                                        }
                                    }
                                });
                        }
                    });
                });
            });
            if dirty {
                self.config.save();
            }
        });

        // 创建对话框
        if self.dialog.mode.is_some() {
            self.show_create_dialog(ctx);
        }
        self.show_file_dialogs(ctx);

        // 执行动作
        if let Some(id) = &selected_id {
            for action in actions {
                match action {
                    Action::Connect => self.connect_selected(),
                    Action::Disconnect => self.disconnect(id),
                    Action::SendNow => self.send_payload(id),
                }
            }
            if save_snapshot {
                self.save_recv_snapshot(id);
            }
        }
        if delete_selected {
            if let Some(id) = &selected_id {
                self.disconnect(id);
                self.ui.remove(id);
                self.config.sessions.retain(|s| &s.id != id);
                self.config.selected = self.config.sessions.first().map(|s| s.id.clone());
                self.config.save();
            }
        }
        if disconnect_all {
            let ids: Vec<String> = self.runtimes.keys().cloned().collect();
            for id in ids {
                self.disconnect(&id);
            }
        }
        if refresh_ips {
            self.refresh_ip_addrs();
            self.status = "已刷新本机网卡地址列表".into();
        }
        if connect_all {
            let ids: Vec<String> = self
                .config
                .sessions
                .iter()
                .map(|s| s.id.clone())
                .collect();
            let mut started = 0;
            let mut running = 0;
            for id in ids {
                if self.runtimes.contains_key(&id) {
                    running += 1;
                    continue;
                }
                if self.connect_one(&id) {
                    started += 1;
                }
            }
            self.status = format!("全部连接：新启动 {started} 条，已运行 {running} 条");
        }
        self.sync_ui_prefs();
    }
}

impl NetProbeApp {
    /// 收发区选项同步回持久化配置（有变化才写盘）
    fn sync_ui_prefs(&mut self) {
        let mut dirty = false;
        for session in &mut self.config.sessions {
            let Some(st) = self.ui.get(&session.id) else {
                continue;
            };
            let prefs = UiPrefs {
                hex_send: st.hex_send,
                hex_recv: st.hex_recv,
                auto_send: st.auto_send,
                interval_ms: st.interval_ms,
                echo: st.echo,
                show_timestamp: st.show_timestamp,
                append_newline: false,
                append_cr: st.append_cr,
                append_lf: st.append_lf,
                save_enabled: st.save_enabled,
                save_path: st.save_path.clone(),
                send_text: st.send_text.clone(),
            };
            if session.prefs != prefs {
                session.prefs = prefs;
                dirty = true;
            }
        }
        if dirty {
            self.config.save();
        }
    }
}

impl Drop for NetProbeApp {
    fn drop(&mut self) {
        for (_, rt) in self.runtimes.drain() {
            rt.stop();
        }
    }
}

impl NetProbeApp {
    fn show_create_dialog(&mut self, ctx: &egui::Context) {
        let ip_addrs = self.ip_addrs.clone();
        let mut refresh_ips = false;
        // 下拉改值的动作在「确定」时统一提交，这里只用来汇总
        let mut dlg_dirty = false;
        let mode = self.dialog.mode.unwrap();
        let title = match mode {
            CreateMode::Client => "创建客户端",
            CreateMode::Server => "创建服务器",
        };
        let mut confirmed = false;
        let mut cancelled = false;
        let frame = egui::Frame::window(&ctx.style()).inner_margin(egui::Margin::same(0.0));
        egui::Window::new(title)
            .title_bar(false)
            .frame(frame)
            .collapsible(false)
            .resizable(false)
            .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                dialog_title(ui, title);
                egui::Frame::default()
                    .inner_margin(egui::Margin {
                        left: 20.0,
                        right: 20.0,
                        top: 16.0,
                        bottom: 16.0,
                    })
                    .show(ui, |ui| {
                let d = &mut self.dialog;
                egui::Grid::new("create_fields")
                    .num_columns(3)
                    .min_col_width(96.0)
                    .spacing(egui::vec2(20.0, 10.0))
                    .show(ui, |ui| {
                        // 类型
                        form_label(ui, "类型");
                        let names: &[&str] = match mode {
                            CreateMode::Client => &["TCP", "UDP 单播", "UDP 组播"],
                            CreateMode::Server => &["TCP", "UDP"],
                        };
                        if d.kind >= names.len() {
                            d.kind = 0;
                        }
                        egui::ComboBox::from_id_salt("create_kind")
                            .selected_text(names[d.kind])
                            .width(DLG_FIELD_W)
                            .show_ui(ui, |ui| {
                                for (i, name) in names.iter().enumerate() {
                                    ui.selectable_value(&mut d.kind, i, *name);
                                }
                            });
                        ui.end_row();

                        match mode {
                            CreateMode::Client => match d.kind {
                                0 => {
                                    // TCP 客户端
                                    form_label(ui, "目标IP");
                                    ui.add(field_edit(&mut d.remote_host, DLG_FIELD_W, ""));
                                    ui.end_row();
                                    form_label(ui, "目标端口");
                                    edit_port_width(ui, &mut d.remote_port, DLG_FIELD_W);
                                    ui.end_row();
                                    form_label(ui, "本地IP");
                                    refresh_ips |= dialog_ip_selector(
                                        ui,
                                        "dlg-tc-localip",
                                        &mut d.local_host,
                                        &ip_addrs,
                                        &mut dlg_dirty,
                                    );
                                    ui.end_row();
                                    form_label(ui, "本地端口");
                                    edit_port_hint(ui, &mut d.local_port, DLG_FIELD_W, "使用随机端口");
                                    ui.end_row();
                                }
                                1 => {
                                    // UDP 单播
                                    form_label(ui, "目标IP");
                                    ui.add(field_edit(&mut d.remote_host, DLG_FIELD_W, ""));
                                    ui.end_row();
                                    form_label(ui, "目标端口");
                                    edit_port_width(ui, &mut d.remote_port, DLG_FIELD_W);
                                    ui.end_row();
                                    form_label(ui, "本地IP");
                                    refresh_ips |= dialog_ip_selector(
                                        ui,
                                        "dlg-udp-localip",
                                        &mut d.local_host,
                                        &ip_addrs,
                                        &mut dlg_dirty,
                                    );
                                    ui.end_row();
                                    form_label(ui, "本地端口");
                                    edit_port_hint(ui, &mut d.local_port, DLG_FIELD_W, "使用随机端口");
                                    ui.end_row();
                                }
                                _ => {
                                    // UDP 组播
                                    form_label(ui, "组地址");
                                    ui.add(field_edit(&mut d.group, DLG_FIELD_W, ""));
                                    ui.end_row();
                                    form_label(ui, "端口");
                                    edit_port_width(ui, &mut d.group_port, DLG_FIELD_W);
                                    ui.end_row();
                                    form_label(ui, "网卡");
                                    refresh_ips |= dialog_ip_selector(
                                        ui,
                                        "dlg-mc-iface",
                                        &mut d.iface,
                                        &ip_addrs,
                                        &mut dlg_dirty,
                                    );
                                    ui.end_row();
                                    form_label(ui, "TTL");
                                    edit_uint(ui, &mut d.ttl, DLG_FIELD_W, 255, "");
                                    ui.end_row();
                                    form_label(ui, "");
                                    ui.checkbox(&mut d.mcast_loop, "本机回环");
                                    ui.end_row();
                                }
                            },
                            CreateMode::Server => {
                                form_label(ui, "监听地址");
                                refresh_ips |= dialog_ip_selector(
                                    ui,
                                    "dlg-srv-listen",
                                    &mut d.listen_host,
                                    &ip_addrs,
                                    &mut dlg_dirty,
                                );
                                ui.end_row();
                                form_label(ui, "端口");
                                edit_port_width(ui, &mut d.listen_port, DLG_FIELD_W);
                                ui.end_row();
                            }
                        }
                    });
                ui.add_space(12.0);
                ui.horizontal(|ui| {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if blue_button(ui, "确定") {
                            confirmed = true;
                        }
                        if cancel_button(ui, "取消") {
                            cancelled = true;
                        }
                    });
                });
                    });
            });
        if refresh_ips {
            self.refresh_ip_addrs();
        }
        if confirmed {
            let params = self.build_params_from_dialog();
            self.add_session(params);
            self.dialog.mode = None;
        } else if cancelled {
            self.dialog.mode = None;
        }
    }

    fn show_file_dialogs(&mut self, ctx: &egui::Context) {
        let open_ids: Vec<String> = self
            .ui
            .iter()
            .filter(|(_, st)| st.file_open)
            .map(|(id, _)| id.clone())
            .collect();
        for id in open_ids {
            let mut confirmed = false;
            let mut cancelled = false;
            let frame = egui::Frame::window(&ctx.style()).inner_margin(egui::Margin::same(0.0));
            egui::Window::new("发送文件")
                .title_bar(false)
                .frame(frame)
                .collapsible(false)
                .resizable(false)
                .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    dialog_title(ui, "发送文件");
                    egui::Frame::default()
                        .inner_margin(egui::Margin {
                            left: 20.0,
                            right: 20.0,
                            top: 16.0,
                            bottom: 16.0,
                        })
                        .show(ui, |ui| {
                    let Some(st) = self.ui.get_mut(&id) else {
                        return;
                    };
                    egui::Grid::new("file_fields")
                        .num_columns(2)
                        .spacing([12.0, 8.0])
                        .show(ui, |ui| {
                            form_label(ui, "文件路径");
                            ui.add(
                                field_edit(&mut st.file_path, 360.0, "例如 /home/user/data.bin"),
                            );
                            ui.end_row();
                        });
                    ui.label(
                        RichText::new("整个文件按原始字节发送；UDP 会话自动按 1400 字节分块，避免超过 MTU")
                            .weak()
                            .small(),
                    );
                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if blue_button(ui, "发送") {
                                confirmed = true;
                            }
                            if cancel_button(ui, "取消") {
                                cancelled = true;
                            }
                        });
                    });
                        });
                });
            if confirmed {
                if let Some(st) = self.ui.get_mut(&id) {
                    st.file_open = false;
                    let path = st.file_path.clone();
                    self.send_file(&id, &path);
                }
            } else if cancelled {
                if let Some(st) = self.ui.get_mut(&id) {
                    st.file_open = false;
                }
            }
        }
    }

    fn send_file(&mut self, id: &str, path: &str) {
        match std::fs::read(path) {
            Ok(data) => {
                if data.is_empty() {
                    self.status = format!("[{id}] 文件为空，未发送");
                    return;
                }
                self.status = format!("[{id}] 正在发送文件 {path}…");
                self.send_bytes(id, data);
            }
            Err(e) => self.status = format!("[{id}] 读取文件失败: {e}"),
        }
    }

    fn build_params_from_dialog(&self) -> SessionParams {
        let d = &self.dialog;
        match d.mode.unwrap() {
            CreateMode::Client => match d.kind {
                0 => {
                    SessionParams::TcpClient(TcpClientConfig {
                        remote_host: d.remote_host.clone(),
                        remote_port: d.remote_port,
                        local_host: d.local_host.clone(),
                        local_port: d.local_port,
                        auto_reconnect: false,
                        reconnect_secs: 3,
                        connect_timeout_secs: 3,
                    })
                }
                1 => SessionParams::UdpUnicast(UdpUnicastConfig {
                    local_host: "0.0.0.0".into(),
                    local_port: d.local_port,
                    remote_host: d.remote_host.clone(),
                    remote_port: d.remote_port,
                    has_remote: true,
                    created_as_server: false,
                }),
                _ => SessionParams::UdpMulticast(UdpMulticastConfig {
                    group: d.group.clone(),
                    port: d.group_port,
                    iface: d.iface.clone(),
                    ttl: d.ttl,
                    loopback: d.mcast_loop,
                }),
            },
            CreateMode::Server => match d.kind {
                0 => SessionParams::TcpServer(TcpServerConfig {
                    listen_host: d.listen_host.clone(),
                    listen_port: d.listen_port,
                }),
                _ => SessionParams::UdpUnicast(UdpUnicastConfig {
                    local_host: d.listen_host.clone(),
                    local_port: d.listen_port,
                    remote_host: String::new(),
                    remote_port: 0,
                    has_remote: false,
                    created_as_server: true,
                }),
            },
        }
    }
}

fn edit_port(ui: &mut egui::Ui, port: &mut u16) -> bool {
    edit_port_width(ui, port, ui.available_width())
}

/// 端口输入框：与 IP 输入框同宽同高，文字垂直居中
fn edit_port_width(ui: &mut egui::Ui, port: &mut u16, width: f32) -> bool {
    edit_port_hint(ui, port, width, "")
}

/// 端口输入框（带占位提示）：空值记为 0，删除内容后不会把旧值填回
fn edit_port_hint(ui: &mut egui::Ui, port: &mut u16, width: f32, hint: &str) -> bool {
    let mut text = match *port {
        0 => String::new(),
        v => v.to_string(),
    };
    let resp = ui.add(field_edit(&mut text, width, hint));
    if resp.changed() {
        let digits: String = text.chars().filter(|c| c.is_ascii_digit()).take(5).collect();
        *port = if digits.is_empty() {
            0
        } else {
            digits.parse::<u32>().map(|v| v.min(65535) as u16).unwrap_or(0)
        };
    }
    resp.changed()
}

/// 统一外框宽度的单行输入框。
/// 注意：`TextEdit::desired_width` 指的是**内部文字区**宽度，外框还要加上左右内边距，
/// 而下拉框 `ComboBox::width` 指的是外框宽度 —— 两者语义不同，容易做出不等宽的表单。
/// 这里统一约定：本函数及所有调用方的 `width` 一律是**外框宽度**，内部自动扣掉内边距。
fn field_edit<'a>(text: &'a mut String, width: f32, hint: &str) -> TextEdit<'a> {
    let mut edit = TextEdit::singleline(text)
        .vertical_align(egui::Align::Center)
        .margin(egui::Margin::symmetric(FIELD_PAD_X, 2.0))
        .desired_width((width - FIELD_PAD_X * 2.0).max(16.0))
        .min_size(egui::vec2(width, 28.0));
    if !hint.is_empty() {
        edit = edit.hint_text(hint);
    }
    edit
}

/// 创建对话框内所有输入框/下拉框的统一外框宽度
const DLG_FIELD_W: f32 = 208.0;
/// TextEdit 左右内边距，与 field_edit 中显式设置的一致
const FIELD_PAD_X: f32 = 4.0;
/// 刷新图标按钮的宽度（也用于反推下拉框该缩短多少）
const ICON_BTN_W: f32 = 24.0;

/// 统一宽度的整数输入框（空值记 0，上限 max）
fn edit_uint(ui: &mut egui::Ui, value: &mut u32, width: f32, max: u32, hint: &str) -> bool {
    let mut text = match *value {
        0 => String::new(),
        v => v.to_string(),
    };
    let resp = ui.add(field_edit(&mut text, width, hint));
    if resp.changed() {
        let digits: String = text
            .chars()
            .filter(|c| c.is_ascii_digit())
            .take(6)
            .collect();
        *value = if digits.is_empty() {
            0
        } else {
            digits.parse::<u32>().unwrap_or(0).min(max)
        };
    }
    resp.changed()
}

fn readonly_box(ui: &mut egui::Ui, text: &str, width: f32) {
    let mut value = text.to_owned();
    ui.add_enabled(false, field_edit(&mut value, width, ""));
}

/// 分组标题：整行底色，文字垂直居中
fn section_title(ui: &mut egui::Ui, text: &str) {
    let width = ui.available_width();
    let (rect, _) = ui.allocate_exact_size(egui::vec2(width, 28.0), egui::Sense::hover());
    ui.painter()
        .rect_filled(rect, 4.0, Color32::from_rgb(228, 233, 241));
    let galley = ui.painter().layout_no_wrap(
        text.to_owned(),
        egui::FontId::proportional(14.0),
        Color32::BLACK,
    );
    let pos = egui::Align2::LEFT_CENTER
        .align_size_within_rect(galley.size(), rect.shrink2(egui::vec2(8.0, 0.0)))
        .min;
    ui.painter().galley(pos, galley, Color32::BLACK);
}

/// 弹框标题栏：去掉系统标题后自绘，保证文字垂直居中
fn dialog_title(ui: &mut egui::Ui, title: &str) {
    let width = ui.available_width();
    let (rect, _) = ui.allocate_exact_size(egui::vec2(width, 32.0), egui::Sense::hover());
    ui.painter()
        .rect_filled(rect, 4.0, Color32::from_rgb(228, 233, 241));
    let galley = ui.painter().layout_no_wrap(
        title.to_owned(),
        egui::FontId::proportional(15.0),
        Color32::BLACK,
    );
    let pos = egui::Align2::LEFT_CENTER
        .align_size_within_rect(galley.size(), rect.shrink2(egui::vec2(10.0, 0.0)))
        .min;
    ui.painter().galley(pos, galley, Color32::BLACK);
}

/// 表单标签：与 28px 输入框同高，文字垂直居中、右对齐
fn form_label(ui: &mut egui::Ui, text: &str) {
    ui.allocate_ui_with_layout(
        egui::vec2(96.0, 28.0),
        egui::Layout::right_to_left(egui::Align::Center),
        |ui| {
            ui.set_min_width(96.0);
            ui.label(text);
        },
    );
}

/// 折叠箭头：手绘 V 形线（收缩向右 >，展开向下 v），带悬停高亮，无字体依赖
/// 分组标题行：整行可点击折叠/展开，右侧绘制 V 形箭头，返回本帧是否被点击
fn group_header(ui: &mut egui::Ui, text: &str, collapsed: bool) -> bool {
    let width = ui.available_width();
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(width, 28.0), egui::Sense::click());
    let hovered = response.hovered();
    let fill = if hovered {
        Color32::from_rgb(214, 222, 235)
    } else {
        Color32::from_rgb(228, 233, 241)
    };
    ui.painter().rect_filled(rect, 4.0, fill);

    let galley = ui.painter().layout_no_wrap(
        text.to_owned(),
        egui::FontId::proportional(14.5),
        Color32::BLACK,
    );
    let pos = egui::Align2::LEFT_CENTER
        .align_size_within_rect(galley.size(), rect.shrink2(egui::vec2(10.0, 0.0)))
        .min;
    ui.painter().galley(pos, galley, Color32::BLACK);

    // 右侧箭头：收缩为 > 形，展开为 v 形
    let c = egui::pos2(rect.right() - 15.0, rect.center().y);
    let r = 4.5;
    let stroke = egui::Stroke::new(2.0_f32, Color32::from_rgb(85, 85, 85));
    if collapsed {
        let a = egui::pos2(c.x - r * 0.6, c.y - r);
        let b = egui::pos2(c.x + r * 0.6, c.y);
        let d = egui::pos2(c.x - r * 0.6, c.y + r);
        ui.painter().line_segment([a, b], stroke);
        ui.painter().line_segment([b, d], stroke);
    } else {
        let a = egui::pos2(c.x - r, c.y - r * 0.6);
        let b = egui::pos2(c.x, c.y + r * 0.6);
        let d = egui::pos2(c.x + r, c.y - r * 0.6);
        ui.painter().line_segment([a, b], stroke);
        ui.painter().line_segment([b, d], stroke);
    }

    if hovered {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    response.clicked()
}

/// 带底色的按钮：在局部样式里改 weak_bg_fill/文字色，不影响全局
fn paint_button(ui: &mut egui::Ui, text: &str, bg: Color32, hover: Color32, active: Color32) -> bool {
    ui.scope(|ui| {
        let w = &mut ui.style_mut().visuals.widgets;
        w.inactive.weak_bg_fill = bg;
        w.inactive.fg_stroke = egui::Stroke::new(1.0_f32, Color32::WHITE);
        w.hovered.weak_bg_fill = hover;
        w.hovered.fg_stroke = egui::Stroke::new(1.0_f32, Color32::WHITE);
        w.active.weak_bg_fill = active;
        w.active.fg_stroke = egui::Stroke::new(1.0_f32, Color32::WHITE);
        ui.button(text).clicked()
    })
    .inner
}

/// 常规操作按钮：蓝色底 + 白字
fn blue_button(ui: &mut egui::Ui, text: &str) -> bool {
    paint_button(
        ui,
        text,
        Color32::from_rgb(40, 120, 235),
        Color32::from_rgb(66, 145, 246),
        Color32::from_rgb(26, 95, 205),
    )
}

/// 取消按钮：灰色底 + 白字
fn cancel_button(ui: &mut egui::Ui, text: &str) -> bool {
    paint_button(
        ui,
        text,
        Color32::from_rgb(140, 146, 155),
        Color32::from_rgb(158, 164, 172),
        Color32::from_rgb(120, 126, 134),
    )
}

/// 删除按钮：红色底 + 白字
fn delete_button(ui: &mut egui::Ui, text: &str) -> bool {
    paint_button(
        ui,
        text,
        Color32::from_rgb(216, 62, 62),
        Color32::from_rgb(232, 84, 84),
        Color32::from_rgb(196, 44, 44),
    )
}

/// 单行输入框：最小高度与下拉框/数字框对齐（28px）


/// IP 输入框：把全角字符归一化为半角，修复中文输入法下无法输入小数点的问题
/// （rime 等输入法按"."输出的是全角「．」「。」，直接写入地址无法解析）
fn ip_text_edit<'a>(ui: &mut egui::Ui, text: &'a mut String) -> egui::Response {
    ip_text_edit_hint(ui, text, "")
}

fn ip_text_edit_hint<'a>(ui: &mut egui::Ui, text: &'a mut String, hint: &'a str) -> egui::Response {
    let w = ui.available_width();
    let mut resp = ui.add(field_edit(text, w, hint));
    if resp.changed() && text.chars().any(|ch| {
        matches!(ch, '．' | '。' | '，' | '：') || ('\u{FF10}'..='\u{FF19}').contains(&ch)
    }) {
        let normalized: String = text
            .chars()
            .map(|ch| match ch {
                '．' | '。' => '.',
                '，' => ',',
                '：' => ':',
                c if ('\u{FF10}'..='\u{FF19}').contains(&c) => {
                    char::from_u32(c as u32 - 0xFEE0).unwrap_or(c)
                }
                other => other,
            })
            .collect();
        *text = normalized;
        resp.mark_changed();
    }
    resp
}

/// 十六进制自动分组后，把光标从旧文本位置重映射到新文本位置：
/// 光标前有 d 个十六进制字符时，新位置 = d + (d-1)/2（每两个字符后插了一个空格）
fn remap_hex_cursor(ctx: &egui::Context, id: egui::Id, old: &str) {
    let Some(mut state) = egui::widgets::text_edit::TextEditState::load(ctx, id) else {
        return;
    };
    let Some(range) = state.cursor.char_range() else {
        return;
    };
    let map = |cc: CCursor| {
        let d = old
            .chars()
            .take(cc.index)
            .filter(|c| c.is_ascii_hexdigit())
            .count();
        let idx = if d == 0 { 0 } else { d + (d - 1) / 2 };
        CCursor::new(idx)
    };
    state.cursor.set_char_range(Some(CCursorRange {
        primary: map(range.primary),
        secondary: map(range.secondary),
    }));
    state.store(ctx, id);
}

/// 秒数输入框：与端口/IP 输入框同宽同高（名字在上，框占满列宽），空值记 0（连接时按 1 秒兜底）
fn edit_secs(ui: &mut egui::Ui, secs: &mut u32) -> bool {
    let w = ui.available_width();
    edit_uint(ui, secs, w, 120, "默认 3")
}

/// 会话页的「本地IP / 网卡 / 监听地址」字段。
/// 下拉框 + 右侧刷新图标，两者合起来恰好占满其它输入框的宽度。
/// 返回 true 表示点了刷新。
fn ip_field(
    ui: &mut egui::Ui,
    salt: &str,
    label: &str,
    value: &mut String,
    addrs: &[(String, String)],
    changed: &mut bool,
) -> bool {
    ui.label(label);
    let outer_w = ui.available_width();
    ip_row(ui, salt, value, addrs, outer_w, changed)
}

/// 创建对话框：同上，宽度固定为 DLG_FIELD_W，与其它输入框右边界对齐
fn dialog_ip_selector(
    ui: &mut egui::Ui,
    salt: &str,
    value: &mut String,
    addrs: &[(String, String)],
    changed: &mut bool,
) -> bool {
    ip_row(ui, salt, value, addrs, DLG_FIELD_W, changed)
}

/// 下拉框 + 刷新图标排在同一行：下拉框按 outer_w 缩短，
/// 使「下拉框 + 间距 + 图标」的总宽度与其它输入框完全一致。
fn ip_row(
    ui: &mut egui::Ui,
    salt: &str,
    value: &mut String,
    addrs: &[(String, String)],
    outer_w: f32,
    changed: &mut bool,
) -> bool {
    let mut refresh = false;
    ui.horizontal(|ui| {
        let gap = ui.spacing().item_spacing.x;
        let combo_w = (outer_w - ICON_BTN_W - gap).max(60.0);
        *changed |= ip_combo(ui, salt, value, addrs, combo_w);
        if refresh_icon_button(ui) {
            refresh = true;
        }
    });
    refresh
}

/// 只画下拉本身
fn ip_combo(
    ui: &mut egui::Ui,
    salt: &str,
    value: &mut String,
    addrs: &[(String, String)],
    w: f32,
) -> bool {
    let current = value.trim().to_string();
    let iface_of = |ip: &str| {
        addrs
            .iter()
            .find(|(a, _)| a == ip)
            .map(|(_, n)| n.clone())
    };
    let mut changed = false;

    let selected_text = match current.as_str() {
        "" | "0.0.0.0" => "0.0.0.0（任意网卡）".to_string(),
        ip => match iface_of(ip) {
            Some(name) => format!("{ip}（{name}）"),
            None => format!("{ip}（本机无此地址）"),
        },
    };

    let mut pick: Option<String> = None;
    egui::ComboBox::from_id_salt(salt)
        .selected_text(selected_text)
        .width(w)
        .show_ui(ui, |ui| {
            let any_sel = current.is_empty() || current == "0.0.0.0";
            if ui
                .selectable_label(any_sel, "0.0.0.0（任意网卡）")
                .clicked()
            {
                pick = Some("0.0.0.0".into());
            }
            for (ip, name) in addrs {
                if ui
                    .selectable_label(current == *ip, format!("{ip}（{name}）"))
                    .clicked()
                {
                    pick = Some(ip.clone());
                }
            }
            // 已配置但本机当前没有的地址，仍列出来，避免被悄悄改掉
            if !current.is_empty() && current != "0.0.0.0" && iface_of(&current).is_none() {
                ui.separator();
                if ui
                    .selectable_label(true, format!("{current}（本机无此地址）"))
                    .clicked()
                {
                    pick = Some(current.clone());
                }
            }
        });

    if let Some(v) = pick {
        if *value != v {
            *value = v;
            changed = true;
        }
    }
    changed
}

/// 刷新图标按钮：手绘环形箭头（不依赖字体字形），配色与其它按钮一致
fn refresh_icon_button(ui: &mut egui::Ui) -> bool {
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(ICON_BTN_W, ICON_BTN_W), egui::Sense::click());
    let response = response.on_hover_text("重新读取本机网卡地址");
    let pressed = response.is_pointer_button_down_on();
    let hovered = response.hovered() || pressed;

    // 无底色，只画图标本身；悬停/按下时图标变蓝作为反馈
    let icon_color = if pressed {
        Color32::from_rgb(20, 75, 165)
    } else if hovered {
        Color32::from_rgb(40, 120, 235)
    } else {
        Color32::from_rgb(95, 105, 120)
    };

    // 环形箭头：300° 圆弧（缺口留在右上）+ 弧末三角箭头，箭头指向顺时针方向
    let c = rect.center();
    let r = 5.6;
    let stroke = egui::Stroke::new(1.7_f32, icon_color);
    let start = -30.0_f32.to_radians();
    let sweep = 300.0_f32.to_radians();
    let steps = 40;
    let mut pts: Vec<egui::Pos2> = Vec::with_capacity(steps + 1);
    for i in 0..=steps {
        let a = start + sweep * (i as f32 / steps as f32);
        pts.push(egui::pos2(c.x + r * a.cos(), c.y + r * a.sin()));
    }
    let end = *pts.last().unwrap();
    let a_end = start + sweep;
    let tangent = egui::vec2(-a_end.sin(), a_end.cos());
    let tip = end + tangent * 3.6;
    let normal = egui::vec2(-tangent.y, tangent.x);
    let b1 = end + normal * 2.4;
    let b2 = end - normal * 2.4;
    ui.painter().add(egui::Shape::line(pts, stroke));
    ui.painter().add(egui::Shape::convex_polygon(
        vec![tip, b1, b2],
        icon_color,
        egui::Stroke::NONE,
    ));

    if hovered {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    response.clicked()
}
