pub fn parse_hex(input: &str) -> Result<Vec<u8>, String> {
    let cleaned: String = input
        .replace("0x", "")
        .replace("0X", "")
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ',')
        .collect();
    if cleaned.is_empty() {
        return Err("十六进制内容为空".into());
    }
    if cleaned.len() % 2 != 0 {
        return Err(format!("十六进制长度为 {}，必须是偶数", cleaned.len()));
    }
    let mut out = Vec::with_capacity(cleaned.len() / 2);
    let bytes = cleaned.as_bytes();
    for pair in bytes.chunks(2) {
        let s = std::str::from_utf8(pair).unwrap();
        let b = u8::from_str_radix(s, 16)
            .map_err(|_| format!("无效十六进制字节 '{s}'"))?;
        out.push(b);
    }
    Ok(out)
}

pub fn format_hex(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 3);
    for (i, b) in data.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// 十六进制编辑辅助：去掉空白、丢弃非十六进制字符，然后每两个字符插一个空格
/// 输入 01A2 / 01 a2 / 01a2 都会变成 01 A2，方便边输入边看字节
pub fn group_hex(input: &str) -> String {
    let hex: String = input
        .chars()
        .filter(|c| !c.is_whitespace() && c.is_ascii_hexdigit())
        .collect();
    let mut out = String::with_capacity(hex.len() * 3 / 2);
    for (i, c) in hex.chars().enumerate() {
        if i > 0 && i % 2 == 0 {
            out.push(' ');
        }
        out.push(c);
    }
    out
}
