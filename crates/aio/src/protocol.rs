use anyhow::{Context, bail, ensure};
use std::net::{IpAddr, SocketAddr};
use tokio::io::{AsyncRead, AsyncReadExt};

fn varint(bytes: &[u8], cursor: &mut usize) -> anyhow::Result<i32> {
    let mut value = 0u32;
    for shift in 0..5 {
        let byte = *bytes.get(*cursor).context("truncated VarInt")?;
        *cursor += 1;
        if shift == 4 {
            ensure!(byte & 0xf0 == 0, "overflowing VarInt");
        }
        value |= u32::from(byte & 0x7f) << (shift * 7);
        if byte & 0x80 == 0 {
            return Ok(value as i32);
        }
    }
    bail!("oversized VarInt")
}

pub async fn handshake<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame: usize,
) -> anyhow::Result<(String, Vec<u8>)> {
    let mut prefix = Vec::with_capacity(5);
    for i in 0..5 {
        let byte = reader.read_u8().await?;
        prefix.push(byte);
        if byte & 0x80 == 0 {
            break;
        }
        ensure!(i != 4, "oversized frame VarInt");
    }
    let size = varint(&prefix, &mut 0)?;
    ensure!(
        size > 0 && size as usize <= max_frame,
        "invalid handshake size"
    );
    let mut frame = vec![0; size as usize];
    reader.read_exact(&mut frame).await?;
    let mut cursor = 0;
    ensure!(
        varint(&frame, &mut cursor)? == 0,
        "expected handshake packet"
    );
    let _version = varint(&frame, &mut cursor)?;
    let host_len = varint(&frame, &mut cursor)?;
    ensure!(
        host_len > 0 && host_len as usize <= max_frame,
        "invalid hostname size"
    );
    let end = cursor
        .checked_add(host_len as usize)
        .context("hostname overflow")?;
    let host = std::str::from_utf8(frame.get(cursor..end).context("truncated hostname")?)?;
    let host = crate::model::normalize_host(host.split('\0').next().unwrap_or_default())?;
    cursor = end;
    ensure!(frame.get(cursor..cursor + 2).is_some(), "truncated port");
    cursor += 2;
    ensure!(
        matches!(varint(&frame, &mut cursor)?, 1 | 2),
        "unsupported next state"
    );
    ensure!(cursor == frame.len(), "trailing handshake bytes");
    prefix.extend(frame);
    Ok((host, prefix))
}

pub fn proxy_v2(source: SocketAddr, destination: SocketAddr) -> anyhow::Result<Vec<u8>> {
    let source = canonical(source);
    let destination = canonical(destination);
    let mut bytes = b"\r\n\r\n\0\r\nQUIT\n".to_vec();
    bytes.push(0x21);
    match (source.ip(), destination.ip()) {
        (IpAddr::V4(src), IpAddr::V4(dst)) => {
            bytes.extend([0x11, 0, 12]);
            bytes.extend(src.octets());
            bytes.extend(dst.octets());
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            bytes.extend([0x21, 0, 36]);
            bytes.extend(src.octets());
            bytes.extend(dst.octets());
        }
        _ => bail!("source and destination address families differ"),
    }
    bytes.extend(source.port().to_be_bytes());
    bytes.extend(destination.port().to_be_bytes());
    Ok(bytes)
}

fn canonical(address: SocketAddr) -> SocketAddr {
    match address.ip() {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(|ip| SocketAddr::new(ip.into(), address.port()))
            .unwrap_or(address),
        _ => address,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn fragmented_handshake_preserves_extension_and_following_packet() {
        let host = b"PLAY.Asorda.Net.\0extension";
        let mut frame = vec![0, 47, host.len() as u8];
        frame.extend(host);
        frame.extend([0x63, 0xdd, 2]);
        let mut packet = vec![frame.len() as u8];
        packet.extend(frame);
        let expected = packet.clone();
        let (mut tx, mut rx) = tokio::io::duplex(128);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            for b in packet {
                tx.write_all(&[b]).await.unwrap();
            }
            tx.write_all(&[9, 8]).await.unwrap();
        });
        let (host, original) = handshake(&mut rx, 4096).await.unwrap();
        assert_eq!(host, "play.asorda.net");
        assert_eq!(original, expected);
        let mut tail = [0; 2];
        rx.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, [9, 8]);
    }
    #[tokio::test]
    async fn rejects_overflow_before_allocating() {
        let mut input = &b"\xff\xff\xff\xff\x7f"[..];
        assert!(handshake(&mut input, 4096).await.is_err());
    }
    #[test]
    fn proxy_header_encodes_addresses_and_ports() {
        let header = proxy_v2(
            "192.0.2.1:12345".parse().unwrap(),
            "192.0.2.2:25565".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(header.len(), 28);
        assert_eq!(&header[12..16], &[0x21, 0x11, 0, 12]);
        assert_eq!(&header[16..24], &[192, 0, 2, 1, 192, 0, 2, 2]);
        assert_eq!(&header[24..], &[0x30, 0x39, 0x63, 0xdd]);
        assert_eq!(
            proxy_v2("[::1]:1".parse().unwrap(), "[::1]:2".parse().unwrap())
                .unwrap()
                .len(),
            52
        );
    }
}
