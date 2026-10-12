/// Two stream descriptors per admitted connection, plus discovery/HTTP/runtime headroom.
pub fn required_descriptors(connections: usize) -> anyhow::Result<u64> {
    (connections as u64)
        .checked_mul(2)
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| anyhow::anyhow!("connection limit overflows descriptor budget"))
}

#[cfg(unix)]
pub fn ensure_descriptors(connections: usize) -> anyhow::Result<u64> {
    let needed = required_descriptors(connections)?;
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes to a valid initialized rlimit; no pointer is retained.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if limit.rlim_cur < needed {
        anyhow::ensure!(
            limit.rlim_max >= needed,
            "max-connections requires RLIMIT_NOFILE >= {needed}; hard limit is {}. Raise the container limit or lower max-connections",
            limit.rlim_max
        );
        limit.rlim_cur = needed;
        // SAFETY: setrlimit reads a valid rlimit and only raises our soft limit within its hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(limit.rlim_cur)
}

#[cfg(test)]
mod tests {
    #[test]
    fn descriptor_budget_accounts_for_both_streams() {
        assert_eq!(super::required_descriptors(4096).unwrap(), 8320);
    }
}
