#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    anyhow::ensure!(
        args.len() == 3 && matches!(args[1].as_str(), "ready" | "live"),
        "usage: aio-probe ready|live ADDRESS_FILE"
    );
    let address = tokio::fs::read_to_string(&args[2]).await?;
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?
        .get(format!("http://{}/health/{}", address.trim(), args[1]))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
