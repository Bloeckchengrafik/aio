use crate::state::State;
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// Recover background services without touching already established forwarding tasks.
/// Each attempt owns its child tasks and must stop them when dropped.
pub async fn supervise<F, Fut>(
    state: Arc<State>,
    name: &'static str,
    mut start: F,
    cancel: CancellationToken,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut backoff = Duration::from_millis(100);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let started = tokio::time::Instant::now();
        // Spawn each attempt so panic is a supervised error, not loss of the supervisor.
        let mut task = tokio::spawn(start());
        let outcome = tokio::select! {
            _ = cancel.cancelled() => {
                if tokio::time::timeout(Duration::from_secs(2), &mut task).await.is_err() { task.abort(); let _ = task.await; }
                break;
            }
            result = &mut task => result,
        };
        if cancel.is_cancelled() {
            break;
        }
        let error = match outcome {
            Ok(Ok(())) => "service ended unexpectedly".into(),
            Ok(Err(error)) => error.to_string(),
            Err(error) => format!("service task failed: {error}"),
        };
        state
            .service_errors
            .write()
            .unwrap()
            .insert(name.into(), error.clone());
        state.event(format!("{name} restarting: {error}"));
        if started.elapsed() >= Duration::from_secs(30) {
            backoff = Duration::from_millis(100);
        }
        tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(backoff) => {} }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn panic_is_reported_then_restarted_without_discarding_state() {
        let state = State::new("node".into(), "instance".into());
        let calls = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervise(
            state.clone(),
            "test",
            {
                let calls = calls.clone();
                let stop = cancel.clone();
                move || {
                    let calls = calls.clone();
                    let stop = stop.clone();
                    async move {
                        if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                            panic!("injected worker crash");
                        }
                        stop.cancelled().await;
                        Ok(())
                    }
                }
            },
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while calls.load(Ordering::Relaxed) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(state.service_errors.read().unwrap().contains_key("test"));
        cancel.cancel();
        task.await.unwrap();
    }
}
