use crate::state::State;
use k8s_openapi::api::coordination::v1::Lease;
use kube::{
    Api, Client,
    api::{DeleteParams, Patch, PatchParams},
};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

pub async fn run(
    state: Arc<State>,
    address: SocketAddr,
    namespace: String,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let api: Api<Lease> = Api::namespaced(Client::try_default().await?, &namespace);
    let name = format!("aio-{}", state.instance);
    let params = PatchParams::apply("aio-registration");
    loop {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let object = serde_json::json!({"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":name,"namespace":namespace,"labels":{"app.kubernetes.io/name":"aio-agent"},"annotations":{"aio.akasha.asorda.net/instance-id":state.instance,"aio.akasha.asorda.net/node-name":state.node,"aio.akasha.asorda.net/url":format!("http://{address}"),"aio.akasha.asorda.net/updated-at":now.to_string(),"aio.akasha.asorda.net/version":env!("CARGO_PKG_VERSION")}},"spec":{"holderIdentity":state.instance,"leaseDurationSeconds":30}});
        if let Err(error) = api.patch(&name, &params, &Patch::Apply(object)).await {
            tracing::warn!(%error,"could not publish instance registration");
            state
                .service_errors
                .write()
                .unwrap()
                .insert("registration".into(), error.to_string());
        } else {
            state.service_errors.write().unwrap().remove("registration");
        }
        tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(10))=>{}}
    }
    let _ = api.delete(&name, &DeleteParams::default()).await;
    Ok(())
}
