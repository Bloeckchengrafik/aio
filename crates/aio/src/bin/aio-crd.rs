use kube::CustomResourceExt;
fn main() {
    println!(
        "{}",
        serde_yaml::to_string(&aio::model::MinecraftRoute::crd()).unwrap()
    );
}
