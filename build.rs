fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let cuda_enabled = std::env::var_os("CARGO_FEATURE_CUDA").is_some();

    if target_os == "linux" && cuda_enabled {
        println!("cargo:rustc-link-arg-bin=memex=-Wl,-rpath,$ORIGIN");
        println!("cargo:rustc-link-arg-bin=memex=-Wl,-rpath,$ORIGIN/deps");
    }
}
