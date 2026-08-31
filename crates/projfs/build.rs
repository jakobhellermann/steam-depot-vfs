fn main() {
    // Delay-load so that programs can handle non-enabled ProjFS.
    // Must be done in each binary build.rs.
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo:rustc-link-arg=/DELAYLOAD:ProjectedFSLib.dll");
        println!("cargo:rustc-link-arg=delayimp.lib");
    }
}
