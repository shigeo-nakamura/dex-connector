fn main() {
    // Only link with the Go shared library if lighter-sdk feature is enabled
    #[cfg(feature = "lighter-sdk")]
    {
        // Dynamically get path from environment variable or auto-detect
        let lighter_go_path = std::env::var("LIGHTER_GO_PATH").unwrap_or_else(|_| {
            if std::path::Path::new("/home/guest/oss/lighter-go").exists() {
                "/home/guest/oss/lighter-go".to_string()
            } else {
                let home = std::env::var("HOME").unwrap_or("/home/ec2-user".to_string());
                format!("{}/lighter-go", home)
            }
        });

        println!("cargo:rustc-link-search=native={}", lighter_go_path);
        println!("cargo:rustc-link-lib=dylib=signer");

        // Rerun build if the shared library changes
        println!("cargo:rerun-if-changed={}/libsigner.so", lighter_go_path);
    }
}
