fn main() {
    // Only link with the Go shared library if lighter-sdk feature is enabled
    #[cfg(feature = "lighter-sdk")]
    {
        println!("cargo:rustc-link-search=native=/home/guest/oss/lighter-go");
        println!("cargo:rustc-link-lib=dylib=signer");

        // Rerun build if the shared library changes
        println!("cargo:rerun-if-changed=/home/guest/oss/lighter-go/libsigner.so");
    }
}
