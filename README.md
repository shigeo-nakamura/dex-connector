# DEX Connector

A Rust library for connecting to multiple decentralized exchanges (DEX) with conditional feature support.

## Supported DEX Protocols

- **Hyperliquid**: Native Rust implementation (always available)
- **Lighter**: Go SDK integration (conditional feature)
- **Extended (Perp)**: Native Rust implementation
- **Others**: Extensible architecture for additional protocols

## Feature Flags

### `lighter-sdk` (default: **disabled** for safety)
Enables Lighter Go SDK integration with external shared library dependency.

**When enabled:**
- Full Lighter DEX functionality
- Requires `libsigner.so` shared library
- Go FFI bindings available
- **Architecture-specific**: Must match target platform

**When disabled:**
- Lighter functions return runtime errors
- No shared library dependencies
- Reduced binary size
- Safe for all architectures

## Build Options

### Default Build (Safe for All Architectures)
```bash
cargo build
```
Builds without native dependencies. Hyperliquid and other connectors work normally.

### With Lighter SDK Support
```bash
cargo build --features lighter-sdk
```

### Cross-Platform Development

**Development Environment (x86_64):**
```bash
# Install x86_64 Go libraries
cd /path/to/lighter-go
go build -buildmode=c-shared -trimpath -o ./build/signer-amd64.so ./sharedlib/sharedlib.go

# Build with Lighter support
export LIGHTER_GO_PATH=/path/to/lighter-go/build
export LD_LIBRARY_PATH=$LIGHTER_GO_PATH:$LD_LIBRARY_PATH
cargo build --features lighter-sdk
```

**Production Environment (ARM64):**
```bash
# Build ARM64 Go libraries
cd /path/to/lighter-go
go build -buildmode=c-shared -trimpath -o ./build/signer-arm64.so ./sharedlib/sharedlib.go

# Set environment and build
export LIGHTER_GO_PATH=/path/to/lighter-go/build
export LD_LIBRARY_PATH=$LIGHTER_GO_PATH:$LD_LIBRARY_PATH
cargo build --features lighter-sdk
```

**Important:** Always ensure `libsigner.so` matches your target architecture. Using x86_64 libraries on ARM64 (or vice versa) will cause segmentation faults.

## Usage Examples

### Creating Connectors

```rust
use dex_connector::*;

// Hyperliquid (always available)
let hyperliquid = create_hyperliquid_connector(
    api_key,
    secret_key,
    base_url,
    websocket_url,
)?;

// Lighter (requires lighter-sdk feature)
let lighter = create_lighter_connector(
    api_key,
    api_key_index,
    api_private_key,
    account_index,
    base_url,
    websocket_url,
)?;

// Extended (Perp)
let extended = create_extended_connector(
    api_key,
    public_key,
    private_key,
    vault,
    Some(base_url),
).await?;
```

### Feature-Conditional Code

The `lighter-sdk` feature enables integration with the Lighter Go shared library for cryptographic operations. When this feature is not available, the connector will return appropriate error messages.

## Dependencies

### Always Required
- Standard Rust crypto libraries
- HTTP/WebSocket clients
- Serde for JSON handling

### Conditional (lighter-sdk feature)
- `libc` for FFI
- External Go shared library (`libsigner.so`)

## Architecture

```
dex-connector/
├── src/
│   ├── lib.rs                    # Public API and traits
│   ├── dex_connector.rs          # Core connector trait
│   ├── extended_connector.rs     # Extended (Perp) implementation
│   ├── hyperliquid_connector.rs  # Hyperliquid implementation
│   └── lighter_connector.rs      # Lighter implementation (conditional)
├── Cargo.toml                    # Feature flag configuration
└── README.md                     # This file
```

## Integration Notes

### For Library Users
- **Default**: Safe build without native dependencies
- **Enable lighter-sdk**: Only when you have matching architecture libraries
- **Handle errors**: Gracefully handle disabled feature runtime errors
- **Cross-platform**: Always rebuild `libsigner.so` for target architecture
- **Extended**: Uses `rust-crypto-lib-base` (from `stark-crypto-wrapper`) for Stark order hashing/signing

### For Contributors
- Mark Lighter-specific code with `#[cfg(feature = "lighter-sdk")]`
- Provide meaningful error messages for disabled features
- Keep core functionality independent of optional features

## Error Handling

When `lighter-sdk` feature is disabled, Lighter-related functions return:
```rust
Err(DexError::Other(
    "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
))
```

This allows applications to handle missing functionality gracefully.
