# DEX Connector

A Rust library for connecting to multiple decentralized exchanges (DEX) with conditional feature support.

## Supported DEX Protocols

- **Hyperliquid**: Native Rust implementation (always available)
- **Lighter**: Go SDK integration (conditional feature)
- **Others**: Extensible architecture for additional protocols

## Feature Flags

### `lighter-sdk` (default: enabled)
Enables Lighter Go SDK integration with external shared library dependency.

**When enabled:**
- Full Lighter DEX functionality
- Requires `libsigner.so` shared library
- Go FFI bindings available

**When disabled:**
- Lighter functions return runtime errors
- No shared library dependencies
- Reduced binary size

## Build Options

### Default Build (All Features)
```bash
cargo build
```

### Lightweight Build (No Lighter SDK)
```bash
cargo build --no-default-features
```

### Custom Features
```bash
cargo build --features lighter-sdk  # Enable only Lighter SDK
```

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
│   ├── hyperliquid_connector.rs  # Hyperliquid implementation
│   └── lighter_connector.rs      # Lighter implementation (conditional)
├── Cargo.toml                    # Feature flag configuration
└── README.md                     # This file
```

## Integration Notes

### For Library Users
- Use default features for full functionality
- Use `--no-default-features` for minimal dependencies
- Handle runtime errors gracefully when features are disabled

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