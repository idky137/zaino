//! Zingo-RPC primitives.

use nym_sdk::mixnet::MixnetClient;
use std::sync::{atomic::AtomicBool, Arc};

/// Configuration data for gRPC server.
pub struct ProxyClient {
    /// Lightwalletd uri.
    /// Used by grpc_passthrough to pass on unimplemented RPCs.
    pub lightwalletd_uri: http::Uri,
    /// Zebrad uri.
    pub zebrad_uri: http::Uri,
    /// Represents the Online status of the gRPC server.
    pub online: Arc<AtomicBool>,
}

/// Wrapper struct for a Nym client.
pub struct NymClient(pub MixnetClient);