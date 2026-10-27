//! A standalone counter primary's configuration, for the tests that boot
//! one on a directory of their own (`startup_events.rs`,
//! `encoder_purity.rs`).
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::path::Path;

use melin_client::{SigningKey, key};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;

/// A standalone primary's configuration on `dir`'s journal, with an
/// `authorized_keys` written there that admits `client_key` alone. `bind`
/// is left for the caller.
pub fn standalone_config(dir: &Path, client_key: &SigningKey) -> ServerConfig {
    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        key::authorized_keys_line("operator", &client_key.verifying_key(), "test") + "\n",
    )
    .expect("write auth keys");

    ServerConfig {
        journal: dir.join("counter.journal"),
        authorized_keys: auth_path,
        standalone: true,
        // The default policy waits on a replica, which a standalone node
        // has none of.
        ack_policy: AckPolicy::Disk,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and the default layout would stack every node's same-role
        // thread on one core while a spinner would starve whatever shares
        // its core, the test's own client included.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    }
}
