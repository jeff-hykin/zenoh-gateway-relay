//! `--auth-file`: the viewers' tokens, the format of zenoh-web-cli's (`{ tokens: { "<token>": "read" | "write" |
//! "lease" | <grant> }, leaseGroups: { "<group>": ["<key expr>"] } }`), re-read when it changes.

use anyhow::{Result, anyhow, bail};
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use zenoh_web::{Grant, Server, ServerBuilder};

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
struct AuthFile {
    tokens: HashMap<String, Role>,
    lease_groups: HashMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Role {
    Named(String),
    Grant(Grant),
}

/// read: subscribe, query and list everything; write: also publish; lease: also lease any group.
fn role_grant(role: &Role) -> Result<Grant> {
    let all = || vec!["**".to_owned()];
    let read = Grant { subscribe: all(), query: all(), list_topics: all(), ..Default::default() };
    match role {
        Role::Grant(grant) => Ok(grant.clone()),
        Role::Named(name) => match name.as_str() {
            "read" => Ok(read),
            "write" => Ok(Grant { publish: all(), ..read }),
            "lease" => Ok(Grant { publish: all(), lease_groups: vec!["*".into()], ..read }),
            other => bail!("unknown role {other:?} (read, write, lease, or a grant object)"),
        },
    }
}

/// token -> grant, and lease group -> keys
type Loaded = (HashMap<String, Grant>, HashMap<String, Vec<String>>);

fn load(path: &Path) -> Result<Loaded> {
    let file: AuthFile = json5::from_str(&std::fs::read_to_string(path)?).map_err(|error| anyhow!("{}: {error}", path.display()))?;
    let tokens = file.tokens.iter().map(|(token, role)| Ok((token.clone(), role_grant(role)?))).collect::<Result<_>>()?;
    Ok((tokens, file.lease_groups))
}

/// Viewers' tokens from a file: [`apply`](Self::apply) it to the viewers' server, then [`watch`](Self::watch) it.
#[derive(Clone)]
pub struct AuthFileTokens {
    path: PathBuf,
    tokens: Arc<RwLock<HashMap<String, Grant>>>,
    lease_groups: HashMap<String, Vec<String>>,
}

impl AuthFileTokens {
    /// Reads the file.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let (tokens, lease_groups) = load(&path)?;
        Ok(AuthFileTokens { path, tokens: Arc::new(RwLock::new(tokens)), lease_groups })
    }

    /// Adds the authorize hook (a token is required) and the file's lease groups.
    pub fn apply(&self, mut builder: ServerBuilder) -> ServerBuilder {
        for (name, keys) in &self.lease_groups {
            builder = builder.lease_group(name.clone(), keys.clone());
        }
        let tokens = self.tokens.clone();
        builder.authorize(move |token, _headers| {
            let token = token.ok_or("a token is required (connect(url, { token }))")?;
            tokens.read().unwrap().get(token).cloned().ok_or_else(|| "unknown token".to_owned())
        })
    }

    /// Polls the file; a token removed or changed there is revoked (its connections close). Runs forever.
    pub async fn watch(self, server: Server) {
        let modified = |path: &Path| std::fs::metadata(path).and_then(|meta| meta.modified()).ok();
        let mut last = modified(&self.path);
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let now = modified(&self.path);
            if now == last {
                continue;
            }
            last = now;
            match load(&self.path) {
                Ok((new_tokens, _)) => {
                    let old = std::mem::replace(&mut *self.tokens.write().unwrap(), new_tokens.clone());
                    for (token, grant) in old {
                        if new_tokens.get(&token) != Some(&grant) {
                            info!("auth file: token changed or removed, revoked {} connection(s)", server.revoke(&token));
                        }
                    }
                }
                Err(error) => warn!("auth file not reloaded: {error:#}"),
            }
        }
    }
}
