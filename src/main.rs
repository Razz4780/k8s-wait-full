use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use kube::{
    api::{ApiResource, DynamicObject},
    discovery::{ApiGroup, Scope},
    runtime::{
        self,
        watcher::{Config, Event},
        WatchStreamExt,
    },
    Api, Client, Discovery,
};
use serde_yaml::Value;
use tokio::{
    fs,
    io::{self, AsyncReadExt},
    time,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Kind of the resource in PascalCase, e.g. `Deployment` or `ReplicaSet`.
    kind: String,

    /// Name of the resource.
    name: String,

    /// Namespace where the resource lives.
    /// Ignored for cluster-wide resources.
    #[arg(long)]
    namespace: Option<String>,

    /// Resource group. Can be used to narrow down search results when discovering available resources.
    #[arg(long)]
    group: Option<String>,

    /// Resource group version. Can be used to narrow down search results when discovering available resources.
    #[arg(long)]
    group_version: Option<String>,

    /// Resource apiVersion. Can be used to narrow down search results when discovering available resources.
    #[arg(long)]
    api_version: Option<String>,

    /// Resource plural name. Can be used to narrow down search results when discovering available resources.
    #[arg(long)]
    plural: Option<String>,

    /// Timeout for watching resource state (seconds).
    #[arg(short, long)]
    timeout: Option<u64>,

    /// Path to YAML file containing resource state filter.
    /// Omit or pass '-' to read from standard input.
    #[arg(short, long)]
    file: Option<PathBuf>,
}

impl Args {
    fn filter_resource(&self, api_resource: &ApiResource) -> bool {
        self.group
            .as_ref()
            .map(|g| *g == api_resource.group)
            .unwrap_or(true)
            && self
                .group_version
                .as_ref()
                .map(|v| *v == api_resource.version)
                .unwrap_or(true)
            && self
                .api_version
                .as_ref()
                .map(|av| *av == api_resource.api_version)
                .unwrap_or(true)
            && self.kind == api_resource.kind
            && self
                .plural
                .as_ref()
                .map(|p| *p == api_resource.api_version)
                .unwrap_or(true)
    }
}

async fn read_state_filter(path: Option<&Path>) -> anyhow::Result<Value> {
    let raw_bytes = match path {
        Some(path) if path != OsStr::new("-") => fs::read(path)
            .await
            .context("failed to read state filter from file")?,
        _ => {
            let mut buf = vec![];
            io::stdin()
                .read_to_end(&mut buf)
                .await
                .context("failed to read state filter from standard input")?;
            buf
        }
    };

    serde_yaml::from_slice(&raw_bytes).context("failed to deserialize state filter")
}

fn match_state(filter: &Value, state: &Value) -> bool {
    let serialized = serde_yaml::to_value(state).expect("serialization should not fail");

    match (filter, &serialized) {
        (Value::Mapping(m1), Value::Mapping(m2)) => {
            for (k, v1) in m1 {
                let Some(v2) = m2.get(k) else {
                    return false;
                };

                if !match_state(v1, v2) {
                    return false;
                }
            }

            true
        }

        (Value::Sequence(s1), Value::Sequence(s2)) => {
            s1.iter().all(|v1| s2.iter().all(|v2| match_state(v1, v2)))
        }

        _ => filter == &serialized,
    }
}

async fn watch_for_condition_met(
    api: Api<DynamicObject>,
    name: &str,
    filter: Value,
) -> Result<Value> {
    let config = Config {
        field_selector: Some(format!("metadata.name={name}")),
        ..Default::default()
    };

    let mut stream = Box::pin(runtime::watcher(api, config).default_backoff());
    while let Some(item) = stream.next().await {
        match item {
            Ok(Event::Applied(state)) => {
                let serialized =
                    serde_yaml::to_value(state).expect("serialization should not fail");
                if match_state(&filter, &serialized) {
                    return Ok(serialized);
                }
            }

            Ok(Event::Deleted(_)) => {}

            Ok(Event::Restarted(states)) => {
                for state in states {
                    let serialized =
                        serde_yaml::to_value(state).expect("serialization should not fail");
                    if match_state(&filter, &serialized) {
                        return Ok(serialized);
                    }
                }
            }

            Err(error) => {
                eprintln!(
                    "Watcher stream encountered an error and will restart with backoff: {error}."
                );
            }
        }
    }

    anyhow::bail!("Watcher stream finished unexpectedly");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let state_filter = read_state_filter(args.file.as_deref())
        .await
        .context("failed to construct state filter for the resource")?;

    let client = Client::try_default().await?;
    let discovery = Discovery::new(client.clone()).run().await?;

    let found = discovery
        .groups()
        .flat_map(ApiGroup::recommended_resources)
        .filter(|(api_resource, _)| args.filter_resource(api_resource))
        .collect::<Vec<_>>();

    anyhow::ensure!(
        !found.is_empty(),
        "No API resources matching filtering criteria were found"
    );

    anyhow::ensure!(
        found.len() == 1,
        "Multiple resources matching filtering criteria were found, try narrowing your filtering criteria"
    );

    let (api_resource, api_capabilities) =
        found.into_iter().next().expect("length was just checked");

    let api: Api<DynamicObject> = match api_capabilities.scope {
        Scope::Cluster => Api::all_with(client.clone(), &api_resource),
        Scope::Namespaced => Api::namespaced_with(
            client.clone(),
            args.namespace
                .as_deref()
                .unwrap_or_else(|| client.default_namespace()),
            &api_resource,
        ),
    };

    let found_state = match args.timeout {
        Some(timeout) => time::timeout(
            Duration::from_secs(timeout),
            watch_for_condition_met(api, &args.name, state_filter),
        )
        .await
        .context("timeout expired")??,
        None => watch_for_condition_met(api, &args.name, state_filter).await?,
    };

    let serialized = serde_yaml::to_string(&found_state)
        .context("failed to serialize matching resource state")?;

    println!("{serialized}");

    Ok(())
}
