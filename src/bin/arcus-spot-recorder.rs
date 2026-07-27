//! One-shot, read-only Arcus Spot indicative round-trip recorder.
//!
//! ARCUS_SPOT_PAIRS and ARCUS_SPOT_NOTIONALS_USD select the matrix.
//! ARCUS_SPOT_JSONL_PATH appends one compact snapshot and
//! ARCUS_SPOT_LATEST_PATH atomically replaces one pretty snapshot. With
//! neither output path set, the compact snapshot is written to stdout.
//! Endpoint, timeout, pacing, and retry fields can be overridden with the
//! corresponding ARCUS_SPOT_* environment variables below.

use dex_connector::{ArcusSpotClient, ArcusSpotConfig, ArcusSpotRecorder, ArcusSpotRecorderConfig};
use fs2::FileExt;
use std::{
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

const DEFAULT_PAIRS: &str = "NVDA/AMD,SPY/QQQ,META/GOOGL";
const DEFAULT_NOTIONALS_USD: &str = "5,10,25,50";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let client_config = client_config_from_env()?;
    let recorder_config = ArcusSpotRecorderConfig::from_csv(
        &env::var("ARCUS_SPOT_PAIRS").unwrap_or_else(|_| DEFAULT_PAIRS.to_string()),
        &env::var("ARCUS_SPOT_NOTIONALS_USD").unwrap_or_else(|_| DEFAULT_NOTIONALS_USD.to_string()),
    )?;
    let recorder = ArcusSpotRecorder::new(ArcusSpotClient::new(client_config)?, recorder_config)?;
    let snapshot = recorder.collect_once().await;
    let compact = serde_json::to_string(&snapshot)?;

    let jsonl_path = optional_path("ARCUS_SPOT_JSONL_PATH");
    let latest_path = optional_path("ARCUS_SPOT_LATEST_PATH");
    if let (Some(jsonl), Some(latest)) = (&jsonl_path, &latest_path) {
        if same_output_path(jsonl, latest)? {
            return Err(format!(
                "ARCUS_SPOT_JSONL_PATH and ARCUS_SPOT_LATEST_PATH must not resolve to the \
                 same file, or every run erases the JSONL archive: {}",
                jsonl.display()
            )
            .into());
        }
    }
    if let Some(path) = &jsonl_path {
        append_jsonl(path, &compact)?;
    }
    if let Some(path) = &latest_path {
        write_latest(path, serde_json::to_string_pretty(&snapshot)?.as_bytes())?;
    }
    if jsonl_path.is_none() && latest_path.is_none() {
        println!("{compact}");
    }

    if snapshot.is_complete() {
        Ok(())
    } else {
        Err("Arcus Spot recorder completed with captured errors".into())
    }
}

fn client_config_from_env() -> Result<ArcusSpotConfig, Box<dyn Error>> {
    let mut config = ArcusSpotConfig::default();
    override_string("ARCUS_SPOT_ROUTER_BASE_URL", &mut config.router_base_url);
    override_string("ARCUS_SPOT_META_BASE_URL", &mut config.meta_base_url);
    override_string("ARCUS_SPOT_INDEXER_BASE_URL", &mut config.indexer_base_url);
    override_from_str("ARCUS_SPOT_CHAIN_ID", &mut config.chain_id)?;
    override_from_str(
        "ARCUS_SPOT_REQUEST_TIMEOUT_MS",
        &mut config.request_timeout_ms,
    )?;
    override_from_str(
        "ARCUS_SPOT_MIN_REQUEST_INTERVAL_MS",
        &mut config.min_request_interval_ms,
    )?;
    override_from_str("ARCUS_SPOT_MAX_ATTEMPTS", &mut config.max_attempts)?;
    override_from_str(
        "ARCUS_SPOT_RETRY_BASE_DELAY_MS",
        &mut config.retry_base_delay_ms,
    )?;
    override_from_str(
        "ARCUS_SPOT_MAX_RETRY_DELAY_MS",
        &mut config.max_retry_delay_ms,
    )?;
    Ok(config)
}

fn override_string(name: &str, target: &mut String) {
    if let Ok(value) = env::var(name) {
        *target = value;
    }
}

fn override_from_str<T>(name: &str, target: &mut T) -> Result<(), Box<dyn Error>>
where
    T: FromStr,
    T::Err: Error + 'static,
{
    if let Ok(value) = env::var(name) {
        *target = value.parse()?;
    }
    Ok(())
}

fn optional_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn ensure_parent(path: &Path) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Resolves each path to catch aliases (symlinks, relative segments,
/// symlinked directories) that plain `PathBuf` equality misses. If the path
/// itself already exists, it is canonicalized through its final component
/// so a symlink pointing at the other output file is detected. Otherwise
/// (the file has not been written yet) only its parent directory is
/// resolved and the unresolved file name is reattached.
fn same_output_path(a: &Path, b: &Path) -> Result<bool, Box<dyn Error>> {
    let resolve = |path: &Path| -> Result<PathBuf, Box<dyn Error>> {
        ensure_parent(path)?;
        if let Ok(canonical) = fs::canonicalize(path) {
            return Ok(canonical);
        }
        let file_name = path
            .file_name()
            .ok_or_else(|| format!("output path must have a file name: {}", path.display()))?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(fs::canonicalize)
            .transpose()?
            .unwrap_or(env::current_dir()?);
        Ok(parent.join(file_name))
    };
    Ok(resolve(a)? == resolve(b)?)
}

fn append_jsonl(path: &Path, json: &str) -> Result<(), Box<dyn Error>> {
    ensure_parent(path)?;
    let mut record = Vec::with_capacity(json.len().saturating_add(1));
    record.extend_from_slice(json.as_bytes());
    record.push(b'\n');
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.lock_exclusive()?;
    let write_result = file.write_all(&record).and_then(|()| file.sync_data());
    let unlock_result = FileExt::unlock(&file);
    write_result?;
    unlock_result?;
    Ok(())
}

fn write_latest(path: &Path, json: &[u8]) -> Result<(), Box<dyn Error>> {
    ensure_parent(path)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("latest output path must have a UTF-8 file name")?;
    let nonce: u64 = rand::random();
    let temporary =
        path.with_file_name(format!(".{file_name}.{}.{nonce:016x}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(json)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, SystemTime},
    };

    #[test]
    fn jsonl_append_waits_for_an_exclusive_file_lock() {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "arcus-spot-recorder-{}-{nonce}.jsonl",
            std::process::id()
        ));
        let blocker = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        blocker.lock_exclusive().unwrap();

        let (ready_tx, ready_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let writer_path = path.clone();
        let writer = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let result =
                append_jsonl(&writer_path, r#"{"writer":1}"#).map_err(|error| error.to_string());
            result_tx.send(result).unwrap();
        });

        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        match result_rx.recv_timeout(Duration::from_millis(200)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the second writer exited before reporting its result")
            }
            Ok(result) => {
                panic!("the second writer bypassed the exclusive file lock: {result:?}")
            }
        }
        FileExt::unlock(&blocker).unwrap();
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        writer.join().unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines, [r#"{"writer":1}"#]);
        serde_json::from_str::<serde_json::Value>(lines[0]).unwrap();
        fs::remove_file(path).unwrap();
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "arcus-spot-recorder-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn same_output_path_detects_identical_configured_paths() {
        let dir = unique_temp_dir("same-path");
        let path = dir.join("snapshot.json");
        assert!(same_output_path(&path, &path).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_detects_relative_segment_aliases() {
        let dir = unique_temp_dir("relative-alias");
        let direct = dir.join("snapshot.json");
        let aliased = dir.join("nested").join("..").join("snapshot.json");
        assert!(same_output_path(&direct, &aliased).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_rejects_distinct_paths() {
        let dir = unique_temp_dir("distinct");
        let jsonl = dir.join("archive.jsonl");
        let latest = dir.join("latest.json");
        assert!(!same_output_path(&jsonl, &latest).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_detects_a_symlink_pointing_at_the_other_output_file() {
        let dir = unique_temp_dir("symlink-alias");
        let latest = dir.join("latest.json");
        fs::write(&latest, b"{}").unwrap();
        let jsonl = dir.join("archive.jsonl");
        std::os::unix::fs::symlink(&latest, &jsonl).unwrap();
        assert!(same_output_path(&jsonl, &latest).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_latest_rejects_a_preexisting_symlink_at_the_temp_path() {
        let dir = unique_temp_dir("symlink");
        let target = dir.join("attacker-owned");
        fs::write(&target, b"do not overwrite me").unwrap();
        let path = dir.join("latest.json");
        let nonce_guess: u64 = rand::random();
        let temp_name = format!(".latest.json.{}.{nonce_guess:016x}.tmp", std::process::id());
        std::os::unix::fs::symlink(&target, dir.join(&temp_name)).unwrap();

        // The real call picks its own random nonce, so this test only proves
        // that *some* pre-existing path at the temp naming scheme is refused
        // rather than followed; it does not depend on guessing the nonce.
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.join(&temp_name));
        assert!(file.is_err(), "create_new must refuse an existing symlink");
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "do not overwrite me",
            "the symlink target must be left untouched"
        );

        write_latest(&path, b"{}").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "{}\n");
        fs::remove_dir_all(dir).unwrap();
    }
}
