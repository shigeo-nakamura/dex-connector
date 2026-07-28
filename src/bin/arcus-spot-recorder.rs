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
    collections::HashSet,
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::MetadataExt,
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
/// so a symlink pointing at the other output file is detected. A dangling
/// symlink (its target does not exist yet either) is followed manually,
/// since `canonicalize` refuses to resolve those; only once the chain ends
/// on a plain, still-missing path do we fall back to resolving just the
/// parent directory and reattaching the unresolved file name. Chain
/// traversal tracks visited paths instead of capping the hop count, so a
/// long-but-acyclic dangling chain still resolves fully and only a genuine
/// symlink cycle is rejected.
///
/// The resolved paths are additionally compared case-insensitively. This
/// process cannot portably learn whether the target filesystem is
/// case-sensitive (ext4), case-insensitive-but-preserving (default macOS,
/// Windows), or something else, so it conservatively treats a case-only
/// difference as a possible alias rather than assuming ext4 semantics.
///
/// Every intermediate hop *produced by following a symlink target* has its
/// redundant `.` components dropped (see `normalize_lexically`) before it is
/// checked against `visited` or read as a symlink again. Without this, a
/// self-referential relative symlink such as `a -> ./a` can produce a new,
/// textually distinct path on some hops (`a`, `./a`, `././a`, ...) even
/// though it names the same cyclic target. `..` is never lexically collapsed,
/// here or for the starting path below: a hop target such as
/// `link/../latest.json` can have `link` be a real symlink, and the
/// filesystem resolves `..` against that symlink's *target* directory, not
/// its own, so collapsing it away first — before the filesystem gets a
/// chance to see `link` — can name the wrong path entirely.
///
/// The starting path itself is, likewise, left entirely unnormalized.
/// Leaving it as given lets the OS resolve any real leading symlink through
/// the final-component canonicalization above, or through the
/// parent-canonicalization fallback below, either of which applies `..`
/// after symlinks the same way the kernel does.
fn same_output_path(a: &Path, b: &Path) -> Result<bool, Box<dyn Error>> {
    let resolve = |path: &Path| -> Result<PathBuf, Box<dyn Error>> {
        ensure_parent(path)?;
        let mut current = path.to_path_buf();
        let mut visited = HashSet::new();
        loop {
            if let Ok(canonical) = fs::canonicalize(&current) {
                return Ok(canonical);
            }
            if !visited.insert(current.clone()) {
                return Err(format!(
                    "output path resolves through a symlink cycle: {}",
                    path.display()
                )
                .into());
            }
            let Ok(target) = fs::read_link(&current) else {
                break;
            };
            current = normalize_lexically(&if target.is_absolute() {
                target
            } else {
                current
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .map(|parent| parent.join(&target))
                    .unwrap_or(target)
            });
        }
        let file_name = current
            .file_name()
            .ok_or_else(|| format!("output path must have a file name: {}", path.display()))?;
        let parent = current
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(fs::canonicalize)
            .transpose()?
            .unwrap_or(env::current_dir()?);
        Ok(parent.join(file_name))
    };
    let (resolved_a, resolved_b) = (resolve(a)?, resolve(b)?);
    if resolved_a == resolved_b || paths_equal_case_insensitive(&resolved_a, &resolved_b) {
        return Ok(true);
    }
    // Canonical path strings are not a filesystem identity: the same file or
    // directory reachable through two different bind mounts canonicalizes to
    // two different (but equally valid) strings. Fall back to comparing
    // device/inode, which bind mounts of the same underlying filesystem
    // share. When the final component does not exist yet on either side
    // (the common case for a fresh recorder run), compare the parent
    // directory's device/inode plus file name instead, since there is no
    // inode for the missing file itself to compare.
    if let (Ok(meta_a), Ok(meta_b)) = (fs::metadata(&resolved_a), fs::metadata(&resolved_b)) {
        if meta_a.dev() == meta_b.dev() && meta_a.ino() == meta_b.ino() {
            return Ok(true);
        }
    }
    if let (Some(parent_a), Some(parent_b), Some(name_a), Some(name_b)) = (
        resolved_a.parent(),
        resolved_b.parent(),
        resolved_a.file_name(),
        resolved_b.file_name(),
    ) {
        if let (Ok(meta_a), Ok(meta_b)) = (fs::metadata(parent_a), fs::metadata(parent_b)) {
            if meta_a.dev() == meta_b.dev()
                && meta_a.ino() == meta_b.ino()
                && name_a.to_string_lossy().to_lowercase()
                    == name_b.to_string_lossy().to_lowercase()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Lexically drops `.` components without touching the filesystem or
/// collapsing `..`, so repeated symlink-chain hops converge on one textual
/// representation instead of growing a new `./` prefix per hop.
///
/// `..` is deliberately left untouched. It used to be collapsed against a
/// preceding `Normal` component here, but a hop target such as
/// `link/../latest.json` can have `link` be a real symlink; the kernel then
/// resolves `..` against `link`'s *target* directory, not its own, so
/// collapsing it away lexically before the filesystem gets a chance to see
/// `link` can name the wrong path entirely (`same_output_path` would then
/// compare that wrong path against the real one and report false aliases as
/// distinct). `.` alone is always the identity component regardless of
/// what's around it, so dropping just that is enough to make a
/// self-referential relative symlink (`a -> ./a`) converge to a repeated,
/// `visited`-detectable path across hops instead of growing forever.
fn normalize_lexically(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    normalized
}

fn paths_equal_case_insensitive(a: &Path, b: &Path) -> bool {
    a.to_string_lossy().to_lowercase() == b.to_string_lossy().to_lowercase()
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
    let temporary = path.with_file_name(format!(
        ".{file_name}.{}.{nonce:016x}.tmp",
        std::process::id()
    ));
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
    fn same_output_path_detects_a_dangling_symlink_pointing_at_the_other_output_file() {
        let dir = unique_temp_dir("dangling-symlink-alias");
        let latest = dir.join("latest.json");
        let jsonl = dir.join("archive.jsonl");
        std::os::unix::fs::symlink(&latest, &jsonl).unwrap();
        assert!(same_output_path(&jsonl, &latest).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_detects_a_dangling_alias_beyond_the_old_traversal_cap() {
        let dir = unique_temp_dir("long-dangling-chain");
        let latest = dir.join("latest.json");
        let mut current = latest.clone();
        // One more hop than the traversal cap this same_output_path used to
        // apply (32), still well within Linux's own symlink resolution
        // limit, so the chain must resolve fully rather than falling
        // through to a mismatched intermediate hop.
        for i in 0..33 {
            let next = dir.join(format!("hop-{i}"));
            std::os::unix::fs::symlink(&current, &next).unwrap();
            current = next;
        }
        let jsonl = current;
        assert!(same_output_path(&jsonl, &latest).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_detects_case_only_aliases_conservatively() {
        // Neither file exists, so this exercises the parent-based fallback.
        // On ext4 these are genuinely distinct inodes; on a case-insensitive
        // filesystem they would collide. We cannot portably tell which
        // filesystem is in play, so the guard must treat this as an alias
        // either way rather than assume ext4 semantics.
        let dir = unique_temp_dir("case-alias");
        let lower = dir.join("archive.jsonl");
        let upper = dir.join("ARCHIVE.JSONL");
        assert!(same_output_path(&lower, &upper).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_detects_a_hard_linked_alias_with_distinct_names() {
        // Two hard links to the same inode canonicalize to two textually
        // distinct paths (no symlink is involved for canonicalize to
        // resolve away), the same shape of alias a bind-mounted duplicate
        // path would produce. The device/inode fallback must still catch it.
        let dir = unique_temp_dir("hard-link-alias");
        let archive = dir.join("archive.jsonl");
        fs::write(&archive, b"{}\n").unwrap();
        let linked = dir.join("also-archive.jsonl");
        fs::hard_link(&archive, &linked).unwrap();
        assert!(same_output_path(&archive, &linked).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_resolves_a_dot_dot_that_follows_a_real_symlink() {
        // `dir/link -> dir/other/sub`, and the configured path is
        // `dir/link/../file.json`. Resolving `link` first lands in
        // `dir/other/sub`, so `..` must climb from there to `dir/other`, not
        // lexically cancel `link` against `..` and land back in `dir`.
        let dir = unique_temp_dir("dotdot-through-symlink");
        let other_dir = dir.join("other");
        let sub_dir = other_dir.join("sub");
        fs::create_dir_all(&sub_dir).unwrap();
        let link = dir.join("link");
        std::os::unix::fs::symlink(&sub_dir, &link).unwrap();

        let direct = other_dir.join("file.json");
        let aliased = dir.join("link").join("..").join("file.json");
        assert!(same_output_path(&direct, &aliased).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_resolves_a_dot_dot_reached_through_a_dangling_hop() {
        // `dir/archive.jsonl -> link/../latest.json` (a dangling, relative
        // symlink target), and `dir/link -> dir/other/sub` (a real symlink).
        // This exercises the hop-target join inside the traversal loop, not
        // the starting-path handling covered above: the kernel resolves the
        // target to `dir/other/latest.json`, so lexically collapsing
        // `link/..` away before canonicalize/read_link ever see the real
        // `link` symlink must not make this compare as a distinct path.
        let dir = unique_temp_dir("dotdot-through-hop");
        let other_dir = dir.join("other");
        let sub_dir = other_dir.join("sub");
        fs::create_dir_all(&sub_dir).unwrap();
        let link = dir.join("link");
        std::os::unix::fs::symlink(&sub_dir, &link).unwrap();

        let archive = dir.join("archive.jsonl");
        std::os::unix::fs::symlink("link/../latest.json", &archive).unwrap();

        let direct = other_dir.join("latest.json");
        assert!(same_output_path(&archive, &direct).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_rejects_a_symlink_cycle() {
        let dir = unique_temp_dir("symlink-cycle");
        let a = dir.join("a");
        let b = dir.join("b");
        std::os::unix::fs::symlink(&b, &a).unwrap();
        std::os::unix::fs::symlink(&a, &b).unwrap();
        assert!(
            same_output_path(&a, &dir.join("latest.json")).is_err(),
            "a symlink cycle must be rejected instead of silently falling \
             through to the missing-path fallback"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_output_path_rejects_a_self_referential_relative_symlink() {
        // `a -> ./a` is a one-hop cycle. Without lexical normalization,
        // whether the `visited` guard converges on a repeat quickly depends
        // on the input path shape (a bare relative starting path can grow a
        // new, textually distinct `./`-prefixed hop forever instead of
        // repeating), so this runs with a timeout as a safety net rather
        // than assuming a particular non-normalized traversal length.
        let dir = unique_temp_dir("self-referential-symlink");
        let a = dir.join("a");
        std::os::unix::fs::symlink("./a", &a).unwrap();
        let latest = dir.join("latest.json");

        let (result_tx, result_rx) = mpsc::channel();
        thread::spawn(move || {
            let result = same_output_path(&a, &latest).map_err(|error| error.to_string());
            let _ = result_tx.send(result);
        });
        let result = result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same_output_path did not terminate on a self-referential symlink");
        assert!(
            result.is_err(),
            "a self-referential relative symlink must be rejected as a cycle"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn normalize_lexically_drops_dot_but_never_collapses_dot_dot() {
        assert_eq!(normalize_lexically(Path::new("./a")), Path::new("a"));
        assert_eq!(normalize_lexically(Path::new("././a")), Path::new("a"));
        // `..` must survive uncollapsed even when a normal component
        // precedes it lexically: that component could be a real symlink, and
        // only the filesystem (not this function) knows what `..` resolves
        // against in that case.
        assert_eq!(
            normalize_lexically(Path::new("dir/./sub/../file")),
            Path::new("dir/sub/../file")
        );
        assert_eq!(
            normalize_lexically(Path::new("/dir/./sub/../file")),
            Path::new("/dir/sub/../file")
        );
        assert_eq!(normalize_lexically(Path::new("../a")), Path::new("../a"));
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
