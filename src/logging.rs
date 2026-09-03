use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Metadata;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::diagnostics::{AccessDiagnostics, Diagnostics};

pub const FEEDS_TARGET: &str = "flowsurface_server::feeds";
pub const ACCESS_TARGET: &str = "flowsurface_server::access";

const LOG_RETENTION_DAYS: usize = 14;
const LOG_MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;
const LOG_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const LOG_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const ACCESS_SUMMARY_INTERVAL: Duration = Duration::from_secs(60);
const OVERSIZED_EVENT_MARKER: &[u8] = b"[log event omitted: exceeds segment limit]\n";

type FallbackWriter = Arc<Mutex<Box<dyn Write + Send>>>;

pub fn init(data_dir: &Path) {
    let logs_dir = data_dir.join("logs");
    let writers = LogWriters::new(&logs_dir);
    let access_file_available = writers.access.is_some();

    let stdout = tracing_subscriber::fmt::layer()
        .compact()
        .with_target(false)
        .with_ansi(true)
        .with_filter(filter_fn(move |metadata| {
            !is_access_target(metadata) || !access_file_available
        }));
    let server = writers.server.map(|writer| {
        tracing_subscriber::fmt::layer()
            .compact()
            .with_target(false)
            .with_ansi(false)
            .with_writer(writer)
            .with_filter(filter_fn(is_server_target))
    });
    let feeds = writers.feeds.map(|writer| {
        tracing_subscriber::fmt::layer()
            .compact()
            .with_target(false)
            .with_ansi(false)
            .with_writer(writer)
            .with_filter(filter_fn(is_feed_target))
    });
    let access = writers.access.map(|writer| {
        tracing_subscriber::fmt::layer()
            .compact()
            .with_target(false)
            .with_ansi(false)
            .with_writer(writer)
            .with_filter(filter_fn(is_access_target))
    });

    let subscriber = tracing_subscriber::registry()
        .with(default_env_filter())
        .with(stdout)
        .with(server)
        .with(feeds)
        .with(access);

    if let Err(error) = subscriber.try_init() {
        eprintln!("Failed to install logging subscriber: {error}");
    }
}

struct LogWriters {
    server: Option<RollingMakeWriter>,
    feeds: Option<RollingMakeWriter>,
    access: Option<RollingMakeWriter>,
}

impl LogWriters {
    fn new(directory: &Path) -> Self {
        Self {
            server: initialize_category(directory, "server", Some(Box::new(io::stderr()))),
            feeds: initialize_category(directory, "feeds", Some(Box::new(io::stderr()))),
            access: initialize_category(directory, "access", Some(Box::new(io::stderr()))),
        }
    }
}

fn initialize_category(
    directory: &Path,
    prefix: &str,
    fallback: Option<Box<dyn Write + Send>>,
) -> Option<RollingMakeWriter> {
    let result = match fallback {
        Some(writer) => RollingMakeWriter::new_with_fallback(directory, prefix, writer),
        None => RollingMakeWriter::new(directory, prefix),
    };

    match result {
        Ok(writer) => Some(writer),
        Err(error) => {
            eprintln!("Failed to initialise {prefix} log: {error:#}");
            None
        }
    }
}

pub fn spawn_access_summary_reporter(
    diagnostics: Arc<Diagnostics>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut previous = AccessDiagnostics::default();
        let mut interval = tokio::time::interval(ACCESS_SUMMARY_INTERVAL);
        interval.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    report_access_delta(&diagnostics, &mut previous);
                    break;
                }
                _ = interval.tick() => {
                    report_access_delta(&diagnostics, &mut previous);
                }
            }
        }
    })
}

fn report_access_delta(diagnostics: &Diagnostics, previous: &mut AccessDiagnostics) {
    let current = diagnostics.access_counts();
    let delta = current.delta_since(*previous);
    *previous = current;

    if delta.is_zero() {
        return;
    }

    tracing::info!(
        target: ACCESS_TARGET,
        auth_failures = delta.auth_failures,
        admission_blocked = delta.admission_blocked,
        rate_limited = delta.rate_limited,
        connection_rejected = delta.connection_rejected,
        accept_timeouts = delta.accept_timeouts,
        "Access summary for the last interval",
    );
}

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,flowsurface_exchange=warn"))
}

fn is_server_target(metadata: &Metadata<'_>) -> bool {
    !is_feed_target(metadata) && !is_access_target(metadata)
}

fn is_feed_target(metadata: &Metadata<'_>) -> bool {
    let target = metadata.target();
    target == FEEDS_TARGET
        || target.starts_with("flowsurface_server::stream")
        || target.starts_with("flowsurface_exchange")
}

fn is_access_target(metadata: &Metadata<'_>) -> bool {
    metadata.target() == ACCESS_TARGET
}

#[derive(Clone)]
struct RollingMakeWriter {
    inner: Arc<Mutex<RollingFile>>,
}

impl RollingMakeWriter {
    fn new(directory: &Path, prefix: &str) -> io::Result<Self> {
        Self::new_with_optional_fallback(directory, prefix, None)
    }

    fn new_with_fallback(
        directory: &Path,
        prefix: &str,
        fallback: Box<dyn Write + Send>,
    ) -> io::Result<Self> {
        Self::new_with_optional_fallback(directory, prefix, Some(Arc::new(Mutex::new(fallback))))
    }

    fn new_with_optional_fallback(
        directory: &Path,
        prefix: &str,
        fallback: Option<FallbackWriter>,
    ) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(RollingFile::new_with_fallback(
                directory,
                prefix,
                LOG_RETENTION_DAYS,
                fallback,
            )?)),
        })
    }
}

impl<'a> MakeWriter<'a> for RollingMakeWriter {
    type Writer = RollingWriterGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        RollingWriterGuard {
            guard: self
                .inner
                .lock()
                .expect("rolling log writer mutex poisoned"),
        }
    }
}

struct RollingWriterGuard<'a> {
    guard: MutexGuard<'a, RollingFile>,
}

impl Write for RollingWriterGuard<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.guard.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

struct RollingFile {
    directory: PathBuf,
    prefix: String,
    retention_days: usize,
    current_date: String,
    current_segment: u32,
    current_path: PathBuf,
    current_size: u64,
    max_file_bytes: u64,
    max_total_bytes: u64,
    suspended_until: Option<Instant>,
    fallback: Option<FallbackWriter>,
    file: File,
}

impl RollingFile {
    fn new_with_fallback(
        directory: &Path,
        prefix: &str,
        retention_days: usize,
        fallback: Option<FallbackWriter>,
    ) -> io::Result<Self> {
        Self::new_with_limits_and_fallback(
            directory,
            prefix,
            retention_days,
            LOG_MAX_FILE_BYTES,
            LOG_MAX_TOTAL_BYTES,
            fallback,
        )
    }

    fn new_with_limits_and_fallback(
        directory: &Path,
        prefix: &str,
        retention_days: usize,
        max_file_bytes: u64,
        max_total_bytes: u64,
        fallback: Option<FallbackWriter>,
    ) -> io::Result<Self> {
        if max_file_bytes == 0 || max_total_bytes < max_file_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log size limits must be positive and the total must fit one file",
            ));
        }

        fs::create_dir_all(directory)?;
        let current_date = utc_date(SystemTime::now());
        let (current_segment, current_path, file, current_size) =
            open_current_log_file(directory, prefix, &current_date, max_file_bytes)?;
        let mut rolling_file = Self {
            directory: directory.to_path_buf(),
            prefix: prefix.to_owned(),
            retention_days,
            current_date,
            current_segment,
            current_path,
            current_size,
            max_file_bytes,
            max_total_bytes,
            suspended_until: None,
            fallback,
            file,
        };
        if let Err(error) = rolling_file.prune_old_files() {
            rolling_file.suspend_file_logging(&error);
        }
        Ok(rolling_file)
    }

    fn prepare_for_event(&mut self, event_size: u64) -> io::Result<()> {
        let date = utc_date(SystemTime::now());
        if date != self.current_date {
            let (segment, path, file, size) =
                open_current_log_file(&self.directory, &self.prefix, &date, self.max_file_bytes)?;
            self.file = file;
            self.current_date = date;
            self.current_segment = segment;
            self.current_path = path;
            self.current_size = size;
            if let Err(error) = self.prune_old_files() {
                self.suspend_file_logging(&error);
                return Ok(());
            }
        }

        if self.current_size.saturating_add(event_size) > self.max_file_bytes {
            self.rotate_segment()?;
            if let Err(error) = self.prune_old_files() {
                self.suspend_file_logging(&error);
            }
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        let next_segment = self
            .current_segment
            .checked_add(1)
            .ok_or_else(|| io::Error::other("log segment number exhausted"))?;
        let (segment, path, file) = create_log_segment(
            &self.directory,
            &self.prefix,
            &self.current_date,
            next_segment,
        )?;
        self.file = file;
        self.current_segment = segment;
        self.current_path = path;
        self.current_size = 0;
        Ok(())
    }

    fn prune_old_files(&self) -> io::Result<()> {
        let mut files = list_log_files(&self.directory, &self.prefix)?;
        files.sort_by(|left, right| {
            right
                .date
                .cmp(&left.date)
                .then_with(|| right.segment.cmp(&left.segment))
        });

        // Reserve enough room for the current file to grow to its segment
        // limit. This keeps the category total bounded between rotations.
        let old_files_budget = self.max_total_bytes.saturating_sub(self.max_file_bytes);
        let mut old_files_size: u64 = 0;
        let mut retained_dates = HashSet::from([self.current_date.clone()]);
        let max_dates = self.retention_days.max(1);

        for log_file in files {
            if log_file.path == self.current_path {
                continue;
            }

            let is_new_date = !retained_dates.contains(&log_file.date);
            let fits_retention = !is_new_date || retained_dates.len() < max_dates;
            let fits_size = old_files_size.saturating_add(log_file.size) <= old_files_budget;

            if fits_retention && fits_size {
                old_files_size = old_files_size.saturating_add(log_file.size);
                retained_dates.insert(log_file.date);
            } else {
                fs::remove_file(log_file.path)?;
            }
        }
        Ok(())
    }

    fn suspend_file_logging(&mut self, error: &io::Error) {
        let now = Instant::now();
        let should_report = self.suspended_until.is_none_or(|retry_at| now >= retry_at);
        if should_report {
            eprintln!(
                "Suspending {} log file after an I/O error: {error}",
                self.prefix
            );
        }
        self.suspended_until = Some(now + LOG_RECOVERY_INTERVAL);
    }

    fn recover_file_logging(&mut self) {
        let Some(retry_at) = self.suspended_until else {
            return;
        };
        if Instant::now() < retry_at {
            return;
        }

        let result = (|| {
            let file = open_existing_log_file(&self.current_path)?;
            let current_size = file.metadata()?.len();
            self.file = file;
            self.current_size = current_size;
            self.prune_old_files()
        })();

        match result {
            Ok(()) => {
                self.suspended_until = None;
                eprintln!("Resumed {} log file output", self.prefix);
            }
            Err(error) => self.suspend_file_logging(&error),
        }
    }

    fn file_logging_suspended(&mut self) -> bool {
        if self.suspended_until.is_some() {
            self.recover_file_logging();
        }
        self.suspended_until.is_some()
    }

    fn write_fallback(&self, buffer: &[u8]) -> io::Result<usize> {
        let Some(fallback) = &self.fallback else {
            return Ok(buffer.len());
        };

        let mut writer = fallback
            .lock()
            .map_err(|_| io::Error::other("log fallback writer mutex poisoned"))?;
        writer.write_all(buffer)?;
        Ok(buffer.len())
    }

    fn flush_fallback(&self) -> io::Result<()> {
        let Some(fallback) = &self.fallback else {
            return Ok(());
        };

        fallback
            .lock()
            .map_err(|_| io::Error::other("log fallback writer mutex poisoned"))?
            .flush()
    }

    fn write_oversized_event(&mut self, event_size: usize) -> io::Result<usize> {
        let marker_len = usize::try_from(self.max_file_bytes)
            .unwrap_or(usize::MAX)
            .min(OVERSIZED_EVENT_MARKER.len());
        if let Err(error) = self.prepare_for_event(marker_len as u64) {
            self.suspend_file_logging(&error);
            return self
                .write_fallback(&OVERSIZED_EVENT_MARKER[..marker_len])
                .map(|_| event_size);
        }
        if self.file_logging_suspended() {
            return self
                .write_fallback(&OVERSIZED_EVENT_MARKER[..marker_len])
                .map(|_| event_size);
        }

        if let Err(error) = self.file.write_all(&OVERSIZED_EVENT_MARKER[..marker_len]) {
            self.suspend_file_logging(&error);
            return self
                .write_fallback(&OVERSIZED_EVENT_MARKER[..marker_len])
                .map(|_| event_size);
        }
        self.current_size += marker_len as u64;
        Ok(event_size)
    }
}

impl Write for RollingFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.file_logging_suspended() {
            return self.write_fallback(buffer);
        }
        if buffer.is_empty() {
            return Ok(buffer.len());
        }

        if buffer.len() as u64 > self.max_file_bytes {
            return self.write_oversized_event(buffer.len());
        }

        if let Err(error) = self.prepare_for_event(buffer.len() as u64) {
            self.suspend_file_logging(&error);
            return self.write_fallback(buffer);
        }
        if self.file_logging_suspended() {
            return self.write_fallback(buffer);
        }

        if let Err(error) = self.file.write_all(buffer) {
            self.suspend_file_logging(&error);
            return self.write_fallback(buffer);
        }

        self.current_size += buffer.len() as u64;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.suspended_until.is_some() {
            return self.flush_fallback();
        }
        if let Err(error) = self.file.flush() {
            self.suspend_file_logging(&error);
            return self.flush_fallback();
        }
        Ok(())
    }
}

struct LogFile {
    date: String,
    segment: u32,
    path: PathBuf,
    size: u64,
}

fn open_current_log_file(
    directory: &Path,
    prefix: &str,
    date: &str,
    max_file_bytes: u64,
) -> io::Result<(u32, PathBuf, File, u64)> {
    let latest = list_log_files(directory, prefix)?
        .into_iter()
        .filter(|log_file| log_file.date == date)
        .max_by_key(|log_file| log_file.segment);

    match latest {
        Some(log_file) if log_file.size < max_file_bytes => {
            let file = open_existing_log_file(&log_file.path)?;
            let size = file.metadata()?.len();
            Ok((log_file.segment, log_file.path, file, size))
        }
        Some(log_file) => {
            let (segment, path, file) = create_log_segment(
                directory,
                prefix,
                date,
                log_file
                    .segment
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("log segment number exhausted"))?,
            )?;
            Ok((segment, path, file, 0))
        }
        None => {
            let (segment, path, file) = create_log_segment(directory, prefix, date, 0)?;
            Ok((segment, path, file, 0))
        }
    }
}

fn list_log_files(directory: &Path, prefix: &str) -> io::Result<Vec<LogFile>> {
    let marker = format!("{}.", prefix);
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(stem) = name
            .strip_prefix(&marker)
            .and_then(|name| name.strip_suffix(".log"))
        else {
            continue;
        };
        let (date, segment) = match stem.split_once('.') {
            Some((date, segment)) => {
                let Ok(segment) = segment.parse() else {
                    continue;
                };
                (date, segment)
            }
            None => (stem, 0),
        };
        if !is_date_key(date) {
            continue;
        }
        let size = entry.metadata()?.len();
        files.push(LogFile {
            date: date.to_owned(),
            segment,
            path,
            size,
        });
    }
    Ok(files)
}

fn open_existing_log_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().append(true).open(path)
}

fn create_log_segment(
    directory: &Path,
    prefix: &str,
    date: &str,
    mut segment: u32,
) -> io::Result<(u32, PathBuf, File)> {
    loop {
        let path = log_file_path(directory, prefix, date, segment);
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => return Ok((segment, path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                segment = segment
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("log segment number exhausted"))?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn log_file_path(directory: &Path, prefix: &str, date: &str, segment: u32) -> PathBuf {
    if segment == 0 {
        directory.join(format!("{prefix}.{date}.log"))
    } else {
        directory.join(format!("{prefix}.{date}.{segment}.log"))
    }
}

fn is_date_key(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if matches!(index, 4 | 7) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        })
}

fn utc_date(now: SystemTime) -> String {
    let days_since_epoch =
        now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64 / 86_400;
    let (year, month, day) = civil_date_from_days(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02}")
}

fn civil_date_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted_days = days_since_epoch + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}
