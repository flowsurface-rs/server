use std::time::{Duration, Instant};

use flowsurface_exchange::adapter::{Event, Exchange, StreamKind};
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedState {
    Starting,
    Connected,
    Reconnecting,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsStatus {
    Starting,
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct AccessDiagnostics {
    pub auth_failures: u64,
    pub admission_blocked: u64,
    pub rate_limited: u64,
    pub connection_rejected: u64,
    pub accept_timeouts: u64,
}

impl AccessDiagnostics {
    pub fn is_zero(self) -> bool {
        self.auth_failures == 0
            && self.admission_blocked == 0
            && self.rate_limited == 0
            && self.connection_rejected == 0
            && self.accept_timeouts == 0
    }

    pub fn delta_since(self, previous: Self) -> Self {
        Self {
            auth_failures: self.auth_failures.saturating_sub(previous.auth_failures),
            admission_blocked: self
                .admission_blocked
                .saturating_sub(previous.admission_blocked),
            rate_limited: self.rate_limited.saturating_sub(previous.rate_limited),
            connection_rejected: self
                .connection_rejected
                .saturating_sub(previous.connection_rejected),
            accept_timeouts: self
                .accept_timeouts
                .saturating_sub(previous.accept_timeouts),
        }
    }

    fn increment(&mut self, event: AccessEvent) {
        let counter = self.counter_mut(event);
        *counter = counter.saturating_add(1);
    }

    fn counter_mut(&mut self, event: AccessEvent) -> &mut u64 {
        match event {
            AccessEvent::AuthFailure => &mut self.auth_failures,
            AccessEvent::AdmissionBlocked => &mut self.admission_blocked,
            AccessEvent::RateLimited => &mut self.rate_limited,
            AccessEvent::ConnectionRejected => &mut self.connection_rejected,
            AccessEvent::AcceptTimeout => &mut self.accept_timeouts,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AccessEvent {
    AuthFailure,
    AdmissionBlocked,
    RateLimited,
    ConnectionRejected,
    AcceptTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLogDecision {
    Emit,
    Suppress,
}

const ACCESS_DETAIL_LIMIT: u64 = 5;
const ACCESS_DETAIL_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize)]
pub struct FeedDiagnostics {
    pub exchange: String,
    pub state: FeedState,
    pub connected_at: Option<i64>,
    pub last_data_at: Option<i64>,
    pub last_error: Option<String>,
    pub disconnect_count: u64,
    pub failed_connect_attempts: u64,
    pub reconnect_count: u64,
    pub stream_restart_count: u64,
    pub last_persist_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineDiagnostics {
    pub persistence_healthy: bool,
    pub last_flush_at: Option<i64>,
    pub last_flush_error: Option<String>,
    pub flush_failure_count: u64,
    pub flush_retry_pending_trades: u64,
    pub flush_dropped_trades: u64,
    pub event_channel_saturated: bool,
    pub event_channel_dropped_trades: u64,
    pub persist_channel_saturated: bool,
    pub persist_channel_dropped_trades: u64,
    pub data_buffer_saturated: bool,
    pub data_buffer_dropped_trades: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticsSnapshot {
    pub status: DiagnosticsStatus,
    pub uptime_secs: u64,
    pub database: DatabaseDiagnostics,
    pub feeds: Vec<FeedDiagnostics>,
    pub pipeline: PipelineDiagnostics,
    pub access: AccessDiagnostics,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct DatabaseDiagnostics {
    pub ok: bool,
}

#[derive(Debug, Clone)]
struct FeedRecord {
    state: FeedState,
    connected_at: Option<i64>,
    last_data_at: Option<i64>,
    last_error: Option<ErrorSummary>,
    disconnect_count: u64,
    failed_connect_attempts: u64,
    reconnect_count: u64,
    stream_restart_count: u64,
    last_persist_at: Option<i64>,
}

impl FeedRecord {
    fn new() -> Self {
        Self {
            state: FeedState::Starting,
            connected_at: None,
            last_data_at: None,
            last_error: None,
            disconnect_count: 0,
            failed_connect_attempts: 0,
            reconnect_count: 0,
            stream_restart_count: 0,
            last_persist_at: None,
        }
    }

    fn snapshot(&self, exchange: Exchange) -> FeedDiagnostics {
        FeedDiagnostics {
            exchange: exchange.to_string(),
            state: self.state,
            connected_at: self.connected_at,
            last_data_at: self.last_data_at,
            last_error: self.last_error.as_ref().map(|error| error.0.clone()),
            disconnect_count: self.disconnect_count,
            failed_connect_attempts: self.failed_connect_attempts,
            reconnect_count: self.reconnect_count,
            stream_restart_count: self.stream_restart_count,
            last_persist_at: self.last_persist_at,
        }
    }
}

#[derive(Debug, Clone)]
struct ErrorSummary(String);

#[derive(Debug, Default)]
struct RuntimeState {
    feeds: FxHashMap<Exchange, FeedRecord>,
    pipeline: PipelineRecord,
    access: AccessDiagnostics,
    access_sampling: AccessSampling,
}

#[derive(Debug)]
struct AccessSampling {
    window_started: Instant,
    emitted: AccessDiagnostics,
}

impl Default for AccessSampling {
    fn default() -> Self {
        Self {
            window_started: Instant::now(),
            emitted: AccessDiagnostics::default(),
        }
    }
}

#[derive(Debug)]
struct PipelineRecord {
    persistence_healthy: bool,
    last_flush_at: Option<i64>,
    last_flush_error: Option<ErrorSummary>,
    flush_failure_count: u64,
    flush_retry_pending_trades: u64,
    flush_dropped_trades: u64,
    event_channel_saturated_feeds: FxHashSet<Exchange>,
    event_channel_dropped_trades: u64,
    persist_channel_saturated: bool,
    persist_channel_dropped_trades: u64,
    data_buffer_saturated: bool,
    data_buffer_dropped_trades: u64,
}

impl Default for PipelineRecord {
    fn default() -> Self {
        Self {
            persistence_healthy: true,
            last_flush_at: None,
            last_flush_error: None,
            flush_failure_count: 0,
            flush_retry_pending_trades: 0,
            flush_dropped_trades: 0,
            event_channel_saturated_feeds: FxHashSet::default(),
            event_channel_dropped_trades: 0,
            persist_channel_saturated: false,
            persist_channel_dropped_trades: 0,
            data_buffer_saturated: false,
            data_buffer_dropped_trades: 0,
        }
    }
}

pub struct Diagnostics {
    started_at: Instant,
    start_wall_ms: i64,
    state: Mutex<RuntimeState>,
}

impl Diagnostics {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            start_wall_ms: unix_now_ms(),
            state: Mutex::new(RuntimeState::default()),
        }
    }

    pub fn register_exchange(&self, exchange: Exchange) {
        let mut state = self.state.lock();
        state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn record_stream_event(&self, event: &Event) {
        match event {
            Event::Connected(streams) => {
                for exchange in stream_exchanges(streams) {
                    self.record_connected(exchange);
                }
            }
            Event::Disconnected(streams, reason) => {
                for exchange in stream_exchanges(streams) {
                    self.record_disconnected(exchange, reason);
                }
            }
            Event::DepthReceived(stream_kind, _, _) => {
                self.record_data_event(stream_kind.ticker_info().exchange())
            }
            Event::TradesReceived(stream_kind, _, _) => {
                self.record_data_event(stream_kind.ticker_info().exchange())
            }
            Event::KlineReceived(stream_kind, _) => {
                self.record_data_event(stream_kind.ticker_info().exchange())
            }
        }
    }

    pub fn record_event_channel_drop(&self, exchange: Exchange, event: &Event) {
        let mut state = self.state.lock();
        state
            .pipeline
            .event_channel_saturated_feeds
            .insert(exchange);
        state.pipeline.event_channel_dropped_trades += event_trade_count(event);
    }

    pub fn record_event_channel_recovered(&self, exchange: Exchange) {
        self.state
            .lock()
            .pipeline
            .event_channel_saturated_feeds
            .remove(&exchange);
    }

    pub fn record_stream_task_stopped(&self, exchange: Exchange, reason: &str) {
        let mut state = self.state.lock();
        let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
        feed.state = FeedState::Stopped;
        feed.last_error = Some(ErrorSummary(summarize_error(reason)));
    }

    pub fn record_stream_task_restarting(&self, exchange: Exchange, reason: &str) {
        let mut state = self.state.lock();
        let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
        feed.state = FeedState::Reconnecting;
        feed.last_error = Some(ErrorSummary(summarize_error(reason)));
        feed.stream_restart_count = feed.stream_restart_count.saturating_add(1);
    }

    pub fn record_persist_channel_drop(&self, event: &Event) {
        let mut state = self.state.lock();
        state.pipeline.persist_channel_saturated = true;
        state.pipeline.persist_channel_dropped_trades += event_trade_count(event);
    }

    pub fn record_persist_channel_recovered(&self) {
        self.state.lock().pipeline.persist_channel_saturated = false;
    }

    pub fn record_buffer_trade_drop(&self) {
        let mut state = self.state.lock();
        state.pipeline.data_buffer_saturated = true;
        state.pipeline.data_buffer_dropped_trades += 1;
    }

    pub fn record_buffer_recovered(&self) {
        self.state.lock().pipeline.data_buffer_saturated = false;
    }

    pub fn record_flush_succeeded(&self, persisted: &[Exchange]) {
        self.record_flush_completed(persisted, false);
    }

    pub fn record_flush_retry_succeeded(&self, persisted: &[Exchange]) {
        self.record_flush_completed(persisted, true);
    }

    fn record_flush_completed(&self, persisted: &[Exchange], retry_committed: bool) {
        let now = self.now_ms();
        let mut state = self.state.lock();
        state.pipeline.last_flush_at = Some(now);

        let retry_pending = state.pipeline.flush_retry_pending_trades > 0;
        if retry_committed || !retry_pending {
            state.pipeline.flush_retry_pending_trades = 0;
            if retry_committed || state.pipeline.flush_dropped_trades == 0 {
                state.pipeline.last_flush_error = None;
            }
            if state.pipeline.flush_dropped_trades == 0 {
                state.pipeline.persistence_healthy = true;
            }
        }

        for &exchange in persisted {
            let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
            feed.last_persist_at = Some(now);
        }
    }

    pub fn record_flush_failed(&self, error: &str, retry_pending_trades: usize) {
        let mut state = self.state.lock();
        state.pipeline.persistence_healthy = false;
        state.pipeline.last_flush_error = Some(ErrorSummary(summarize_error(error)));
        state.pipeline.flush_failure_count = state.pipeline.flush_failure_count.saturating_add(1);
        state.pipeline.flush_retry_pending_trades = retry_pending_trades as u64;
    }

    pub fn record_flush_dropped(&self, trades: usize) {
        let mut state = self.state.lock();
        state.pipeline.persistence_healthy = false;
        state.pipeline.flush_retry_pending_trades = 0;
        state.pipeline.flush_dropped_trades = state
            .pipeline
            .flush_dropped_trades
            .saturating_add(trades as u64);
    }

    pub fn record_access(&self, event: AccessEvent) -> AccessLogDecision {
        self.record_access_at(event, Instant::now())
    }

    fn record_access_at(&self, event: AccessEvent, now: Instant) -> AccessLogDecision {
        let mut state = self.state.lock();
        if now.saturating_duration_since(state.access_sampling.window_started)
            >= ACCESS_DETAIL_WINDOW
        {
            state.access_sampling.window_started = now;
            state.access_sampling.emitted = AccessDiagnostics::default();
        }

        state.access.increment(event);
        let emitted = state.access_sampling.emitted.counter_mut(event);
        if *emitted < ACCESS_DETAIL_LIMIT {
            *emitted = emitted.saturating_add(1);
            AccessLogDecision::Emit
        } else {
            AccessLogDecision::Suppress
        }
    }

    pub fn access_counts(&self) -> AccessDiagnostics {
        self.state.lock().access
    }

    pub fn snapshot(&self, database_ok: bool) -> DiagnosticsSnapshot {
        let state = self.state.lock();
        let mut feeds: Vec<_> = state
            .feeds
            .iter()
            .map(|(exchange, feed)| feed.snapshot(*exchange))
            .collect();
        feeds.sort_by(|left, right| left.exchange.cmp(&right.exchange));

        DiagnosticsSnapshot {
            status: overall_status(database_ok, &state),
            uptime_secs: self.uptime().as_secs(),
            database: DatabaseDiagnostics { ok: database_ok },
            feeds,
            pipeline: state.pipeline.snapshot(),
            access: state.access,
        }
    }

    fn record_connected(&self, exchange: Exchange) {
        let now = self.now_ms();
        let mut state = self.state.lock();
        let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
        if feed.state == FeedState::Reconnecting {
            feed.reconnect_count += 1;
        }
        feed.state = FeedState::Connected;
        feed.connected_at = Some(now);
        feed.last_error = None;
    }

    fn record_disconnected(&self, exchange: Exchange, reason: &str) {
        let mut state = self.state.lock();
        let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
        if feed.state == FeedState::Connected {
            feed.disconnect_count += 1;
        } else {
            feed.failed_connect_attempts += 1;
        }
        feed.state = FeedState::Reconnecting;
        feed.last_error = Some(ErrorSummary(summarize_error(reason)));
    }

    fn record_data_event(&self, exchange: Exchange) {
        let mut state = self.state.lock();
        let feed = state.feeds.entry(exchange).or_insert_with(FeedRecord::new);
        feed.last_data_at = Some(self.now_ms());
    }

    fn now_ms(&self) -> i64 {
        self.start_wall_ms + self.started_at.elapsed().as_millis() as i64
    }
}

impl PipelineRecord {
    fn snapshot(&self) -> PipelineDiagnostics {
        PipelineDiagnostics {
            persistence_healthy: self.persistence_healthy,
            last_flush_at: self.last_flush_at,
            last_flush_error: self.last_flush_error.as_ref().map(|error| error.0.clone()),
            flush_failure_count: self.flush_failure_count,
            flush_retry_pending_trades: self.flush_retry_pending_trades,
            flush_dropped_trades: self.flush_dropped_trades,
            event_channel_saturated: !self.event_channel_saturated_feeds.is_empty(),
            event_channel_dropped_trades: self.event_channel_dropped_trades,
            persist_channel_saturated: self.persist_channel_saturated,
            persist_channel_dropped_trades: self.persist_channel_dropped_trades,
            data_buffer_saturated: self.data_buffer_saturated,
            data_buffer_dropped_trades: self.data_buffer_dropped_trades,
        }
    }
}

fn overall_status(database_ok: bool, state: &RuntimeState) -> DiagnosticsStatus {
    if !database_ok
        || !state.pipeline.persistence_healthy
        || state.pipeline.flush_retry_pending_trades > 0
        || state.pipeline.flush_dropped_trades > 0
        || !state.pipeline.event_channel_saturated_feeds.is_empty()
        || state.pipeline.persist_channel_saturated
        || state.pipeline.data_buffer_saturated
        || state
            .feeds
            .values()
            .any(|feed| matches!(feed.state, FeedState::Reconnecting | FeedState::Stopped))
    {
        return DiagnosticsStatus::Degraded;
    }

    if state
        .feeds
        .values()
        .any(|feed| feed.state == FeedState::Starting)
    {
        DiagnosticsStatus::Starting
    } else {
        DiagnosticsStatus::Healthy
    }
}

fn event_trade_count(event: &Event) -> u64 {
    match event {
        Event::TradesReceived(_, _, trades) => trades.len() as u64,
        _ => 0,
    }
}

fn stream_exchanges(streams: &[StreamKind]) -> FxHashSet<Exchange> {
    streams
        .iter()
        .map(|stream| stream.ticker_info().exchange())
        .collect()
}

fn summarize_error(error: &str) -> String {
    const MAX_ERROR_LENGTH: usize = 512;
    error.chars().take(MAX_ERROR_LENGTH).collect()
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
