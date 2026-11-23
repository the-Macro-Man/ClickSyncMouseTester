use crate::{backend::SessionPrefix, chart::ChartResult, product::Settings};
use clicksync_core::{SessionConfig, SessionSnapshot, SharedByteBudget};
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum PollingExportMode {
    Reports,
    Rates,
    #[default]
    Combined,
    Separate,
}

pub enum PollingDestination {
    Reports(PathBuf),
    Rates(PathBuf),
    Combined(PathBuf),
    Separate { reports: PathBuf, rates: PathBuf },
}

#[derive(Clone, Copy)]
enum PollingRows {
    Reports,
    Rates,
    Both,
}

pub enum PollingAnalysisSource {
    Capture {
        snapshot: Arc<SessionSnapshot>,
        samples: Vec<clicksync_chart::analysis::polling::HistoryPoint>,
        source_complete: bool,
    },
    Import(PathBuf),
    Existing(Arc<crate::product::polling_analysis::Data>),
}

pub enum FileRequest {
    ImportAnalysis {
        path: PathBuf,
        operation: u64,
        budget: Arc<SharedByteBudget>,
        existing: Vec<(Arc<SessionSnapshot>, Option<f64>)>,
    },
    ConvertRange {
        snapshots: Vec<Arc<SessionSnapshot>>,
        range: (f64, f64),
        logical: bool,
        operation: u64,
    },
    Export {
        path: PathBuf,
        snapshot: Arc<SessionSnapshot>,
        cpi: Option<f64>,
    },
    ExportPolling {
        destination: PollingDestination,
        snapshot: Arc<SessionSnapshot>,
        samples: Vec<clicksync_chart::analysis::polling::HistoryPoint>,
        source_complete: bool,
    },
    AnalyzePolling {
        source: PollingAnalysisSource,
        comparison: Option<Arc<crate::product::polling_analysis::Data>>,
        replace_comparison: bool,
        target: u32,
    },
    Query {
        result: Arc<ChartResult>,
        key: u32,
        range: std::ops::Range<usize>,
    },
    Settings(Settings),
}
#[derive(Clone, Serialize)]
pub struct QuerySummary {
    pub request_id: u64,
    pub key: u32,
    pub generation: u64,
    pub end: usize,
    pub range: std::ops::Range<usize>,
    pub record_count: usize,
    pub distribution: Option<clicksync_chart::analysis::statistics::Distribution>,
    pub residual_distribution: Option<clicksync_chart::analysis::statistics::Distribution>,
    pub sum_x: i64,
    pub sum_y: i64,
}
pub enum FileResult {
    ImportedAnalysis {
        group: crate::product::performance_analysis::Group,
        operation: u64,
    },
    Duplicate {
        operation: u64,
    },
    ConvertedRange {
        operation: u64,
        range: Option<(f64, f64)>,
    },
    Saved(PathBuf),
    SavedPollingPair {
        reports: PathBuf,
        rates: PathBuf,
    },
    PollingAnalysis {
        primary: Arc<crate::product::polling_analysis::Data>,
        primary_summary: crate::product::polling_analysis::Summary,
        comparison: Option<(
            Arc<crate::product::polling_analysis::Data>,
            crate::product::polling_analysis::Summary,
        )>,
        target: u32,
    },
    Query(QuerySummary),
}

pub struct FileWorker {
    sender: Option<mpsc::SyncSender<FileRequest>>,
    receiver: mpsc::Receiver<Result<FileResult, String>>,
    thread: Option<JoinHandle<()>>,
}
impl FileWorker {
    pub fn spawn() -> Self {
        let (sender, requests) = mpsc::sync_channel(1);
        let (results, receiver) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            while let Ok(request) = requests.recv() {
                let result = execute(request);
                if results.send(result).is_err() {
                    break;
                }
            }
        });
        Self {
            sender: Some(sender),
            receiver,
            thread: Some(thread),
        }
    }
    pub fn submit(&self, request: FileRequest) -> Result<(), String> {
        self.sender
            .as_ref()
            .ok_or("file worker closed")?
            .try_send(request)
            .map_err(|e| e.to_string())
    }
    pub fn take(&self) -> Option<Result<FileResult, String>> {
        self.receiver.try_recv().ok()
    }
    /// Finish fixed-input operations and the latest unsaved preference value.
    pub fn finish(&mut self, settings: Option<Settings>) -> Result<(), String> {
        let mut failure = None;
        if let Some(settings) = settings
            && let Some(sender) = &self.sender
        {
            let mut request = FileRequest::Settings(settings);
            loop {
                match sender.try_send(request) {
                    Ok(()) => break,
                    Err(mpsc::TrySendError::Full(pending)) => {
                        request = pending;
                        if let Ok(Err(error)) =
                            self.receiver.recv_timeout(Duration::from_millis(10))
                        {
                            failure.get_or_insert(error);
                        }
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        failure
                            .get_or_insert("file worker disconnected before settings save".into());
                        break;
                    }
                }
            }
        }
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            while !thread.is_finished() {
                if let Ok(Err(error)) = self.receiver.recv_timeout(Duration::from_millis(10)) {
                    failure.get_or_insert(error);
                }
            }
            while let Ok(result) = self.receiver.try_recv() {
                if let Err(error) = result {
                    failure.get_or_insert(error);
                }
            }
            if thread.join().is_err() {
                failure.get_or_insert("file worker panicked".into());
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for FileWorker {
    fn drop(&mut self) {
        let _ = self.finish(None);
    }
}
pub fn execute(request: FileRequest) -> Result<FileResult, String> {
    match request {
        FileRequest::ConvertRange {
            snapshots,
            range,
            logical,
            operation,
        } => Ok(FileResult::ConvertedRange {
            operation,
            range: crate::product::performance_analysis::convert_group_range(
                &snapshots.iter().map(Arc::as_ref).collect::<Vec<_>>(),
                range,
                logical,
            ),
        }),
        FileRequest::ImportAnalysis {
            path,
            operation,
            budget,
            existing,
        } => {
            let imported = clicksync_core::exchange::import_reader(
                BufReader::new(File::open(path).map_err(|error| error.to_string())?),
                SessionConfig::default(),
                budget,
            )
            .map_err(|error| error.to_string())?;
            let snapshot = Arc::new(imported.snapshot);
            let cpi = imported.effective_cpi;
            if existing.iter().any(|(previous, previous_cpi)| {
                crate::product::performance_analysis::same_group(
                    &snapshot,
                    cpi,
                    previous,
                    *previous_cpi,
                )
            }) {
                return Ok(FileResult::Duplicate { operation });
            }
            let identity = crate::product::performance_analysis::group_identity(&snapshot, cpi);
            Ok(FileResult::ImportedAnalysis {
                group: crate::product::performance_analysis::Group {
                    key: 0,
                    snapshot,
                    cpi,
                    identity,
                },
                operation,
            })
        }
        FileRequest::Export {
            path,
            snapshot,
            cpi,
        } => {
            atomic_file(&path, |file| {
                let mut writer = BufWriter::with_capacity(65536, file);
                clicksync_core::exchange::export_writer(&mut writer, &snapshot, cpi, &utc_now())
                    .map_err(|e| e.to_string())?;
                writer.flush().map_err(|e| e.to_string())
            })?;
            Ok(FileResult::Saved(path))
        }
        FileRequest::ExportPolling {
            destination,
            snapshot,
            samples,
            source_complete,
        } => {
            let save = |path: &Path, rows| {
                atomic_file(path, |file| {
                    let mut writer = BufWriter::with_capacity(65536, file);
                    write_polling_csv(&mut writer, &snapshot, &samples, source_complete, rows)?;
                    writer.flush().map_err(|error| error.to_string())
                })
            };
            match destination {
                PollingDestination::Reports(path) => {
                    save(&path, PollingRows::Reports)?;
                    Ok(FileResult::Saved(path))
                }
                PollingDestination::Rates(path) => {
                    save(&path, PollingRows::Rates)?;
                    Ok(FileResult::Saved(path))
                }
                PollingDestination::Combined(path) => {
                    save(&path, PollingRows::Both)?;
                    Ok(FileResult::Saved(path))
                }
                PollingDestination::Separate { reports, rates } => {
                    if same_destination(&reports, &rates)? {
                        return Err("Choose different files for reports and rates".into());
                    }
                    save(&reports, PollingRows::Reports)?;
                    save(&rates, PollingRows::Rates).map_err(|error| {
                        format!(
                            "Reports saved to {}; failed to save rates to {}: {error}",
                            reports.display(),
                            rates.display()
                        )
                    })?;
                    Ok(FileResult::SavedPollingPair { reports, rates })
                }
            }
        }
        FileRequest::AnalyzePolling {
            source,
            comparison,
            replace_comparison,
            target,
        } => {
            let loaded = match source {
                PollingAnalysisSource::Capture {
                    snapshot,
                    samples,
                    source_complete,
                } => Arc::new(crate::product::polling_analysis::Data::from_snapshot(
                    &snapshot,
                    &samples,
                    source_complete,
                )?),
                PollingAnalysisSource::Import(path) => {
                    Arc::new(crate::product::polling_analysis::import(&path)?)
                }
                PollingAnalysisSource::Existing(data) => data,
            };
            let (primary, comparison) = if replace_comparison {
                (
                    comparison.ok_or("Load a first test before comparing")?,
                    Some(loaded),
                )
            } else {
                (loaded, comparison)
            };
            let primary_summary = primary.summary(target)?;
            let comparison = comparison
                .map(|data| data.summary(target).map(|summary| (data, summary)))
                .transpose()?;
            Ok(FileResult::PollingAnalysis {
                primary,
                primary_summary,
                comparison,
                target,
            })
        }
        FileRequest::Settings(mut settings) => {
            settings.validate();
            let path = Settings::path();
            std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
            atomic_file(&path, |file| {
                serde_json::to_writer(file, &settings).map_err(|e| e.to_string())
            })?;
            Ok(FileResult::Saved(path))
        }

        FileRequest::Query { result, key, range } => {
            let _reservation = result
                .byte_budget
                .as_ref()
                .map(|budget| {
                    let count = result
                        .snapshot(key)?
                        .event_count()
                        .max(4096)
                        .checked_next_power_of_two()
                        .ok_or("query capacity overflow")?;
                    let unit = std::mem::size_of::<clicksync_core::SessionEvent>() * 2
                        + std::mem::size_of::<clicksync_chart::Sample>() * 12
                        + 512;
                    let bytes = count.checked_mul(unit).ok_or("query capacity overflow")?;
                    budget.try_reserve(bytes).map_err(|error| error.to_string())
                })
                .transpose()?;
            let mut query = result.query(key, range.clone())?;
            let parameters = result.request.parameters_for(key);
            let selected_range = result
                .request
                .analysis
                .as_ref()
                .and_then(|analysis| analysis.range);
            crate::product::performance_analysis::filter_query(
                &mut query,
                selected_range,
                parameters.time_basis,
                result.snapshot(key)?.timestamp_frequency(),
            );
            let (distribution, residual_distribution) =
                if let Some(mut secondary) = result.query_secondary(key, range.clone())? {
                    crate::product::performance_analysis::filter_query(
                        &mut secondary,
                        selected_range,
                        parameters.time_basis,
                        result.snapshot(key)?.timestamp_frequency(),
                    );
                    let mut secondary_parameters = parameters.clone();
                    secondary_parameters.metric = result.request.secondary_metric.unwrap();
                    (
                        clicksync_chart::analysis::statistics::summarize_pair(
                            &query,
                            &secondary,
                            &parameters,
                        ),
                        clicksync_chart::analysis::statistics::residual_distribution_pair(
                            &query,
                            &secondary,
                            &parameters,
                            &secondary_parameters,
                        ),
                    )
                } else {
                    (
                        clicksync_chart::analysis::statistics::summarize(&query, &parameters),
                        clicksync_chart::analysis::statistics::residual_distribution(
                            &query,
                            &parameters,
                        ),
                    )
                };
            Ok(FileResult::Query(QuerySummary {
                request_id: result.request.id,
                key,
                generation: query.data_version.generation,
                end: query.data_version.end,
                range,
                record_count: query.records.len(),
                distribution,
                residual_distribution,
                sum_x: query.records.iter().map(|e| e.delta_x as i64).sum(),
                sum_y: query.records.iter().map(|e| e.delta_y as i64).sum(),
            }))
        }
    }
}

fn write_polling_csv(
    writer: &mut impl Write,
    snapshot: &SessionSnapshot,
    samples: &[clicksync_chart::analysis::polling::HistoryPoint],
    source_complete: bool,
    rows: PollingRows,
) -> Result<(), String> {
    let frequency = snapshot.timestamp_frequency();
    let origin = snapshot
        .segments()
        .first()
        .map_or(0, |segment| segment.started_at_raw_capture_ticks);
    let origin_ms = origin as f64 * 1000.0 / frequency as f64;
    let row = |writer: &mut dyn Write, fields: &[String]| -> Result<(), String> {
        for (index, field) in fields.iter().enumerate() {
            if index != 0 {
                write!(writer, ",").map_err(|error| error.to_string())?;
            }
            if field.contains([',', '"', '\r', '\n']) {
                write!(writer, "\"{}\"", field.replace('"', "\"\""))
                    .map_err(|error| error.to_string())?;
            } else {
                write!(writer, "{field}").map_err(|error| error.to_string())?;
            }
        }
        writeln!(writer).map_err(|error| error.to_string())
    };
    writeln!(
        writer,
        "type,elapsed_ms,active_ms,capture_segment,rate_segment,raw_ticks,tick_hz,rate_hz,rounded_hz,dx,dy,packet_kind,button_flags,button_data,mouse_flags,extra_information,capture_sequence,timing_sequence,integrity,device_name,movement_mode,ignored_after_limit,overflow,disconnected,source_complete"
    )
    .map_err(|error| error.to_string())?;
    let mut fields: [String; 25] = Default::default();
    fields[0] = "metadata".into();
    fields[6] = frequency.to_string();
    fields[18] = format!("{:?}", snapshot.integrity());
    fields[19] = snapshot.metadata().display_name.clone();
    let evidence = snapshot.capture_evidence();
    fields[21] = evidence.ignored_after_limit.to_string();
    fields[22] = evidence.overflow.to_string();
    fields[23] = evidence.disconnected.to_string();
    fields[24] = source_complete.to_string();
    row(writer, &fields)?;
    if matches!(rows, PollingRows::Reports | PollingRows::Both) {
        for event in snapshot.events() {
            let mut fields: [String; 25] = Default::default();
            fields[0] = "report".into();
            fields[1] = format!(
                "{:.6}",
                event.raw_capture_ticks as f64 * 1000.0 / frequency as f64 - origin_ms
            );
            fields[2] = format!(
                "{:.6}",
                event.logical_ticks as f64 * 1000.0 / frequency as f64
            );
            fields[3] = event.session_segment_id.to_string();
            fields[5] = event.raw_capture_ticks.to_string();
            fields[9] = event.raw_delta_x.to_string();
            fields[10] = event.raw_delta_y.to_string();
            fields[11] = format!("{:?}", event.packet_kind);
            fields[12] = event.button_flags.to_string();
            fields[13] = event.button_data.to_string();
            fields[14] = event.raw_mouse_flags.to_string();
            fields[15] = event.extra_information.to_string();
            fields[16] = event.capture_sequence.to_string();
            fields[17] = event.timing_sequence.to_string();
            fields[20] = format!("{:?}", event.movement_mode);
            row(writer, &fields)?;
        }
    }
    if matches!(rows, PollingRows::Rates | PollingRows::Both) {
        for sample in samples {
            let mut fields: [String; 25] = Default::default();
            fields[0] = "rate".into();
            fields[1] = format!("{:.6}", sample.observed_ms - origin_ms);
            fields[2] = format!("{:.6}", sample.logical_ms - origin_ms);
            fields[4] = sample.segment.to_string();
            fields[7] = format!("{:.6}", sample.rate);
            fields[8] = (sample.rate.round_ties_even() as i32).to_string();
            row(writer, &fields)?;
        }
    }
    Ok(())
}

fn same_destination(first: &Path, second: &Path) -> Result<bool, String> {
    let normalize = |path: &Path| -> Result<PathBuf, String> {
        let parent = path.parent().ok_or("Invalid export destination")?;
        let name = path.file_name().ok_or("Invalid export filename")?;
        Ok(std::fs::canonicalize(parent)
            .map_err(|error| format!("Cannot access export directory: {error}"))?
            .join(name))
    };
    let first = normalize(first)?;
    let second = normalize(second)?;
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows::Win32::Globalization::{CSTR_EQUAL, CompareStringOrdinal};
        let first: Vec<_> = first.as_os_str().encode_wide().collect();
        let second: Vec<_> = second.as_os_str().encode_wide().collect();
        let result = unsafe { CompareStringOrdinal(&first, &second, true) };
        if result.0 == 0 {
            return Err("Cannot compare export filenames".into());
        }
        Ok(result == CSTR_EQUAL)
    }
    #[cfg(not(windows))]
    {
        Ok(first == second)
    }
}

pub fn imported_prefix(
    snapshot: Arc<SessionSnapshot>,
    key: u32,
    identity: u64,
) -> Arc<SessionPrefix> {
    Arc::new(SessionPrefix {
        generation: identity,
        received_sequence: snapshot.event_count() as u64,
        received_records: snapshot.event_count() as u64,
        phase: crate::backend::CapturePhase::Stopped,
        complete: snapshot.integrity() == clicksync_core::session::CaptureIntegrity::Complete,
        datasets: vec![(key, snapshot)],
    })
}
pub fn atomic_file(
    path: &Path,
    write: impl FnOnce(&mut File) -> Result<(), String>,
) -> Result<(), String> {
    let parent = path.parent().ok_or("invalid destination")?;
    let temporary = parent.join(format!(
        ".clicksync-{}-{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| e.to_string())?;
        write(&mut file)?;
        file.sync_all().map_err(|e| e.to_string())?;
        drop(file);
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows::Win32::Storage::FileSystem::{
                MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
            };
            let from: Vec<u16> = temporary.as_os_str().encode_wide().chain([0]).collect();
            let to: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
            unsafe {
                MoveFileExW(
                    windows::core::PCWSTR(from.as_ptr()),
                    windows::core::PCWSTR(to.as_ptr()),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            }
            .map_err(|e| e.to_string())?;
        }
        #[cfg(not(windows))]
        std::fs::rename(&temporary, path).map_err(|e| e.to_string())?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
fn utc_now() -> String {
    let time = unsafe { windows::Win32::System::SystemInformation::GetSystemTime() };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        time.wYear,
        time.wMonth,
        time.wDay,
        time.wHour,
        time.wMinute,
        time.wSecond,
        time.wMilliseconds
    )
}

#[cfg(test)]
mod polling_export_tests {
    use super::*;
    use clicksync_chart::analysis::polling::HistoryPoint;
    use clicksync_core::{RawMouseMovementMode, RawMouseRecord, Session, SessionMetadata};

    fn example() -> (Arc<SessionSnapshot>, Vec<HistoryPoint>) {
        let mut session = Session::new(
            SessionConfig {
                timestamp_frequency: 1000,
                ..Default::default()
            },
            SharedByteBudget::new(4 * 1024 * 1024),
            SessionMetadata {
                display_name: "Mouse, \"test\"".into(),
                ..Default::default()
            },
        )
        .unwrap();
        session.begin_collecting(1000, true, true).unwrap();
        session
            .append(RawMouseRecord::new(
                2,
                -3,
                1010,
                1,
                1,
                RawMouseMovementMode::Relative,
            ))
            .unwrap();
        session.pause();
        session.begin_collecting(5000, false, true).unwrap();
        session
            .append(RawMouseRecord::new(
                0,
                0,
                5010,
                2,
                2,
                RawMouseMovementMode::Relative,
            ))
            .unwrap();
        session.pause();
        let snapshot = Arc::new(session.snapshot().unwrap());
        let samples = vec![HistoryPoint {
            observed_ms: 5010.0,
            logical_ms: 1020.0,
            rate: 996.6666667,
            segment: 2,
        }];
        (snapshot, samples)
    }

    #[test]
    fn csv_contains_selected_rows_and_escaped_metadata() {
        let (snapshot, samples) = example();
        let mut output = Vec::new();
        write_polling_csv(&mut output, &snapshot, &samples, true, PollingRows::Both).unwrap();
        let csv = String::from_utf8(output).unwrap();
        assert!(csv.contains("\"Mouse, \"\"test\"\"\""));
        let lines: Vec<_> = csv.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(lines[2].starts_with("report,10.000000,10.000000,1,,1010,"));
        assert!(lines[3].starts_with("report,4010.000000,20.000000,2,,5010,"));
        assert!(lines[4].starts_with("rate,4010.000000,20.000000,,2,,,996.666667,997,"));
        for (rows, expected_reports, expected_rates) in [
            (PollingRows::Reports, 2, 0),
            (PollingRows::Rates, 0, 1),
            (PollingRows::Both, 2, 1),
        ] {
            let mut output = Vec::new();
            write_polling_csv(&mut output, &snapshot, &samples, true, rows).unwrap();
            let csv = String::from_utf8(output).unwrap();
            assert_eq!(
                csv.lines()
                    .filter(|line| line.starts_with("metadata,"))
                    .count(),
                1
            );
            assert_eq!(
                csv.lines()
                    .filter(|line| line.starts_with("report,"))
                    .count(),
                expected_reports
            );
            assert_eq!(
                csv.lines().filter(|line| line.starts_with("rate,")).count(),
                expected_rates
            );
        }
    }

    #[test]
    fn polling_analysis_imports_export_modes_and_rejects_bad_csv() {
        let (snapshot, samples) = example();
        for rows in [PollingRows::Rates, PollingRows::Both] {
            let mut output = Vec::new();
            write_polling_csv(&mut output, &snapshot, &samples, true, rows).unwrap();
            let data = crate::product::polling_analysis::import_reader(output.as_slice()).unwrap();
            assert_eq!(data.name, "Mouse, \"test\"");
            assert_eq!(data.rates.len(), 1);
            assert!((data.rates[0].rate - 996.666667).abs() < 0.00001);
            assert_eq!(
                data.quality.reports,
                if matches!(rows, PollingRows::Both) {
                    Some(2)
                } else {
                    None
                }
            );
            assert!(
                data.intervals.is_empty(),
                "intervals must not cross capture segments"
            );
        }
        let mut output = Vec::new();
        write_polling_csv(&mut output, &snapshot, &samples, true, PollingRows::Reports).unwrap();
        assert!(
            crate::product::polling_analysis::import_reader(output.as_slice())
                .err()
                .unwrap()
                .contains("No valid rate")
        );
        assert!(
            crate::product::polling_analysis::import_reader("type,elapsed_ms\nrate,1\n".as_bytes())
                .is_err()
        );
        let mut output = Vec::new();
        write_polling_csv(&mut output, &snapshot, &samples, true, PollingRows::Rates).unwrap();
        let bad = String::from_utf8(output)
            .unwrap()
            .replace("996.666667", "NaN");
        assert!(crate::product::polling_analysis::import_reader(bad.as_bytes()).is_err());
    }

    #[test]
    fn polling_comparison_recomputes_both_summaries_at_target() {
        let (snapshot, samples) = example();
        let primary = Arc::new(
            crate::product::polling_analysis::Data::from_snapshot(&snapshot, &samples, true)
                .unwrap(),
        );
        let other = Arc::new(
            crate::product::polling_analysis::Data::from_snapshot(&snapshot, &samples, true)
                .unwrap(),
        );
        let result = execute(FileRequest::AnalyzePolling {
            source: PollingAnalysisSource::Existing(other.clone()),
            comparison: Some(primary.clone()),
            replace_comparison: true,
            target: 500,
        })
        .unwrap();
        let FileResult::PollingAnalysis {
            primary: actual,
            primary_summary,
            comparison,
            target,
        } = result
        else {
            panic!("unexpected result")
        };
        assert!(Arc::ptr_eq(&actual, &primary));
        assert_eq!(target, 500);
        assert_eq!(primary_summary.minimum, samples[0].rate);
        let (actual_other, summary) = comparison.unwrap();
        assert!(Arc::ptr_eq(&actual_other, &other));
        assert_eq!(summary.minimum, samples[0].rate);
    }

    #[test]
    fn separate_exports_use_two_distinct_destinations() {
        let directory = std::env::temp_dir().join(format!(
            "clicksync-export-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let reports = directory.join("reports.csv");
        let rates = directory.join("rates.csv");
        let (snapshot, samples) = example();
        let request = |destination| FileRequest::ExportPolling {
            destination,
            snapshot: snapshot.clone(),
            samples: samples.clone(),
            source_complete: true,
        };
        let result = execute(request(PollingDestination::Separate {
            reports: reports.clone(),
            rates: rates.clone(),
        }));
        assert!(matches!(result, Ok(FileResult::SavedPollingPair { .. })));
        let report_text = std::fs::read_to_string(&reports).unwrap();
        let rate_text = std::fs::read_to_string(&rates).unwrap();
        assert!(report_text.lines().any(|line| line.starts_with("report,")));
        assert!(!report_text.lines().any(|line| line.starts_with("rate,")));
        assert!(rate_text.lines().any(|line| line.starts_with("rate,")));
        assert!(!rate_text.lines().any(|line| line.starts_with("report,")));
        assert!(
            execute(request(PollingDestination::Separate {
                reports: reports.clone(),
                rates: directory.join("REPORTS.CSV"),
            }))
            .is_err()
        );
        assert_eq!(std::fs::read_to_string(&reports).unwrap(), report_text);
        let partial_report = directory.join("partial.csv");
        let invalid_rate = directory.join("rate-directory");
        std::fs::create_dir(&invalid_rate).unwrap();
        let error = match execute(request(PollingDestination::Separate {
            reports: partial_report.clone(),
            rates: invalid_rate.clone(),
        })) {
            Err(error) => error,
            Ok(_) => panic!("saving over a directory must fail"),
        };
        assert!(error.contains("Reports saved to"));
        assert!(error.contains("failed to save rates"));
        assert!(partial_report.is_file());
        std::fs::remove_file(partial_report).unwrap();
        std::fs::remove_dir(invalid_rate).unwrap();
        std::fs::remove_file(reports).unwrap();
        std::fs::remove_file(rates).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn csv_propagates_write_errors() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut session = Session::new(
            SessionConfig::default(),
            SharedByteBudget::new(4 * 1024 * 1024),
            SessionMetadata::default(),
        )
        .unwrap();
        assert!(
            write_polling_csv(
                &mut Broken,
                &session.snapshot().unwrap(),
                &[],
                true,
                PollingRows::Both
            )
            .unwrap_err()
            .contains("disk full")
        );
    }
}
