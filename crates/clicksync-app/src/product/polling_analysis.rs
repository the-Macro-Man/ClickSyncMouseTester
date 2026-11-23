use clicksync_chart::analysis::polling::HistoryPoint;
use clicksync_core::SessionSnapshot;
use std::{fs::File, io::Read, path::Path};

const MAX_SAMPLES: usize = 1_000_000;
const MAX_INTERVAL_POINTS: usize = 50_000;
const MAX_CSV_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_CONTINUOUS_GAP_MS: f64 = 50.0;
const MAX_DISPLAYED_DIPS: usize = 500;

#[derive(Clone, Copy, Debug)]
pub struct RatePoint {
    pub time_ms: f64,
    pub rate: f64,
    pub segment: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct IntervalPoint {
    pub time_ms: f64,
    pub interval_ms: f64,
    pub segment: u32,
}

#[derive(Clone, Default)]
pub struct Quality {
    pub integrity: String,
    pub source_complete: Option<bool>,
    pub overflow: u64,
    pub ignored: u64,
    pub disconnected: bool,
    pub reports: Option<u64>,
    pub zero_motion: u64,
    pub equal_timestamps: u64,
}

#[derive(Clone)]
pub struct Data {
    pub name: String,
    pub rates: Vec<RatePoint>,
    pub overview: Vec<RatePoint>,
    pub intervals: Vec<IntervalPoint>,
    pub interval_stride: usize,
    pub quality: Quality,
}

#[derive(Clone, Debug)]
pub struct Dip {
    pub start_ms: f64,
    pub end_ms: f64,
    pub minimum: f64,
}

#[derive(Clone)]
pub struct Summary {
    pub average: f64,
    pub median: f64,
    pub p1: f64,
    pub minimum: f64,
    pub covered_ms: f64,
    pub near_percent: Option<f64>,
    pub below_percent: Option<f64>,
    pub above_percent: Option<f64>,
    pub gaps: usize,
    pub dip_count: usize,
    pub dips: Vec<Dip>,
}

impl Data {
    pub fn from_snapshot(
        snapshot: &SessionSnapshot,
        samples: &[HistoryPoint],
        source_complete: bool,
    ) -> Result<Self, String> {
        let origin = snapshot
            .segments()
            .first()
            .ok_or("Capture has no start time")?
            .started_at_raw_capture_ticks;
        let frequency = snapshot.timestamp_frequency();
        if frequency == 0 {
            return Err("Invalid capture timestamp frequency".into());
        }
        let origin_ms = origin as f64 * 1000.0 / frequency as f64;
        let rates = samples
            .iter()
            .map(|p| RatePoint {
                time_ms: p.observed_ms - origin_ms,
                rate: p.rate,
                segment: p.segment,
            })
            .collect();
        let evidence = snapshot.capture_evidence();
        let mut quality = Quality {
            integrity: format!("{:?}", snapshot.integrity()),
            source_complete: Some(source_complete),
            overflow: evidence.overflow,
            ignored: evidence.ignored_after_limit,
            disconnected: evidence.disconnected,
            reports: Some(0),
            ..Default::default()
        };
        let mut intervals = IntervalCollector::default();
        for event in snapshot.events() {
            quality.reports = Some(quality.reports.unwrap_or(0) + 1);
            quality.zero_motion += u64::from(event.raw_delta_x == 0 && event.raw_delta_y == 0);
            intervals.push(
                event.raw_capture_ticks,
                event.session_segment_id,
                origin,
                frequency,
                &mut quality,
            )?;
        }
        let mut data = Self {
            name: snapshot.metadata().display_name.clone(),
            rates,
            overview: Vec::new(),
            intervals: intervals.points,
            interval_stride: intervals.stride.max(1),
            quality,
        };
        data.validate()?;
        data.build_overview();
        Ok(data)
    }

    fn build_overview(&mut self) {
        const BUCKETS: usize = 4000;
        let width = self.rates.len().div_ceil(BUCKETS).max(1);
        self.overview.clear();
        for chunk in self.rates.chunks(width) {
            let min = chunk
                .iter()
                .enumerate()
                .min_by(|a, b| a.1.rate.total_cmp(&b.1.rate))
                .unwrap()
                .0;
            let max = chunk
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.rate.total_cmp(&b.1.rate))
                .unwrap()
                .0;
            let mut indices = [0, min, max, chunk.len() - 1];
            indices.sort_unstable();
            let mut previous = None;
            for index in indices {
                if previous != Some(index) {
                    self.overview.push(chunk[index]);
                    previous = Some(index);
                }
            }
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.rates.is_empty() {
            return Err("No valid rate samples are available to analyze".into());
        }
        if self.rates.len() > MAX_SAMPLES {
            return Err("Rate sample limit exceeded".into());
        }
        let mut previous = f64::NEG_INFINITY;
        for point in &self.rates {
            if !point.time_ms.is_finite()
                || !point.rate.is_finite()
                || point.time_ms < 0.0
                || !(0.0..=20_000.0).contains(&point.rate)
                || point.time_ms <= previous
            {
                return Err("Invalid or unordered rate samples in recording".into());
            }
            previous = point.time_ms;
        }
        Ok(())
    }

    pub fn summary(&self, target: u32) -> Result<Summary, String> {
        if target == 0 {
            return Err("Target rate must be greater than zero".into());
        }
        self.validate()?;
        let mut sorted: Vec<f64> = self.rates.iter().map(|p| p.rate).collect();
        sorted.sort_by(f64::total_cmp);
        let percentile = |p: f64| {
            let pos = p * (sorted.len() - 1) as f64;
            let low = pos.floor() as usize;
            let high = pos.ceil() as usize;
            sorted[low] + (sorted[high] - sorted[low]) * (pos - low as f64)
        };
        let threshold = f64::from(target) * 0.90;
        let mut covered = 0.0;
        let mut near = 0.0;
        let mut below = 0.0;
        let mut above = 0.0;
        let mut gaps = 0;
        let mut dips = Vec::new();
        let mut dip_count = 0;
        let mut active: Option<Dip> = None;
        let finish = |active: &mut Option<Dip>, dips: &mut Vec<Dip>, count: &mut usize| {
            if let Some(dip) = active.take() {
                *count += 1;
                if dips.len() < MAX_DISPLAYED_DIPS {
                    dips.push(dip);
                }
            }
        };
        for pair in self.rates.windows(2) {
            let a = pair[0];
            let b = pair[1];
            let elapsed = b.time_ms - a.time_ms;
            if a.segment != b.segment || elapsed > MAX_CONTINUOUS_GAP_MS {
                gaps += 1;
                finish(&mut active, &mut dips, &mut dip_count);
                continue;
            }
            covered += elapsed;
            if (a.rate - target as f64).abs() <= target as f64 * 0.05 {
                near += elapsed;
            } else if a.rate < target as f64 {
                below += elapsed;
            } else {
                above += elapsed;
            }
            if a.rate < threshold {
                let dip = active.get_or_insert(Dip {
                    start_ms: a.time_ms,
                    end_ms: b.time_ms,
                    minimum: a.rate,
                });
                dip.end_ms = b.time_ms;
                dip.minimum = dip.minimum.min(a.rate);
            } else {
                finish(&mut active, &mut dips, &mut dip_count);
            }
        }
        finish(&mut active, &mut dips, &mut dip_count);
        Ok(Summary {
            average: sorted.iter().sum::<f64>() / sorted.len() as f64,
            median: percentile(0.5),
            p1: percentile(0.01),
            minimum: sorted[0],
            covered_ms: covered,
            near_percent: (covered > 0.0).then_some(near / covered * 100.0),
            below_percent: (covered > 0.0).then_some(below / covered * 100.0),
            above_percent: (covered > 0.0).then_some(above / covered * 100.0),
            gaps,
            dip_count,
            dips,
        })
    }
}

#[derive(Default)]
struct IntervalCollector {
    points: Vec<IntervalPoint>,
    previous: Option<(u64, u32)>,
    count: usize,
    stride: usize,
}

impl IntervalCollector {
    fn push(
        &mut self,
        ticks: u64,
        segment: u32,
        origin: u64,
        frequency: u64,
        quality: &mut Quality,
    ) -> Result<(), String> {
        if ticks < origin {
            return Err("Report timestamp precedes capture start".into());
        }
        if let Some((previous, previous_segment)) = self.previous
            && segment == previous_segment
        {
            let difference = ticks
                .checked_sub(previous)
                .ok_or("Report timestamps are not ordered within a capture segment")?;
            quality.equal_timestamps += u64::from(difference == 0);
            let stride = self.stride.max(1);
            if self.count.is_multiple_of(stride) {
                if self.points.len() == MAX_INTERVAL_POINTS {
                    self.points = self.points.iter().step_by(2).copied().collect();
                    self.stride = stride * 2;
                }
                if self.count.is_multiple_of(self.stride.max(1)) {
                    self.points.push(IntervalPoint {
                        time_ms: (ticks - origin) as f64 * 1000.0 / frequency as f64,
                        interval_ms: difference as f64 * 1000.0 / frequency as f64,
                        segment,
                    });
                }
            }
            self.count += 1;
        }
        if self.previous.is_some_and(|(previous, _)| ticks < previous) {
            return Err("Report timestamps are not ordered".into());
        }
        self.previous = Some((ticks, segment));
        Ok(())
    }
}

pub fn import(path: &Path) -> Result<Data, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    if file.metadata().map_err(|error| error.to_string())?.len() > MAX_CSV_BYTES {
        return Err("CSV file exceeds the 2 GiB import limit".into());
    }
    import_reader(file)
}

pub(crate) fn import_reader(reader: impl Read) -> Result<Data, String> {
    let mut reader = csv::Reader::from_reader(reader);
    let headers = reader.headers().map_err(|error| error.to_string())?.clone();
    let column = |name: &str| {
        headers
            .iter()
            .position(|value| value == name)
            .ok_or_else(|| format!("CSV is missing the {name} column"))
    };
    let kind = column("type")?;
    let time = column("elapsed_ms")?;
    let rate = column("rate_hz")?;
    let rate_segment = column("rate_segment")?;
    let ticks = column("raw_ticks")?;
    let capture_segment = column("capture_segment")?;
    let dx = column("dx")?;
    let dy = column("dy")?;
    let frequency = column("tick_hz")?;
    let integrity = column("integrity")?;
    let source_complete = column("source_complete")?;
    let overflow = column("overflow")?;
    let ignored = column("ignored_after_limit")?;
    let disconnected = column("disconnected")?;
    let name = column("device_name")?;
    let parse = |text: &str, field: &str| -> Result<u64, String> {
        text.parse().map_err(|_| format!("Invalid {field} in CSV"))
    };
    let mut quality = Quality::default();
    let mut dataset = Data {
        name: String::new(),
        rates: Vec::new(),
        overview: Vec::new(),
        intervals: Vec::new(),
        interval_stride: 1,
        quality: Quality::default(),
    };
    let mut collector = IntervalCollector::default();
    let mut clock = None;
    let mut origin = None;
    for (index, record) in reader.records().enumerate() {
        let record = record.map_err(|error| format!("CSV row {}: {error}", index + 2))?;
        let field = |column| record.get(column).unwrap_or("");
        match field(kind) {
            "metadata" if clock.is_none() => {
                let value = parse(field(frequency), "tick_hz")?;
                if value == 0 {
                    return Err("Invalid tick_hz in CSV".into());
                }
                clock = Some(value);
                quality.integrity = field(integrity).to_owned();
                quality.source_complete = Some(
                    field(source_complete)
                        .parse()
                        .map_err(|_| "Invalid source_complete in CSV")?,
                );
                quality.overflow = parse(field(overflow), "overflow")?;
                quality.ignored = parse(field(ignored), "ignored_after_limit")?;
                quality.disconnected = field(disconnected)
                    .parse()
                    .map_err(|_| "Invalid disconnected in CSV")?;
                dataset.name = field(name).to_owned();
            }
            "report" => {
                let freq = clock.ok_or("CSV metadata must precede reports")?;
                let tick = parse(field(ticks), "raw_ticks")?;
                let elapsed = field(time)
                    .parse::<f64>()
                    .map_err(|_| "Invalid report elapsed_ms in CSV")?;
                if !elapsed.is_finite() || elapsed < 0.0 {
                    return Err("Invalid report elapsed_ms in CSV".into());
                }
                let origin_tick = *origin.get_or_insert_with(|| {
                    tick.saturating_sub((elapsed * freq as f64 / 1000.0).round() as u64)
                });
                let segment = u32::try_from(parse(field(capture_segment), "capture_segment")?)
                    .map_err(|_| "Invalid capture_segment in CSV")?;
                let x: i32 = field(dx).parse().map_err(|_| "Invalid dx in CSV")?;
                let y: i32 = field(dy).parse().map_err(|_| "Invalid dy in CSV")?;
                quality.reports = Some(quality.reports.unwrap_or(0) + 1);
                quality.zero_motion += u64::from(x == 0 && y == 0);
                collector.push(tick, segment, origin_tick, freq, &mut quality)?;
            }
            "rate" => {
                if clock.is_none() {
                    return Err("CSV metadata must precede rates".into());
                }
                if dataset.rates.len() == MAX_SAMPLES {
                    return Err("Rate sample limit exceeded".into());
                }
                dataset.rates.push(RatePoint {
                    time_ms: field(time)
                        .parse()
                        .map_err(|_| "Invalid rate elapsed_ms in CSV")?,
                    rate: field(rate).parse().map_err(|_| "Invalid rate_hz in CSV")?,
                    segment: parse(field(rate_segment), "rate_segment")?,
                });
            }
            "metadata" => return Err("CSV contains multiple metadata rows".into()),
            _ => return Err(format!("Unknown row type at CSV row {}", index + 2)),
        }
    }
    if clock.is_none() {
        return Err("CSV has no metadata row".into());
    }
    dataset.intervals = collector.points;
    dataset.interval_stride = collector.stride.max(1);
    dataset.quality = quality;
    dataset.validate()?;
    dataset.build_overview();
    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_does_not_bridge_pauses_or_missing_observations() {
        let data = Data {
            name: "test".into(),
            rates: vec![
                RatePoint {
                    time_ms: 0.0,
                    rate: 1000.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 10.0,
                    rate: 500.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 20.0,
                    rate: 400.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 30.0,
                    rate: 1000.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 1000.0,
                    rate: 400.0,
                    segment: 2,
                },
                RatePoint {
                    time_ms: 1010.0,
                    rate: 1000.0,
                    segment: 2,
                },
            ],
            overview: vec![],
            intervals: vec![],
            interval_stride: 1,
            quality: Quality::default(),
        };
        let summary = data.summary(1000).unwrap();
        assert_eq!(summary.covered_ms, 40.0);
        assert_eq!(summary.near_percent, Some(25.0));
        assert_eq!(summary.below_percent, Some(75.0));
        assert_eq!(summary.above_percent, Some(0.0));
        assert_eq!(summary.dip_count, 2);
        assert_eq!(summary.dips[0].start_ms, 10.0);
        assert_eq!(summary.dips[0].end_ms, 30.0);
        assert_eq!(summary.dips[0].minimum, 400.0);
        assert_eq!(summary.gaps, 1);
        let lower_target = data.summary(500).unwrap();
        assert_eq!(lower_target.dip_count, 2);
        assert_eq!(lower_target.below_percent, Some(50.0));
        assert!(data.summary(0).is_err());
    }

    #[test]
    fn report_intervals_never_cross_capture_segments() {
        let mut collector = IntervalCollector::default();
        let mut quality = Quality::default();
        for (ticks, segment) in [(100, 1), (101, 1), (101, 1), (500, 2), (502, 2)] {
            collector
                .push(ticks, segment, 100, 1000, &mut quality)
                .unwrap();
        }
        assert_eq!(collector.points.len(), 3);
        assert_eq!(collector.points[0].interval_ms, 1.0);
        assert_eq!(collector.points[1].interval_ms, 0.0);
        assert_eq!(collector.points[2].interval_ms, 2.0);
        assert_eq!(quality.equal_timestamps, 1);
        assert!(collector.push(501, 3, 100, 1000, &mut quality).is_err());
    }

    #[test]
    fn long_gaps_do_not_count_toward_dips_or_coverage() {
        let data = Data {
            name: String::new(),
            rates: vec![
                RatePoint {
                    time_ms: 0.0,
                    rate: 400.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 100.0,
                    rate: 1000.0,
                    segment: 1,
                },
                RatePoint {
                    time_ms: 110.0,
                    rate: 1000.0,
                    segment: 1,
                },
            ],
            overview: Vec::new(),
            intervals: Vec::new(),
            interval_stride: 1,
            quality: Quality::default(),
        };
        let summary = data.summary(1000).unwrap();
        assert_eq!(summary.gaps, 1);
        assert_eq!(summary.covered_ms, 10.0);
        assert_eq!(summary.dip_count, 0);
        assert_eq!(summary.near_percent, Some(100.0));
    }

    #[test]
    fn overview_retains_extremes_when_samples_are_dense() {
        let mut data = Data {
            name: String::new(),
            rates: (0..100_000)
                .map(|i| RatePoint {
                    time_ms: i as f64,
                    rate: if i == 12_345 { 400.0 } else { 1000.0 },
                    segment: 1,
                })
                .collect(),
            overview: Vec::new(),
            intervals: Vec::new(),
            interval_stride: 1,
            quality: Quality::default(),
        };
        data.build_overview();
        assert!(data.overview.len() <= 16_000);
        assert!(
            data.overview
                .iter()
                .any(|point| point.time_ms == 12_345.0 && point.rate == 400.0)
        );
    }
}
