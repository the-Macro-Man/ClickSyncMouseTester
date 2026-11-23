use super::*;
use crate::product::polling_analysis::{Data, Summary};
use crate::product_files::PollingAnalysisSource;

pub(in crate::ui) struct AnalysisState {
    pub open: bool,
    target: u32,
    primary: Option<(Arc<Data>, Summary)>,
    comparison: Option<(Arc<Data>, Summary)>,
    start: f64,
    end: f64,
    intervals: bool,
    hover: Option<f64>,
    bounds: Arc<Mutex<[f32; 4]>>,
}

impl Default for AnalysisState {
    fn default() -> Self {
        Self {
            open: false,
            target: 1000,
            primary: None,
            comparison: None,
            start: 0.0,
            end: 1.0,
            intervals: false,
            hover: None,
            bounds: Arc::new(Mutex::new([0.0; 4])),
        }
    }
}

impl AnalysisState {
    pub(in crate::ui) fn set_result(
        &mut self,
        primary: Arc<Data>,
        summary: Summary,
        comparison: Option<(Arc<Data>, Summary)>,
        target: u32,
    ) {
        let changed = self
            .primary
            .as_ref()
            .is_none_or(|(old, _)| !Arc::ptr_eq(old, &primary));
        if changed {
            self.start = primary.rates.first().map_or(0.0, |p| p.time_ms);
            self.end = primary.rates.last().map_or(self.start + 1.0, |p| p.time_ms);
            self.end = self.end.max(self.start + 1.0);
            self.hover = None;
        }
        self.primary = Some((primary, summary));
        self.comparison = comparison;
        self.target = target;
    }

    fn zoom(&mut self, factor: f64, center: f64) {
        let Some((data, _)) = &self.primary else {
            return;
        };
        let first = data.rates[0].time_ms;
        let last = data.rates.last().unwrap().time_ms.max(first + 1.0);
        let width = ((self.end - self.start) * factor).clamp(1.0, last - first);
        self.start = (center - width / 2.0).clamp(first, last - width);
        self.end = self.start + width;
    }
}

fn nearest_index<T>(points: &[T], time: f64, get_time: impl Fn(&T) -> f64) -> usize {
    let next = points.partition_point(|point| get_time(point) < time);
    if next == points.len() {
        return points.len() - 1;
    }
    if next > 0 && time - get_time(&points[next - 1]) <= get_time(&points[next]) - time {
        next - 1
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hover_selects_closest_sample() {
        let points = [0.0, 10.0, 30.0];
        assert_eq!(nearest_index(&points, -1.0, |p| *p), 0);
        assert_eq!(nearest_index(&points, 6.0, |p| *p), 1);
        assert_eq!(nearest_index(&points, 19.0, |p| *p), 1);
        assert_eq!(nearest_index(&points, 100.0, |p| *p), 2);
    }
}

impl GpuiHost {
    fn submit_polling_analysis(
        &mut self,
        source: PollingAnalysisSource,
        comparison: Option<Arc<Data>>,
        replace_comparison: bool,
        target: u32,
    ) {
        let result = self.runtime.borrow_mut().submit_file(
            self.receiver,
            FileRequest::AnalyzePolling {
                source,
                comparison,
                replace_comparison,
                target,
            },
        );
        match result {
            Ok(()) => {
                self.file_busy = true;
                self.control_error = None;
            }

            Err(error) => self.control_error = Some(error),
        }
    }

    pub(super) fn open_polling_analysis(&mut self, _cx: &mut Context<Self>) {
        if self.file_busy || self.lock.locked() || self.runtime.borrow().pending_action().is_some()
        {
            return;
        }
        self.polling.analysis.open = true;
        self.control_error = None;
        let prefix = self
            .runtime
            .borrow()
            .backend
            .as_ref()
            .and_then(|b| b.latest_prefix());
        let Some(prefix) = prefix.filter(|p| {
            p.generation >= self.page_generation
                && matches!(
                    p.phase,
                    CapturePhase::Paused | CapturePhase::Stopped | CapturePhase::Faulted
                )
        }) else {
            return;
        };
        let Some((_, snapshot)) = prefix
            .datasets
            .iter()
            .find(|(key, _)| Some(*key) == self.polling.session_device)
        else {
            return;
        };
        let samples = self.runtime.borrow().polling_stream.reader().paused_log(
            self.receiver.0,
            prefix.generation,
            self.domain_reset,
            self.polling.revision,
        );
        match samples {
            Ok(samples) => self.submit_polling_analysis(
                PollingAnalysisSource::Capture {
                    snapshot: snapshot.clone(),
                    samples,
                    source_complete: prefix.complete,
                },
                None,
                false,
                self.polling.analysis.target,
            ),
            Err(error) => self.control_error = Some(error),
        }
    }

    fn import_polling_analysis(&mut self, compare: bool, cx: &mut Context<Self>) {
        if self.file_busy {
            return;
        }
        let primary = self
            .polling
            .analysis
            .primary
            .as_ref()
            .map(|(data, _)| data.clone());
        if compare && primary.is_none() {
            return;
        }
        let chosen = cx.prompt_for_paths(gpui::PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("ClickSync CSV".into()),
        });
        self.file_busy = true;
        cx.spawn(async move |host, cx| {
            let answer = chosen.await;
            let _ = host.update(cx, |host, cx| {
                host.file_busy = false;
                match answer {
                    Ok(Ok(Some(paths))) if !paths.is_empty() => {
                        host.submit_polling_analysis(
                            PollingAnalysisSource::Import(paths[0].clone()),
                            if compare { primary } else { None },
                            compare,
                            host.polling.analysis.target,
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => host.control_error = Some(error.to_string()),
                    Err(error) => host.control_error = Some(error.to_string()),
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn polling_analysis_target(&mut self, target: u32) {
        if self.file_busy {
            return;
        }
        let Some((data, _)) = &self.polling.analysis.primary else {
            self.polling.analysis.target = target;
            return;
        };
        self.submit_polling_analysis(
            PollingAnalysisSource::Existing(data.clone()),
            self.polling
                .analysis
                .comparison
                .as_ref()
                .map(|(data, _)| data.clone()),
            false,
            target,
        );
    }

    pub(super) fn render_polling_analysis(
        &mut self,
        cx: &mut Context<Self>,
    ) -> gpui::Stateful<gpui::Div> {
        let palette = design::Palette::for_theme(self.settings.dark);
        let english = self.settings.english;
        let state = &self.polling.analysis;
        let mut actions = div().flex().gap(px(8.0));
        for (id, label, action, enabled) in [
            (
                "analysis-back",
                self.text("返回", "BACK"),
                0,
                !self.file_busy,
            ),
            (
                "analysis-import",
                self.text("打开 CSV", "OPEN CSV"),
                1,
                !self.file_busy,
            ),
            (
                "analysis-compare",
                self.text("比较 CSV", "COMPARE CSV"),
                2,
                !self.file_busy && state.primary.is_some(),
            ),
        ] {
            actions = actions.child(
                design::button(id, label, palette, english, false, enabled)
                    .px(px(12.0))
                    .on_click(cx.listener(move |host, _, _, cx| {
                        if host.file_busy {
                            return;
                        }
                        match action {
                            0 => host.polling.analysis.open = false,
                            1 => host.import_polling_analysis(false, cx),
                            _ => host.import_polling_analysis(true, cx),
                        }
                        cx.notify();
                    })),
            );
        }
        let mut targets = div().flex().gap(px(6.0));
        for target in [125, 250, 500, 1000, 2000, 4000, 8000] {
            targets = targets.child(
                design::button(
                    format!("analysis-target-{target}"),
                    format!("{target} Hz"),
                    palette,
                    english,
                    false,
                    !self.file_busy,
                )
                .px(px(8.0))
                .when(state.target == target, |b| {
                    b.bg(rgb(palette.selected))
                        .border_color(rgb(palette.accent))
                })
                .on_click(cx.listener(move |host, _, _, cx| {
                    host.polling_analysis_target(target);
                    cx.notify();
                })),
            );
        }
        let mut root = div()
            .id("polling-analysis-view")
            .size_full()
            .overflow_y_scroll()
            .p(px(24.0))
            .flex()
            .flex_col()
            .gap(px(12.0))
            .text_color(rgb(palette.text))
            .child(
                div()
                    .text_size(px(24.0))
                    .child(self.text("回报率分析", "REPORT RATE ANALYSIS")),
            )
            .child(actions)
            .child(
                div()
                    .text_size(px(14.0))
                    .text_color(rgb(palette.muted))
                    .child(self.text(
                        "目标频率：接近目标为 ±5%，低谷为低于目标的 90%。",
                        "Target: near target means within ±5%; dips are below 90% of target.",
                    )),
            )
            .child(targets);
        if self.file_busy {
            root = root.child(self.text("正在分析...", "Analyzing..."));
        }
        if let Some(error) = &self.control_error {
            root = root.child(div().text_color(rgb(palette.muted)).child(error.clone()));
        }
        let Some((data, summary)) = &state.primary else {
            return root.child(self.text(
                "暂停测试或打开包含回报率的 CSV。",
                "Pause a test or open a CSV containing rate samples.",
            ));
        };
        let summary_line = |label: &str, data: &Data, summary: &Summary| {
            let percent = |value: Option<f64>| value.map_or("N/A".into(), |v| format!("{v:.1}%"));
            if english {
                format!(
                    "{label}: {} | n={} | avg {:.1} Hz | median {:.1} Hz | 1st percentile {:.1} Hz | min {:.1} Hz | below {} / near {} / above {} | covered {:.2} s | dips {} | gaps {}",
                    data.name,
                    data.rates.len(),
                    summary.average,
                    summary.median,
                    summary.p1,
                    summary.minimum,
                    percent(summary.below_percent),
                    percent(summary.near_percent),
                    percent(summary.above_percent),
                    summary.covered_ms / 1000.0,
                    summary.dip_count,
                    summary.gaps
                )
            } else {
                format!(
                    "{label}: {} | 采样 {} | 平均 {:.1} Hz | 中位 {:.1} Hz | 第1百分位 {:.1} Hz | 最低 {:.1} Hz | 低于 {} / 接近 {} / 高于 {} | 覆盖 {:.2} 秒 | 低谷 {} | 缺口 {}",
                    data.name,
                    data.rates.len(),
                    summary.average,
                    summary.median,
                    summary.p1,
                    summary.minimum,
                    percent(summary.below_percent),
                    percent(summary.near_percent),
                    percent(summary.above_percent),
                    summary.covered_ms / 1000.0,
                    summary.dip_count,
                    summary.gaps
                )
            }
        };
        root = root.child(
            div()
                .text_size(px(14.0))
                .child(summary_line("A", data, summary)),
        );
        if let Some((other, other_summary)) = &state.comparison {
            root = root.child(div().text_size(px(14.0)).child(summary_line(
                "B",
                other,
                other_summary,
            )));
        }
        for (label, current) in std::iter::once(("A", data.as_ref())).chain(
            state
                .comparison
                .as_ref()
                .map(|(other, _)| ("B", other.as_ref())),
        ) {
            let quality = &current.quality;
            let unavailable = self.text("无数据", "not in CSV");
            let report_count = quality
                .reports
                .map_or(unavailable.into(), |n| n.to_string());
            let zero_motion = quality
                .reports
                .map_or(unavailable.into(), |_| quality.zero_motion.to_string());
            let equal_timestamps = quality
                .reports
                .map_or(unavailable.into(), |_| quality.equal_timestamps.to_string());
            let info = if english {
                format!(
                    "{label} capture: {} | complete: {:?} | overflow: {} | ignored: {} | disconnected: {} | reports: {report_count} | zero-motion: {zero_motion} | equal timestamps: {equal_timestamps}",
                    quality.integrity,
                    quality.source_complete,
                    quality.overflow,
                    quality.ignored,
                    quality.disconnected
                )
            } else {
                format!(
                    "{label} 采集: {} | 完整: {:?} | 溢出: {} | 忽略: {} | 断开: {} | 报告: {report_count} | 零位移: {zero_motion} | 相同时间戳: {equal_timestamps}",
                    quality.integrity,
                    quality.source_complete,
                    quality.overflow,
                    quality.ignored,
                    quality.disconnected
                )
            };
            root = root.child(
                div()
                    .text_size(px(13.0))
                    .text_color(rgb(palette.muted))
                    .child(info),
            );
        }
        root = root.child(div().text_size(px(13.0)).text_color(rgb(palette.muted)).child(self.text(
            "回报率为 300 毫秒内的移动报告；停止移动、系统延迟和时间戳批处理可能影响结果，低谷不代表丢包。",
            "Rate counts movement reports over 300 ms. Stopping movement, scheduling delays and timestamp batching can affect results; a dip is not proof of packet loss.",
        )));
        let mut navigation = div().flex().gap(px(8.0));
        for (id, label, factor, shift) in [
            ("analysis-zoom-in", "+", 0.5, 0.0),
            ("analysis-zoom-out", "-", 2.0, 0.0),
            ("analysis-left", "<", 1.0, -0.5),
            ("analysis-right", ">", 1.0, 0.5),
        ] {
            navigation = navigation.child(
                design::button(id, label, palette, english, false, true)
                    .px(px(12.0))
                    .on_click(cx.listener(move |host, _, _, cx| {
                        let state = &mut host.polling.analysis;
                        let center =
                            (state.start + state.end) / 2.0 + (state.end - state.start) * shift;
                        state.zoom(factor, center);
                        cx.notify();
                    })),
            );
        }
        navigation = navigation.child(
            design::button(
                "analysis-mode",
                if state.intervals {
                    self.text("回报率", "RATE")
                } else {
                    self.text("报告间隔", "REPORT INTERVALS")
                },
                palette,
                english,
                false,
                true,
            )
            .px(px(12.0))
            .on_click(cx.listener(|host, _, _, cx| {
                host.polling.analysis.intervals = !host.polling.analysis.intervals;
                host.polling.analysis.hover = None;
                cx.notify();
            })),
        );
        root = root
            .child(navigation)
            .child(div().text_size(px(13.0)).child(format!(
                "{:.3}–{:.3} s | {}",
                state.start / 1000.0,
                state.end / 1000.0,
                if state.intervals {
                    if english {
                        format!("intervals (every {}th retained)", data.interval_stride)
                    } else {
                        format!("报告间隔（每 {} 个保留 1 个）", data.interval_stride)
                    }
                } else {
                    self.text(
                        "回报率采样（放大查看详情）",
                        "rate samples (zoom for detail)",
                    )
                    .into()
                },
            )));
        if state.intervals && data.intervals.is_empty() {
            root = root.child(self.text(
                "此文件没有可用的原始报告间隔。",
                "No raw report intervals are available in this file.",
            ));
        } else {
            root = root.child(self.polling_analysis_chart(data.clone(), cx));
        }
        if let Some(time) = state.hover {
            let points = &data.rates;
            let index = nearest_index(points, time, |p| p.time_ms);
            if state.intervals && !data.intervals.is_empty() {
                let i = nearest_index(&data.intervals, time, |p| p.time_ms);
                let p = data.intervals[i];
                root = root.child(format!(
                    "Nearby: {:.3} s, {:.4} ms (segment {})",
                    p.time_ms / 1000.0,
                    p.interval_ms,
                    p.segment
                ));
            } else {
                let p = points[index];
                root = root.child(format!(
                    "Nearby: {:.3} s, {:.1} Hz",
                    p.time_ms / 1000.0,
                    p.rate
                ));
            }
        }
        root = root.child(div().text_size(px(16.0)).child(if english {
            format!(
                "Dips: {} (first {} shown)",
                summary.dip_count,
                summary.dips.len()
            )
        } else {
            format!(
                "低谷: {}（显示前 {} 个）",
                summary.dip_count,
                summary.dips.len()
            )
        }));
        for (index, dip) in summary.dips.iter().enumerate() {
            let at = (dip.start_ms + dip.end_ms) / 2.0;
            let width = (dip.end_ms - dip.start_ms).max(1000.0) * 2.0;
            root = root.child(
                design::button(
                    format!("analysis-dip-{index}"),
                    format!(
                        "{:.3}–{:.3} s | {:.0} Hz min | {:.0} ms",
                        dip.start_ms / 1000.0,
                        dip.end_ms / 1000.0,
                        dip.minimum,
                        dip.end_ms - dip.start_ms
                    ),
                    palette,
                    english,
                    false,
                    true,
                )
                .px(px(8.0))
                .on_click(cx.listener(move |host, _, _, cx| {
                    let state = &mut host.polling.analysis;
                    state.intervals = false;
                    state.zoom(width / (state.end - state.start), at);
                    cx.notify();
                })),
            );
        }
        root
    }

    fn polling_analysis_chart(
        &self,
        data: Arc<Data>,
        cx: &mut Context<Self>,
    ) -> gpui::Stateful<gpui::Div> {
        let state = &self.polling.analysis;
        let (start, end, intervals) = (state.start, state.end, state.intervals);
        let target = state.target;
        let bounds = state.bounds.clone();
        let paint_bounds = bounds.clone();
        let color = design::Palette::for_theme(self.settings.dark).accent;
        let marks: Vec<(f64, f64)> = if intervals {
            let points = &data.intervals;
            let a = points.partition_point(|p| p.time_ms < start);
            let b = points.partition_point(|p| p.time_ms <= end);
            let stride = ((b - a) / 4000).max(1);
            points[a..b]
                .iter()
                .step_by(stride)
                .map(|p| (p.time_ms, p.interval_ms))
                .collect()
        } else {
            let a = data.rates.partition_point(|p| p.time_ms < start);
            let b = data.rates.partition_point(|p| p.time_ms <= end);
            let points = if b - a > 16000 {
                &data.overview
            } else {
                &data.rates
            };
            let a = points.partition_point(|p| p.time_ms < start);
            let b = points.partition_point(|p| p.time_ms <= end);
            points[a..b].iter().map(|p| (p.time_ms, p.rate)).collect()
        };
        let max = if intervals {
            marks.iter().map(|p| p.1).fold(1.0, f64::max)
        } else {
            marks.iter().map(|p| p.1).fold(target as f64, f64::max)
        };
        div()
            .id("analysis-chart")
            .w_full()
            .h(px(230.0))
            .flex_shrink_0()
            .relative()
            .on_mouse_move(
                cx.listener(move |host, event: &gpui::MouseMoveEvent, _, cx| {
                    let [x, _, width, _] = *bounds.lock().unwrap();
                    if width > 0.0 {
                        let fraction = ((f32::from(event.position.x) - x) / width).clamp(0.0, 1.0);
                        host.polling.analysis.hover =
                            Some(start + f64::from(fraction) * (end - start));
                        cx.notify();
                    }
                }),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .text_size(px(12.0))
                    .child(if intervals {
                        format!("{max:.2} ms")
                    } else {
                        format!("{max:.0} Hz")
                    }),
            )
            .child(
                canvas(
                    move |bounds, _, _| {
                        *paint_bounds.lock().unwrap() = [
                            f32::from(bounds.left()),
                            f32::from(bounds.top()),
                            f32::from(bounds.size.width),
                            f32::from(bounds.size.height),
                        ];
                    },
                    move |bounds, _, window, _| {
                        let mut path = gpui::PathBuilder::stroke(px(1.5));
                        for (time, value) in &marks {
                            let x = bounds.left()
                                + px(((*time - start) / (end - start)
                                    * f64::from(f32::from(bounds.size.width)))
                                    as f32);
                            let y = bounds.bottom()
                                - px(
                                    (value / max * f64::from(f32::from(bounds.size.height))) as f32
                                );
                            path.move_to(gpui::point(x, y));
                            path.line_to(gpui::point(x, y + px(2.0)));
                        }
                        if let Ok(path) = path.build() {
                            window.paint_path(path, gpui::rgba((color << 8) | 0xff));
                        }
                    },
                )
                .size_full(),
            )
    }
}
