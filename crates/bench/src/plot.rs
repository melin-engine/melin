//! Generate SVG plots from benchmark JSON results.
//!
//! Reads JSON files produced by `melin-bench --json` and generates:
//! 1. **Latency CDF** — percentile plot comparing benchmark configs
//! 2. **Saturation curve** — throughput vs latency at multiple load levels
//! 3. **Pipeline breakdown** — stage utilization bar chart
//!
//! Usage:
//!   melin-plot latency-cdf -o latency.svg results/*.json
//!   melin-plot saturation -o saturation.svg sweep/*.json
//!   melin-plot pipeline -o pipeline.svg --stats pipeline-stats.log

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use plotters::prelude::*;
use serde::Deserialize;

// --- JSON schema matching melin-bench output ---

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BenchResult {
    label: String,
    #[allow(dead_code)]
    measured_orders: u64,
    #[allow(dead_code)]
    warmup_orders: u64,
    #[allow(dead_code)]
    wall_ms: f64,
    throughput_ops: f64,
    latency: BTreeMap<String, f64>,
}

/// Pipeline stats parsed from tracing output.
#[derive(Debug)]
struct PipelineStage {
    name: String,
    pct_busy: f64,
}

// --- Colors ---
// Professional palette suitable for investor presentations.
const COLOR_FSYNC: RGBColor = RGBColor(41, 98, 255); // blue
const COLOR_NO_PERSIST: RGBColor = RGBColor(0, 200, 83); // green
const COLOR_SINGLE: RGBColor = RGBColor(255, 145, 0); // amber
const COLORS: [RGBColor; 6] = [
    COLOR_FSYNC,
    COLOR_NO_PERSIST,
    COLOR_SINGLE,
    RGBColor(156, 39, 176), // purple
    RGBColor(244, 67, 54),  // red
    RGBColor(0, 188, 212),  // cyan
];

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "latency-cdf" => cmd_latency_cdf(&args[2..]),
        "saturation" => cmd_saturation(&args[2..]),
        "sweep" => cmd_sweep(&args[2..]),
        "stability" => cmd_stability(&args[2..]),
        "pipeline" => cmd_pipeline(&args[2..]),
        "all" => cmd_all(&args[2..]),
        _ => {
            eprintln!("unknown command: {}", args[1]);
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("Usage: melin-plot <command> [options] <files...>");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  latency-cdf  Percentile plot comparing benchmark configs");
    eprintln!("  saturation   Throughput vs latency at multiple load levels");
    eprintln!("  sweep        Parameter vs throughput (e.g. window depth sweep)");
    eprintln!("  stability    Latency stability over time (from time-series JSON)");
    eprintln!("  pipeline     Pipeline stage utilization bar chart");
    eprintln!("  all          Generate all plots from a results directory");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  -o <file>    Output SVG path (default: <command>.svg)");
    eprintln!("  --stats <f>  Pipeline stats log file (for pipeline command)");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  melin-plot latency-cdf results/1-fsync.json results/2-no-persist.json");
    eprintln!("  melin-plot saturation sweep/*.json");
    eprintln!("  melin-plot pipeline --stats /tmp/melin-server.log");
    eprintln!("  melin-plot all results/");
}

// --- Argument parsing (minimal, no clap dependency for this tool) ---

struct PlotArgs {
    output: PathBuf,
    stats_file: Option<PathBuf>,
    files: Vec<PathBuf>,
}

fn parse_args(args: &[String], default_output: &str) -> PlotArgs {
    let mut output = PathBuf::from(default_output);
    let mut stats_file = None;
    let mut files = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-o" => {
                i += 1;
                if i < args.len() {
                    output = PathBuf::from(&args[i]);
                }
            }
            "--stats" => {
                i += 1;
                if i < args.len() {
                    stats_file = Some(PathBuf::from(&args[i]));
                }
            }
            _ => {
                files.push(PathBuf::from(&args[i]));
            }
        }
        i += 1;
    }

    PlotArgs {
        output,
        stats_file,
        files,
    }
}

/// A loaded benchmark result paired with its source filename.
struct LoadedResult {
    result: BenchResult,
    filename: String,
}

fn load_results(files: &[PathBuf]) -> Vec<LoadedResult> {
    let mut results = Vec::new();
    for path in files {
        let data = fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("warning: cannot read {}: {}", path.display(), e);
            String::new()
        });
        if data.is_empty() {
            continue;
        }
        match serde_json::from_str::<BenchResult>(&data) {
            Ok(r) => {
                let filename = path
                    .file_name()
                    .map(|f| f.to_string_lossy().into_owned())
                    .unwrap_or_default();
                results.push(LoadedResult {
                    result: r,
                    filename,
                });
            }
            Err(e) => eprintln!("warning: cannot parse {}: {}", path.display(), e),
        }
    }
    results
}

// --- Percentile extraction ---

/// Standard percentile labels in order, mapped to quantile values.
const PERCENTILES: &[(&str, &str, f64)] = &[
    ("min_us", "min", 0.0),
    ("p50_us", "p50", 50.0),
    ("p90_us", "p90", 90.0),
    ("p99_us", "p99", 99.0),
    ("p99.9_us", "p99.9", 99.9),
    ("p99.9.9_us", "p99.99", 99.99),
    ("p99.9.9.9_us", "p99.999", 99.999),
    ("p99.9.9.9.9_us", "p99.9999", 99.9999),
    ("p99.9.9.9.9.9_us", "p99.99999", 99.99999),
    ("max_us", "max", 100.0),
];

fn extract_percentiles(result: &BenchResult) -> Vec<(f64, f64)> {
    let mut points = Vec::new();
    for &(key, _, pct) in PERCENTILES {
        if let Some(&val) = result.latency.get(key) {
            points.push((pct, val));
        }
    }
    points
}

/// Format throughput as a compact string (e.g. "4.0M/s").
fn format_throughput(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.0}K/s", ops / 1_000.0)
    } else {
        format!("{:.0}/s", ops)
    }
}

/// Extract a human-readable mode name from a filename like "1-fsync.json".
///
/// Strips the leading number prefix and extension, then title-cases the
/// remainder: "1-fsync.json" -> "Fsync", "4-replication.json" -> "Replication",
/// "2-no-persist.json" -> "No-persist".
fn mode_from_filename(filename: &str) -> String {
    let stem = filename.strip_suffix(".json").unwrap_or(filename);
    // Strip leading digit-dash prefix (e.g. "1-", "42-").
    let name = stem
        .find('-')
        .and_then(|pos| {
            if stem[..pos].chars().all(|c| c.is_ascii_digit()) {
                Some(&stem[pos + 1..])
            } else {
                None
            }
        })
        .unwrap_or(stem);
    // Title-case the first character.
    let mut chars = name.chars();
    match chars.next() {
        Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str()),
        None => name.to_string(),
    }
}

/// Build a label for the latency CDF plot: "Mode (throughput)".
///
/// Uses the source filename to infer the mode name, then appends throughput.
/// Example: "1-fsync.json" with 4.0M ops/s -> "Fsync (4.0M/s)".
fn cdf_label(loaded: &LoadedResult) -> String {
    let mode = mode_from_filename(&loaded.filename);
    let tp = format_throughput(loaded.result.throughput_ops);
    format!("{mode} ({tp})")
}

/// Build a label for saturation plots using the filename stem directly.
///
/// Example: "w256.json" -> "w256", "i100.json" -> "i100".
fn saturation_label(loaded: &LoadedResult) -> String {
    loaded
        .filename
        .strip_suffix(".json")
        .unwrap_or(&loaded.filename)
        .to_string()
}

// =====================================================================
// Command: latency-cdf
// =====================================================================

fn cmd_latency_cdf(args: &[String]) {
    let opts = parse_args(args, "latency-cdf.svg");
    let results = load_results(&opts.files);
    if results.is_empty() {
        eprintln!("error: no result files loaded");
        std::process::exit(1);
    }
    plot_latency_cdf(&results, &opts.output);
}

fn plot_latency_cdf(results: &[LoadedResult], output: &PathBuf) {
    // Filter out low-throughput results (e.g. single-order latency at ~13K ops/s)
    // that are incomparable with peak-load benchmarks on the same CDF axes.
    let results: Vec<&LoadedResult> = results
        .iter()
        .filter(|r| r.result.throughput_ops >= 100_000.0)
        .collect();
    if results.is_empty() {
        eprintln!("warning: no results >= 100K ops/s for CDF plot, skipping");
        return;
    }

    let root = SVGBackend::new(output, (900, 500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    // Find max latency across all results for Y-axis range.
    let max_lat = results
        .iter()
        .flat_map(|r| r.result.latency.values())
        .cloned()
        .fold(0.0f64, f64::max)
        * 1.1;

    // Percentile labels for X-axis.
    let pct_labels: Vec<&str> = PERCENTILES.iter().map(|&(_, label, _)| label).collect();

    let mut chart = ChartBuilder::on(&root)
        .caption("Latency by Percentile", ("sans-serif", 22).into_font())
        .margin(15)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .build_cartesian_2d(0usize..pct_labels.len().saturating_sub(1), 0.0..max_lat)
        .unwrap();

    chart
        .configure_mesh()
        .x_desc("Percentile")
        .y_desc("Latency (µs)")
        .x_label_formatter(&|idx| pct_labels.get(*idx).copied().unwrap_or("").to_string())
        .y_label_formatter(&|v| {
            if *v >= 1000.0 {
                format!("{:.1}ms", v / 1000.0)
            } else {
                format!("{:.0}µs", v)
            }
        })
        .draw()
        .unwrap();

    for (i, loaded) in results.iter().enumerate() {
        let points = extract_percentiles(&loaded.result);
        let color = COLORS[i % COLORS.len()];
        let label = cdf_label(loaded);

        // Map percentile values to x-indices.
        let indexed: Vec<(usize, f64)> = points
            .iter()
            .filter_map(|(pct, val)| {
                PERCENTILES
                    .iter()
                    .position(|&(_, _, p)| (p - pct).abs() < 0.000001)
                    .map(|idx| (idx, *val))
            })
            .collect();

        chart
            .draw_series(LineSeries::new(indexed.clone(), color.stroke_width(2)))
            .unwrap()
            .label(label)
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2))
            });

        // Draw dots at each data point.
        chart
            .draw_series(
                indexed
                    .iter()
                    .map(|&(x, y)| Circle::new((x, y), 3, color.filled())),
            )
            .unwrap();
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK.mix(0.3))
        .draw()
        .unwrap();

    root.present().unwrap();
    eprintln!("wrote {}", output.display());
}

// =====================================================================
// Command: saturation
// =====================================================================

fn cmd_saturation(args: &[String]) {
    let opts = parse_args(args, "saturation.svg");
    let loaded = load_results(&opts.files);
    if loaded.len() < 2 {
        eprintln!("error: need at least 2 result files for saturation curve");
        std::process::exit(1);
    }
    plot_saturation(&loaded, &opts.output);
}

fn plot_saturation(results: &[LoadedResult], output: &PathBuf) {
    let root = SVGBackend::new(output, (900, 500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    // Sort by throughput for the curve.
    let mut sorted: Vec<&LoadedResult> = results.iter().collect();
    sorted.sort_by(|a, b| {
        a.result
            .throughput_ops
            .partial_cmp(&b.result.throughput_ops)
            .unwrap()
    });

    let max_throughput = sorted
        .last()
        .map(|r| r.result.throughput_ops)
        .unwrap_or(1.0)
        * 1.1;
    let max_latency = sorted
        .iter()
        .filter_map(|r| r.result.latency.get("p99_us"))
        .cloned()
        .fold(0.0f64, f64::max)
        * 1.3;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Throughput vs Latency (Saturation Curve)",
            ("sans-serif", 22).into_font(),
        )
        .margin(15)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .build_cartesian_2d(0.0..max_throughput, 0.0..max_latency)
        .unwrap();

    chart
        .configure_mesh()
        .x_desc("Throughput (orders/sec)")
        .y_desc("Latency (µs)")
        .x_label_formatter(&|v| {
            if *v >= 1_000_000.0 {
                format!("{:.1}M", v / 1_000_000.0)
            } else if *v >= 1_000.0 {
                format!("{:.0}K", v / 1_000.0)
            } else {
                format!("{:.0}", v)
            }
        })
        .y_label_formatter(&|v| {
            if *v >= 1000.0 {
                format!("{:.1}ms", v / 1000.0)
            } else {
                format!("{:.0}µs", v)
            }
        })
        .draw()
        .unwrap();

    // Plot p50 line.
    let p50_points: Vec<(f64, f64)> = sorted
        .iter()
        .filter_map(|r| {
            r.result
                .latency
                .get("p50_us")
                .map(|lat| (r.result.throughput_ops, *lat))
        })
        .collect();

    if !p50_points.is_empty() {
        chart
            .draw_series(LineSeries::new(
                p50_points.clone(),
                COLOR_FSYNC.stroke_width(2),
            ))
            .unwrap()
            .label("p50")
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], COLOR_FSYNC.stroke_width(2))
            });
        chart
            .draw_series(
                p50_points
                    .iter()
                    .map(|&(x, y)| Circle::new((x, y), 4, COLOR_FSYNC.filled())),
            )
            .unwrap();
    }

    // Plot p99 line.
    let p99_points: Vec<(f64, f64)> = sorted
        .iter()
        .filter_map(|r| {
            r.result
                .latency
                .get("p99_us")
                .map(|lat| (r.result.throughput_ops, *lat))
        })
        .collect();

    if !p99_points.is_empty() {
        chart
            .draw_series(LineSeries::new(
                p99_points.clone(),
                COLOR_NO_PERSIST.stroke_width(2),
            ))
            .unwrap()
            .label("p99")
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], COLOR_NO_PERSIST.stroke_width(2))
            });
        chart
            .draw_series(
                p99_points
                    .iter()
                    .map(|&(x, y)| Circle::new((x, y), 4, COLOR_NO_PERSIST.filled())),
            )
            .unwrap();
    }

    // Plot p99.9 line.
    let p999_points: Vec<(f64, f64)> = sorted
        .iter()
        .filter_map(|r| {
            r.result
                .latency
                .get("p99.9_us")
                .map(|lat| (r.result.throughput_ops, *lat))
        })
        .collect();

    if !p999_points.is_empty() {
        chart
            .draw_series(LineSeries::new(
                p999_points.clone(),
                COLOR_SINGLE.stroke_width(2),
            ))
            .unwrap()
            .label("p99.9")
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], COLOR_SINGLE.stroke_width(2))
            });
        chart
            .draw_series(
                p999_points
                    .iter()
                    .map(|&(x, y)| Circle::new((x, y), 4, COLOR_SINGLE.filled())),
            )
            .unwrap();
    }

    // Label each data point with its filename stem (e.g. "w32", "w256")
    // above the p99.9 dot for readability.
    let labels: Vec<(f64, f64, String)> = sorted
        .iter()
        .filter_map(|r| {
            r.result
                .latency
                .get("p99.9_us")
                .or_else(|| r.result.latency.get("p99_us"))
                .map(|lat| (r.result.throughput_ops, *lat, saturation_label(r)))
        })
        .collect();
    if !labels.is_empty() {
        chart
            .draw_series(labels.iter().map(|(x, y, label)| {
                Text::new(
                    label.clone(),
                    (*x, *y + max_latency * 0.04),
                    ("sans-serif", 11).into_font(),
                )
            }))
            .unwrap();
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK.mix(0.3))
        .draw()
        .unwrap();

    root.present().unwrap();
    eprintln!("wrote {}", output.display());
}

// =====================================================================
// Command: sweep — parameter vs throughput
// =====================================================================

fn cmd_sweep(args: &[String]) {
    let opts = parse_args(args, "sweep.svg");
    let results = load_results(&opts.files);
    if results.len() < 2 {
        eprintln!("error: need at least 2 result files for sweep plot");
        std::process::exit(1);
    }
    plot_sweep(&results, &opts.output);
}

/// Extract the numeric value from a filename stem.
/// "w256.json" → 256, "i100.json" → 100, "w32.json" → 32.
fn extract_sweep_value(filename: &str) -> Option<f64> {
    let stem = filename.strip_suffix(".json").unwrap_or(filename);
    // Strip leading non-digit prefix (e.g. "w" from "w256").
    let digits: String = stem.chars().skip_while(|c| !c.is_ascii_digit()).collect();
    digits.parse::<f64>().ok()
}

/// Extract the parameter name prefix from a filename stem.
/// "w256.json" → "Window depth", "i100.json" → "Instruments", "c64.json" → "Clients".
fn sweep_x_label(filename: &str) -> &'static str {
    let stem = filename.strip_suffix(".json").unwrap_or(filename);
    if stem.starts_with('w') {
        "Window depth"
    } else if stem.starts_with('i') {
        "Instruments"
    } else if stem.starts_with('c') {
        "Clients"
    } else if stem.starts_with('a') {
        "Accounts"
    } else {
        "Parameter"
    }
}

fn plot_sweep(results: &[LoadedResult], output: &PathBuf) {
    let root = SVGBackend::new(output, (900, 500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    // Build (parameter_value, throughput, p99_latency) points.
    let mut points: Vec<(f64, f64, f64)> = results
        .iter()
        .filter_map(|r| {
            let val = extract_sweep_value(&r.filename)?;
            let p99 = r.result.latency.get("p99_us").copied().unwrap_or(0.0);
            Some((val, r.result.throughput_ops, p99))
        })
        .collect();
    points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    if points.is_empty() {
        eprintln!("error: no valid sweep data (could not extract numeric values from filenames)");
        std::process::exit(1);
    }

    let x_label = sweep_x_label(&results[0].filename);
    let max_x = points.last().map(|p| p.0).unwrap_or(1.0) * 1.15;
    let max_throughput = points.iter().map(|p| p.1).fold(0.0f64, f64::max) * 1.15;
    let max_latency = points.iter().map(|p| p.2).fold(0.0f64, f64::max) * 1.3;

    // Split into left (throughput) and right (latency) areas.
    let (left, right) = root.split_horizontally(800);

    let mut chart = ChartBuilder::on(&left)
        .caption(
            format!("Throughput & Latency vs {x_label}"),
            ("sans-serif", 22).into_font(),
        )
        .margin(15)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .right_y_label_area_size(70)
        .build_cartesian_2d(0.0..max_x, 0.0..max_throughput)
        .unwrap()
        .set_secondary_coord(0.0..max_x, 0.0..max_latency);

    chart
        .configure_mesh()
        .x_desc(x_label)
        .y_desc("Throughput (orders/sec)")
        .x_label_formatter(&|v| {
            if *v >= 1000.0 {
                format!("{:.0}K", v / 1000.0)
            } else {
                format!("{:.0}", v)
            }
        })
        .y_label_formatter(&|v| {
            if *v >= 1_000_000.0 {
                format!("{:.1}M", v / 1_000_000.0)
            } else if *v >= 1_000.0 {
                format!("{:.0}K", v / 1_000.0)
            } else {
                format!("{:.0}", v)
            }
        })
        .draw()
        .unwrap();

    chart
        .configure_secondary_axes()
        .y_desc("p99 Latency (µs)")
        .y_label_formatter(&|v| {
            if *v >= 1000.0 {
                format!("{:.1}ms", v / 1000.0)
            } else {
                format!("{:.0}µs", v)
            }
        })
        .draw()
        .unwrap();

    // Throughput line (primary Y-axis).
    let tp_points: Vec<(f64, f64)> = points.iter().map(|p| (p.0, p.1)).collect();
    chart
        .draw_series(LineSeries::new(
            tp_points.clone(),
            COLOR_FSYNC.stroke_width(2),
        ))
        .unwrap()
        .label("Throughput")
        .legend(move |(x, y)| {
            PathElement::new(vec![(x, y), (x + 20, y)], COLOR_FSYNC.stroke_width(2))
        });
    chart
        .draw_series(
            tp_points
                .iter()
                .map(|&(x, y)| Circle::new((x, y), 4, COLOR_FSYNC.filled())),
        )
        .unwrap();

    // p99 latency line (secondary Y-axis).
    let lat_points: Vec<(f64, f64)> = points.iter().map(|p| (p.0, p.2)).collect();
    chart
        .draw_secondary_series(LineSeries::new(
            lat_points.clone(),
            COLOR_SINGLE.stroke_width(2),
        ))
        .unwrap()
        .label("p99 Latency")
        .legend(move |(x, y)| {
            PathElement::new(vec![(x, y), (x + 20, y)], COLOR_SINGLE.stroke_width(2))
        });
    chart
        .draw_secondary_series(
            lat_points
                .iter()
                .map(|&(x, y)| Circle::new((x, y), 4, COLOR_SINGLE.filled())),
        )
        .unwrap();

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK.mix(0.3))
        .draw()
        .unwrap();

    // We don't draw on the right area — it's just padding for the secondary axis.
    let _ = right;

    root.present().unwrap();
    eprintln!("wrote {}", output.display());
}

// =====================================================================
// Command: stability — latency stability over time
// =====================================================================

/// Time-series data embedded in the JSON output.
#[derive(Debug, Deserialize)]
struct TimeSeriesResult {
    #[allow(dead_code)]
    label: String,
    throughput_ops: f64,
    #[serde(default)]
    time_series: Vec<TimeSeriesPoint>,
}

/// Deserialized from JSON time-series output; fields exist for schema
/// compatibility even when not all are plotted.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TimeSeriesPoint {
    elapsed_secs: f64,
    p99_us: f64,
    p999_us: f64,
    p9999_us: f64,
}

fn cmd_stability(args: &[String]) {
    let opts = parse_args(args, "stability.svg");
    if opts.files.is_empty() {
        eprintln!("error: need at least 1 result file with time_series data");
        std::process::exit(1);
    }

    let mut all_results = Vec::new();
    for path in &opts.files {
        let data = fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!("warning: cannot read {}: {}", path.display(), e);
            String::new()
        });
        if data.is_empty() {
            continue;
        }
        match serde_json::from_str::<TimeSeriesResult>(&data) {
            Ok(r) if !r.time_series.is_empty() => {
                let filename = path
                    .file_name()
                    .map(|f| f.to_string_lossy().into_owned())
                    .unwrap_or_default();
                all_results.push((r, filename));
            }
            Ok(_) => eprintln!(
                "warning: {} has no time_series data, skipping",
                path.display()
            ),
            Err(e) => eprintln!("warning: cannot parse {}: {}", path.display(), e),
        }
    }

    if all_results.is_empty() {
        eprintln!("error: no files with time_series data found");
        std::process::exit(1);
    }

    plot_stability(&all_results, &opts.output);
}

fn plot_stability(results: &[(TimeSeriesResult, String)], output: &PathBuf) {
    let root = SVGBackend::new(output, (900, 500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let max_time = results
        .iter()
        .flat_map(|(r, _)| r.time_series.iter().map(|p| p.elapsed_secs))
        .fold(0.0f64, f64::max)
        * 1.05;

    let max_lat = results
        .iter()
        .flat_map(|(r, _)| r.time_series.iter().map(|p| p.p9999_us))
        .fold(0.0f64, f64::max)
        * 1.2;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Latency Stability Over Time",
            ("sans-serif", 22).into_font(),
        )
        .margin(15)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .build_cartesian_2d(0.0..max_time, 0.0..max_lat)
        .unwrap();

    chart
        .configure_mesh()
        .x_desc("Time (seconds)")
        .y_desc("Latency (µs)")
        .x_label_formatter(&|v| format!("{:.0}s", v))
        .y_label_formatter(&|v| {
            if *v >= 1000.0 {
                format!("{:.1}ms", v / 1000.0)
            } else {
                format!("{:.0}µs", v)
            }
        })
        .draw()
        .unwrap();

    for (i, (result, filename)) in results.iter().enumerate() {
        let color = COLORS[i % COLORS.len()];
        let mode = mode_from_filename(filename);
        let tp = format_throughput(result.throughput_ops);

        // p99.99 line — fewer points than p99.9, easier to read.
        let p9999_points: Vec<(f64, f64)> = result
            .time_series
            .iter()
            .map(|p| (p.elapsed_secs, p.p9999_us))
            .collect();
        chart
            .draw_series(LineSeries::new(p9999_points, color.stroke_width(2)))
            .unwrap()
            .label(format!("{mode} p99.99 ({tp})"))
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2))
            });
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK.mix(0.3))
        .draw()
        .unwrap();

    root.present().unwrap();
    eprintln!("wrote {}", output.display());
}

// =====================================================================
// Command: pipeline
// =====================================================================

fn cmd_pipeline(args: &[String]) {
    let opts = parse_args(args, "pipeline.svg");

    // Try loading stats from --stats file or from JSON files.
    let stages = if let Some(ref stats_path) = opts.stats_file {
        parse_pipeline_stats(&fs::read_to_string(stats_path).unwrap_or_default())
    } else {
        eprintln!("error: --stats <log-file> required for pipeline command");
        std::process::exit(1);
    };

    if stages.is_empty() {
        eprintln!("error: no pipeline stats found in log file");
        std::process::exit(1);
    }

    plot_pipeline(&stages, &opts.output);
}

fn parse_pipeline_stats(log: &str) -> Vec<PipelineStage> {
    let mut stages = Vec::new();
    for line in log.lines() {
        // Match: stage="matching" pct_busy=18.48%
        if let Some(stage_start) = line.find("stage=\"") {
            let rest = &line[stage_start + 7..];
            if let Some(end) = rest.find('"') {
                let name = rest[..end].to_string();
                if let Some(pct_start) = line.find("pct_busy=") {
                    let pct_rest = &line[pct_start + 9..];
                    if let Some(pct_end) = pct_rest.find('%')
                        && let Ok(pct) = pct_rest[..pct_end].parse::<f64>()
                    {
                        stages.push(PipelineStage {
                            name,
                            pct_busy: pct,
                        });
                    }
                }
            }
        }
    }
    stages
}

fn plot_pipeline(stages: &[PipelineStage], output: &PathBuf) {
    let root = SVGBackend::new(output, (700, 400)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let max_pct = stages
        .iter()
        .map(|s| s.pct_busy)
        .fold(0.0f64, f64::max)
        .max(10.0)
        * 1.3;

    let mut chart = ChartBuilder::on(&root)
        .caption("Pipeline Stage Utilization", ("sans-serif", 22).into_font())
        .margin(15)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .build_cartesian_2d(0usize..stages.len(), 0.0..max_pct)
        .unwrap();

    chart
        .configure_mesh()
        .x_desc("Stage")
        .y_desc("Busy %")
        .x_label_formatter(&|idx| stages.get(*idx).map(|s| s.name.clone()).unwrap_or_default())
        .y_label_formatter(&|v| format!("{:.0}%", v))
        .draw()
        .unwrap();

    chart
        .draw_series(stages.iter().enumerate().map(|(i, stage)| {
            let color = COLORS[i % COLORS.len()];
            let x0 = i;
            let x1 = i + 1;
            Rectangle::new([(x0, 0.0), (x1, stage.pct_busy)], color.filled())
        }))
        .unwrap();

    // Value labels on top of bars.
    chart
        .draw_series(stages.iter().enumerate().map(|(i, stage)| {
            Text::new(
                format!("{:.1}%", stage.pct_busy),
                (i, stage.pct_busy + max_pct * 0.03),
                ("sans-serif", 14).into_font(),
            )
        }))
        .unwrap();

    root.present().unwrap();
    eprintln!("wrote {}", output.display());
}

// =====================================================================
// Command: all — generate all plots from a results directory
// =====================================================================

fn cmd_all(args: &[String]) {
    let opts = parse_args(args, ".");

    let dir = if opts.files.is_empty() {
        PathBuf::from(".")
    } else {
        opts.files[0].clone()
    };

    // Find all JSON files in the directory.
    let mut json_files: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                json_files.push(path);
            }
        }
    }
    json_files.sort();

    if json_files.is_empty() {
        eprintln!("error: no JSON files found in {}", dir.display());
        std::process::exit(1);
    }

    let results = load_results(&json_files);
    if results.is_empty() {
        eprintln!("error: no valid results loaded");
        std::process::exit(1);
    }

    // 1. Latency CDF (plot_latency_cdf filters out < 100K ops/s internally).
    let cdf_path = dir.join("latency-cdf.svg");
    plot_latency_cdf(&results, &cdf_path);

    // 2. Saturation curve (only if we have multiple load levels).
    if results.len() >= 2 {
        let sat_path = dir.join("saturation.svg");
        plot_saturation(&results, &sat_path);
    } else {
        eprintln!("skipping saturation plot (need >= 2 result files)");
    }

    // 3. Pipeline breakdown (look for server log).
    let log_candidates = ["server.log", "melin-server.log"];
    for log_name in &log_candidates {
        let log_path = dir.join(log_name);
        if log_path.exists() {
            let stages = parse_pipeline_stats(&fs::read_to_string(&log_path).unwrap_or_default());
            if !stages.is_empty() {
                let pipe_path = dir.join("pipeline.svg");
                plot_pipeline(&stages, &pipe_path);
            }
            break;
        }
    }

    eprintln!("all plots generated in {}", dir.display());
}
