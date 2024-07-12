use std::time::Instant;
use std::{env, process};

use s3_benchrunner_rust::{BenchmarkConfig, BenchmarkRunner, TransferManagerRunner};

fn main() {
    let args: Vec<String> = env::args().collect();

    let [_, s3_client_id, workload, bucket, region, target_throughput_gigabits] = &args[..] else {
        eprintln!("usage: s3-benchrunner-rust S3_CLIENT WORKLOAD BUCKET REGION TARGET_THROUGHPUT");
        process::exit(1);
    };

    let target_throughput_gigabits: f64 = target_throughput_gigabits
        .parse()
        .expect("TARGET_THROUGHPUT should be a gigabits-per-sec float");

    let config = BenchmarkConfig::new(workload, bucket, region, target_throughput_gigabits);

    // create appropriate benchmark runner
    let runner: Box<dyn BenchmarkRunner> = match s3_client_id.as_str() {
        "sdk-rust-tm" => Box::new(TransferManagerRunner::new(config)),
        _ => panic!("Unknown S3_CLIENT: {s3_client_id}"),
    };

    let workload = &runner.config().workload;
    let bytes_per_run: u64 = workload.tasks.iter().map(|x| x.size).sum();
    let gigabits_per_run = ((bytes_per_run * 8) as f64) / 1_000_000_000.0;

    // repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    let app_start = Instant::now();
    for run_i in 0..workload.max_repeat_count {
        let run_start = Instant::now();

        runner.run();

        let run_secs = (Instant::now() - run_start).as_secs_f64();
        println!(
            "Run:{} Secs:{:.6} Gb/s:{:.6}",
            run_i + 1,
            run_secs,
            gigabits_per_run / run_secs
        );

        // break out if we've exceeded max_repeat_secs
        let app_secs = (Instant::now() - app_start).as_secs_f64();
        if app_secs >= workload.max_repeat_secs as f64 {
            break;
        }
    }
}
