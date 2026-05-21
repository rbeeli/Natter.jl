use async_nats::Subject;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::json;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn percentile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((values.len() - 1) as f64) * q).round() as usize;
    values[idx.min(values.len() - 1)]
}

async fn wait_for<F>(mut predicate: F, timeout: Duration) -> Result<(), String>
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err("timed out waiting for benchmark condition".to_string())
}

async fn timed_repeated<F, Fut>(
    unit_count: usize,
    min_seconds: f64,
    mut run_round: F,
) -> Result<(usize, usize, f64), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let mut units = 0usize;
    let mut rounds = 0usize;
    let started = Instant::now();
    loop {
        run_round().await?;
        units += unit_count;
        rounds += 1;
        let seconds = started.elapsed().as_secs_f64();
        if seconds >= min_seconds {
            return Ok((units, rounds, seconds));
        }
    }
}

async fn bench_publish_batch(
    url: &str,
    subject: &str,
    payload: Bytes,
    messages: usize,
    min_seconds: f64,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = async_nats::connect(url).await?;
    let subject = Subject::from(subject.to_owned());
    let warmup = messages.min(1000);
    for _ in 0..warmup {
        client.publish(subject.clone(), payload.clone()).await?;
    }
    client.flush().await?;

    let (total_messages, rounds, seconds) = timed_repeated(messages, min_seconds, || {
        let client = client.clone();
        let subject = subject.clone();
        let payload = payload.clone();
        async move {
            for _ in 0..messages {
                client.publish(subject.clone(), payload.clone()).await?;
            }
            client.flush().await?;
            Ok(())
        }
    })
    .await?;
    Ok(json!({
        "messages": total_messages,
        "configured_messages": messages,
        "rounds": rounds,
        "seconds": seconds,
        "messages_per_second": total_messages as f64 / seconds,
        "payload_mib_per_second": total_messages as f64 * payload.len() as f64 / seconds / 1024.0 / 1024.0,
    }))
}

async fn bench_publish_flush_each(
    url: &str,
    subject: &str,
    payload: Bytes,
    messages: usize,
    min_seconds: f64,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = async_nats::connect(url).await?;
    let subject = Subject::from(subject.to_owned());
    let warmup = messages.min(100);
    for _ in 0..warmup {
        client.publish(subject.clone(), payload.clone()).await?;
        client.flush().await?;
    }

    let (total_messages, rounds, seconds) = timed_repeated(messages, min_seconds, || {
        let client = client.clone();
        let subject = subject.clone();
        let payload = payload.clone();
        async move {
            for _ in 0..messages {
                client.publish(subject.clone(), payload.clone()).await?;
                client.flush().await?;
            }
            Ok(())
        }
    })
    .await?;
    Ok(json!({
        "messages": total_messages,
        "configured_messages": messages,
        "rounds": rounds,
        "seconds": seconds,
        "messages_per_second": total_messages as f64 / seconds,
        "payload_mib_per_second": total_messages as f64 * payload.len() as f64 / seconds / 1024.0 / 1024.0,
    }))
}

async fn bench_callback_dispatch(
    url: &str,
    subject: &str,
    payload: Bytes,
    messages: usize,
    timeout: Duration,
    min_seconds: f64,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let sub_client = async_nats::connect(url).await?;
    let pub_client = async_nats::connect(url).await?;
    let subject = Subject::from(subject.to_owned());
    let mut sub = sub_client.subscribe(subject.clone()).await?;
    sub_client.flush().await?;

    let received = Arc::new(AtomicUsize::new(0));
    let task_received = received.clone();
    tokio::spawn(async move {
        while sub.next().await.is_some() {
            task_received.fetch_add(1, Ordering::Relaxed);
        }
    });

    let warmup = messages.min(1000);
    for _ in 0..warmup {
        pub_client.publish(subject.clone(), payload.clone()).await?;
    }
    pub_client.flush().await?;
    wait_for(|| received.load(Ordering::Relaxed) >= warmup, timeout).await?;
    let mut target = received.load(Ordering::Relaxed);

    let (total_messages, rounds, seconds) = timed_repeated(messages, min_seconds, || {
        let pub_client = pub_client.clone();
        let subject = subject.clone();
        let payload = payload.clone();
        let received = received.clone();
        target += messages;
        let round_target = target;
        async move {
            for _ in 0..messages {
                pub_client.publish(subject.clone(), payload.clone()).await?;
            }
            pub_client.flush().await?;
            wait_for(|| received.load(Ordering::Relaxed) >= round_target, timeout).await?;
            Ok(())
        }
    })
    .await?;
    Ok(json!({
        "messages": total_messages,
        "configured_messages": messages,
        "received": total_messages,
        "rounds": rounds,
        "seconds": seconds,
        "messages_per_second": total_messages as f64 / seconds,
    }))
}

async fn bench_request_reply(
    url: &str,
    subject: &str,
    payload: Bytes,
    requests: usize,
    min_seconds: f64,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let service = async_nats::connect(url).await?;
    let client = async_nats::connect(url).await?;
    let subject = Subject::from(subject.to_owned());
    let mut sub = service.subscribe(subject.clone()).await?;
    service.flush().await?;

    let service_payload = payload.clone();
    tokio::spawn(async move {
        while let Some(request) = sub.next().await {
            if let Some(reply) = request.reply {
                let _ = service.publish(reply, service_payload.clone()).await;
            }
        }
    });

    let warmup = requests.min(200);
    for _ in 0..warmup {
        client.request(subject.clone(), payload.clone()).await?;
    }

    let mut latencies = Vec::with_capacity(requests);
    let mut total_requests = 0usize;
    let mut rounds = 0usize;
    let started = Instant::now();
    loop {
        for _ in 0..requests {
            let request_started = Instant::now();
            client.request(subject.clone(), payload.clone()).await?;
            latencies.push(request_started.elapsed().as_secs_f64() * 1000.0);
        }
        total_requests += requests;
        rounds += 1;
        if started.elapsed().as_secs_f64() >= min_seconds {
            break;
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let mut p50 = latencies.clone();
    let mut p95 = latencies.clone();
    let mut p99 = latencies.clone();
    let max = latencies.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Ok(json!({
        "requests": total_requests,
        "configured_requests": requests,
        "rounds": rounds,
        "seconds": seconds,
        "requests_per_second": total_requests as f64 / seconds,
        "latency_ms_mean": mean,
        "latency_ms_p50": percentile(&mut p50, 0.50),
        "latency_ms_p95": percentile(&mut p95, 0.95),
        "latency_ms_p99": percentile(&mut p99, 0.99),
        "latency_ms_max": max,
    }))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let url = arg_value(&args, "--url", "nats://127.0.0.1:4222");
    let messages = arg_value(&args, "--messages", "200000").parse::<usize>()?;
    let requests = arg_value(&args, "--requests", "20000").parse::<usize>()?;
    let payload_bytes = arg_value(&args, "--payload-bytes", "64").parse::<usize>()?;
    let timeout = Duration::from_secs_f64(arg_value(&args, "--timeout", "90").parse::<f64>()?);
    let min_seconds = arg_value(&args, "--min-seconds", "5.0").parse::<f64>()?;
    if min_seconds <= 0.0 {
        return Err("--min-seconds must be positive".into());
    }
    let json_path = arg_value(&args, "--json", "/tmp/rust-nats.json");
    let payload = Bytes::from(vec![b'x'; payload_bytes]);
    let prefix = format!(
        "rust.perf.{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos()
    );

    let benchmarks = json!({
        "publish_batch": bench_publish_batch(&url, &format!("{prefix}.publish.batch"), payload.clone(), messages, min_seconds).await?,
        "publish_flush_each": bench_publish_flush_each(&url, &format!("{prefix}.publish.flush"), payload.clone(), messages, min_seconds).await?,
        "callback_dispatch": bench_callback_dispatch(&url, &format!("{prefix}.callback"), payload.clone(), messages, timeout, min_seconds).await?,
        "request_reply": bench_request_reply(&url, &format!("{prefix}.request"), payload, requests, min_seconds).await?,
    });
    let report = json!({
        "generated_at": format!("{:?}", SystemTime::now()),
        "environment": {
            "client": "async-nats",
            "url": url,
            "messages": messages.to_string(),
            "requests": requests.to_string(),
            "payload_bytes": payload_bytes.to_string(),
            "min_seconds": min_seconds.to_string(),
            "flush_semantics": "client_flush",
        },
        "benchmarks": benchmarks,
    });
    std::fs::write(&json_path, format!("{}\n", serde_json::to_string_pretty(&report)?))?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
