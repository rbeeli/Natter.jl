# JetStream Work Queue

This example creates a work queue stream, publishes jobs, and processes them with a durable pull consumer.

```julia
using Natter

client = connect("nats://127.0.0.1:4222"; name="worker")
js = jetstream(client)

stream_create(js, StreamConfig(
    name="JOBS",
    subjects=["jobs.ready"],
    retention=RetentionPolicy.WORK_QUEUE,
    storage=StorageType.FILE,
    max_msgs=-1,
    max_bytes=-1,
    max_consumers=-1,
))

for id in 1:100
    js_publish(js, "jobs.ready", string(id); stream="JOBS")
end

worker = pull_subscribe(js, "jobs.ready";
    stream="JOBS",
    durable="job-workers",
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30.0,
        max_ack_pending=200,
    ),
)

for msg in fetch(worker, 20; timeout=2.0)
    try
        job_id = parse(Int, String(msg.data))
        @info "processing job" job_id
        ack(msg)
    catch err
        nak(msg; delay=1.0)
    end
end

close(worker)
close(client)
```

For long-running workers, fetch in a loop and stop on your service shutdown signal. Use `in_progress(msg)` for jobs that need longer than `ack_wait`.
