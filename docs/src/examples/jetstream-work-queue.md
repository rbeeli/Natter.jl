# JetStream Work Queue

This recipe creates a work-queue stream, publishes jobs with message IDs, and processes them with a durable pull consumer.

```julia
using Natter

client = connect("nats://127.0.0.1:4222"; name="job-worker")
js = jetstream(client)

stream_create(js, StreamConfig(
    name="JOBS",
    subjects=["jobs.ready"],
    retention=RetentionPolicy.WORK_QUEUE,
    storage=StorageType.FILE,
    max_msgs=-1,
    max_bytes=-1,
    duplicate_window=120.0,
))

for id in 1:100
    js_publish(js, "jobs.ready", string(id);
        stream="JOBS",
        msg_id="job-$id",
    )
end

worker = pull_subscribe(js, "jobs.ready";
    stream="JOBS",
    durable="job-workers",
    timeout=2.0,
    config=ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30.0,
        max_ack_pending=200,
    ),
)

try
    for msg in fetch(worker, 20; timeout=2.0)
        try
            job_id = parse(Int, String(msg))
            process_job(job_id)
            ack(msg)
        catch err
            @error "job failed" exception=err
            nak(msg; delay=1.0)
        end
    end
finally
    close(worker)
    close(client)
end
```

`msg_id` lets the server suppress duplicate publishes within the stream duplicate window. Use it for jobs where publish retries or reconnect ambiguity could otherwise create duplicate work.

Inspect stored data when an operator or replay tool needs it:

```julia
stored = stream_message_get(js, "JOBS"; seq=1)
@info "first stored job" data=String(stored)
```

Process a fetched batch concurrently when handlers are I/O-heavy:

```julia
msgs = fetch(worker, 20; timeout=2.0)

@sync for msg in msgs
    @async begin
        try
            job_id = parse(Int, String(msg))
            process_job(job_id)
            ack(msg)
        catch err
            @error "job failed" exception=err
            nak(msg; delay=1.0)
        end
    end
end
```

For long-running workers, fetch in a loop or use `messages(worker; ...)` to keep a bounded stream refilled. Call `in_progress(msg)` for work that may exceed the consumer `ack_wait`.
