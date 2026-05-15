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
    js_publish(js, "jobs.ready", string(id);
        stream="JOBS",
        msg_id="job-$id",
    )
end

stored = stream_message_get(js, "JOBS"; seq=1)
@info "first stored job" data=String(stored.data)

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

`msg_id` lets the server suppress duplicate publishes within the stream duplicate window. `stream_message_get` reads stored data by sequence when an application needs to inspect or replay messages before they are removed.

For long-running workers, fetch in a loop and stop on your service shutdown signal. Use `in_progress(msg)` for jobs that need longer than `ack_wait`.

To process a fetched batch concurrently, keep the same Natter calls and add a Julia task boundary:

```julia
msgs = fetch(worker, 20; timeout=2.0)

@sync for msg in msgs
    @async begin
        try
            job_id = parse(Int, String(msg.data))
            @info "processing job" job_id
            ack(msg)
        catch err
            nak(msg; delay=1.0)
        end
    end
end
```
