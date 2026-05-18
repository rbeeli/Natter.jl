"""
    *_async(...)

Run the matching synchronous operation in a Julia task and return a `NatterTask`.
Use `fetch(handle)` to get the same return value as the synchronous operation, or
to throw the original operation exception. These helpers intentionally share the
synchronous implementation so reconnect, cleanup, validation, and timeout
behavior stay identical across both API styles.
"""
struct NatterTask
    task::Task
end

function _task_failure(task::Task)
    exceptions = Base.current_exceptions(task)
    isempty(exceptions) ? nothing : last(exceptions)
end

function _throw_task_failure(task::Task)
    failure = _task_failure(task)
    if isnothing(failure)
        throw(TaskFailedException(task))
    end
    throw(failure.exception)
end

function _wait_task_done(task::Task)
    try
        wait(task)
    catch err
        err isa TaskFailedException && err.task === task || rethrow()
    end
    task
end

function Base.wait(handle::NatterTask)
    _wait_task_done(handle.task)
    if istaskfailed(handle.task)
        _throw_task_failure(handle.task)
    end
    handle
end

function Base.fetch(handle::NatterTask)
    wait(handle)
    fetch(handle.task)
end

Base.istaskdone(handle::NatterTask) = istaskdone(handle.task)
Base.istaskfailed(handle::NatterTask) = istaskfailed(handle.task)
Base.show(io::IO, handle::NatterTask) = print(io, "NatterTask(", handle.task, ")")

function _natter_async(f::F, args...; kwargs...)::NatterTask where {F}
    NatterTask(@async f(args...; kwargs...))
end

connect_async(url_or_urls=nothing; kwargs...)::NatterTask = _natter_async(connect, url_or_urls; kwargs...)

publish_async(client::Client, subject::AbstractString, data=nothing; kwargs...)::NatterTask =
    _natter_async(publish, client, subject, data; kwargs...)

publish_async(client::Client, frame::PublishFrame; kwargs...)::NatterTask =
    _natter_async(publish, client, frame; kwargs...)

subscribe_async(client::Client, subject::AbstractString; kwargs...)::NatterTask =
    _natter_async(subscribe, client, subject; kwargs...)

subscribe_async(callback, client::Client, subject::AbstractString; kwargs...)::NatterTask =
    _natter_async(subscribe, callback, client, subject; kwargs...)

unsubscribe_async(sub::Subscription; kwargs...)::NatterTask =
    _natter_async(unsubscribe, sub; kwargs...)

next_async(sub::Subscription; kwargs...)::NatterTask =
    _natter_async(next, sub; kwargs...)

next_async(psub::PushSubscription; kwargs...)::NatterTask =
    _natter_async(next, psub; kwargs...)

request_async(client::Client, subject::AbstractString, data=nothing; kwargs...)::NatterTask =
    _natter_async(request, client, subject, data; kwargs...)

flush_async(client::Client; kwargs...)::NatterTask =
    _natter_async(flush, client; kwargs...)

ping_async(client::Client; kwargs...)::NatterTask =
    _natter_async(ping, client; kwargs...)

drain_async(client::Client; kwargs...)::NatterTask =
    _natter_async(drain, client; kwargs...)

drain_async(sub::Subscription; kwargs...)::NatterTask =
    _natter_async(drain, sub; kwargs...)

close_async(client::Client; kwargs...)::NatterTask =
    _natter_async(close, client; kwargs...)

close_async(sub::Subscription; kwargs...)::NatterTask =
    _natter_async(close, sub; kwargs...)

js_publish_async_complete_async(js::JetStreamContext; kwargs...)::NatterTask =
    _natter_async(js_publish_async_complete, js; kwargs...)

stream_create_async(js::JetStreamContext, config; kwargs...)::NatterTask =
    _natter_async(stream_create, js, config; kwargs...)

stream_update_async(js::JetStreamContext, config; kwargs...)::NatterTask =
    _natter_async(stream_update, js, config; kwargs...)

stream_info_async(js::JetStreamContext, name::AbstractString; kwargs...)::NatterTask =
    _natter_async(stream_info, js, name; kwargs...)

stream_list_async(js::JetStreamContext; kwargs...)::NatterTask =
    _natter_async(stream_list, js; kwargs...)

stream_list_page_async(js::JetStreamContext; kwargs...)::NatterTask =
    _natter_async(stream_list_page, js; kwargs...)

stream_names_async(js::JetStreamContext; kwargs...)::NatterTask =
    _natter_async(stream_names, js; kwargs...)

stream_names_page_async(js::JetStreamContext; kwargs...)::NatterTask =
    _natter_async(stream_names_page, js; kwargs...)

stream_purge_async(js::JetStreamContext, name::AbstractString; kwargs...)::NatterTask =
    _natter_async(stream_purge, js, name; kwargs...)

stream_delete_async(js::JetStreamContext, name::AbstractString; kwargs...)::NatterTask =
    _natter_async(stream_delete, js, name; kwargs...)

stream_message_get_async(js::JetStreamContext, stream::AbstractString; kwargs...)::NatterTask =
    _natter_async(stream_message_get, js, stream; kwargs...)

stream_message_delete_async(js::JetStreamContext, stream::AbstractString, seq; kwargs...)::NatterTask =
    _natter_async(stream_message_delete, js, stream, seq; kwargs...)

consumer_create_async(js::JetStreamContext, stream::AbstractString, config; kwargs...)::NatterTask =
    _natter_async(consumer_create, js, stream, config; kwargs...)

consumer_create_or_update_async(js::JetStreamContext, stream::AbstractString, config; kwargs...)::NatterTask =
    _natter_async(consumer_create_or_update, js, stream, config; kwargs...)

consumer_update_async(js::JetStreamContext, stream::AbstractString, config; kwargs...)::NatterTask =
    _natter_async(consumer_update, js, stream, config; kwargs...)

consumer_info_async(js::JetStreamContext, stream::AbstractString, consumer::AbstractString; kwargs...)::NatterTask =
    _natter_async(consumer_info, js, stream, consumer; kwargs...)

consumer_list_async(js::JetStreamContext, stream::AbstractString; kwargs...)::NatterTask =
    _natter_async(consumer_list, js, stream; kwargs...)

consumer_list_page_async(js::JetStreamContext, stream::AbstractString; kwargs...)::NatterTask =
    _natter_async(consumer_list_page, js, stream; kwargs...)

consumer_delete_async(js::JetStreamContext, stream::AbstractString, consumer::AbstractString; kwargs...)::NatterTask =
    _natter_async(consumer_delete, js, stream, consumer; kwargs...)

pull_subscribe_async(js::JetStreamContext, subject::AbstractString; kwargs...)::NatterTask =
    _natter_async(pull_subscribe, js, subject; kwargs...)

push_subscribe_async(js::JetStreamContext, subject::AbstractString; kwargs...)::NatterTask =
    _natter_async(push_subscribe, js, subject; kwargs...)

fetch_async(psub::PullSubscription, batch=1; kwargs...)::NatterTask =
    _natter_async(fetch, psub, batch; kwargs...)

close_async(psub::PullSubscription; kwargs...)::NatterTask =
    _natter_async(close, psub; kwargs...)

close_async(stream::PullMessageStream; kwargs...)::NatterTask =
    _natter_async(close, stream; kwargs...)

close_async(psub::PushSubscription; kwargs...)::NatterTask =
    _natter_async(close, psub; kwargs...)

close_async(watcher::KeyValueWatcher; kwargs...)::NatterTask =
    _natter_async(close, watcher; kwargs...)

ack_async(msg::AbstractJetStreamMsg; kwargs...)::NatterTask =
    _natter_async(ack, msg; kwargs...)

ack_sync_async(msg::AbstractJetStreamMsg; kwargs...)::NatterTask =
    _natter_async(ack_sync, msg; kwargs...)

nak_async(msg::AbstractJetStreamMsg; kwargs...)::NatterTask =
    _natter_async(nak, msg; kwargs...)

in_progress_async(msg::AbstractJetStreamMsg; kwargs...)::NatterTask =
    _natter_async(in_progress, msg; kwargs...)

term_async(msg::AbstractJetStreamMsg; kwargs...)::NatterTask =
    _natter_async(term, msg; kwargs...)

kv_create_async(js::JetStreamContext, bucket::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_create, js, bucket; kwargs...)

kv_open_async(js::JetStreamContext, bucket::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_open, js, bucket; kwargs...)

kv_delete_bucket_async(kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_delete_bucket, kv; kwargs...)

kv_status_async(kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_status, kv; kwargs...)

kv_get_async(kv::KeyValue, key::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_get, kv, key; kwargs...)

kv_put_async(kv::KeyValue, key::AbstractString, value; kwargs...)::NatterTask =
    _natter_async(kv_put, kv, key, value; kwargs...)

kv_create_key_async(kv::KeyValue, key::AbstractString, value; kwargs...)::NatterTask =
    _natter_async(kv_create_key, kv, key, value; kwargs...)

kv_update_async(kv::KeyValue, key::AbstractString, value, revision; kwargs...)::NatterTask =
    _natter_async(kv_update, kv, key, value, revision; kwargs...)

kv_delete_async(kv::KeyValue, key::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_delete, kv, key; kwargs...)

kv_purge_async(kv::KeyValue, key::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_purge, kv, key; kwargs...)

kv_history_async(kv::KeyValue, key::AbstractString; kwargs...)::NatterTask =
    _natter_async(kv_history, kv, key; kwargs...)

kv_keys_async(kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_keys, kv; kwargs...)

kv_watch_async(kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_watch, kv; kwargs...)

kv_watch_async(callback, kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_watch, callback, kv; kwargs...)

kv_purge_deletes_async(kv::KeyValue; kwargs...)::NatterTask =
    _natter_async(kv_purge_deletes, kv; kwargs...)
