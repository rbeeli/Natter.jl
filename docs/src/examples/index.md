# Examples

These recipes are meant to be copied into real services and adjusted. They avoid framework-specific code and use normal Julia task concurrency.

| Recipe | Shows |
| :--- | :--- |
| [Connection Auth And TLS](connection-auth-tls.md) | Token, user/password, NKEY, JWT, `.creds`, TLS, and mTLS connection snippets. |
| [Basic Publish And Subscribe](basic-pub-sub.md) | Channel subscriptions, callback subscriptions, queue groups, headers, and flush. |
| [Request Reply Service](request-reply.md) | A small service endpoint, client requests, no-responder handling, and concurrent requests. |
| [JetStream Work Queue](jetstream-work-queue.md) | Stream setup, idempotent publish, durable pull worker, ack/nak, and concurrent batch processing. |
| [KeyValue Store](keyvalue-store.md) | Bucket setup, optimistic writes, reads, history, watches, and concurrent reads. |
| [Production Client](production-client.md) | Multi-server TLS connection, callbacks, JetStream worker setup, retry-safe publish, and graceful shutdown. |

For the shortest path through the docs, read [Getting Started](../getting-started.md), then the recipe closest to your application.
