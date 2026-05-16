Do a full deep review of this pure Julia NATS client. Look for performance issues, type stability issues, unneccessary allocations, easy to do optimizations, unneccessary branchy code etc. This should be a fast and efficient library, especially on the hot path. Advise then on how to improve/fix the found issues. Only report major findings, but report all of them.

---

Do a full review of the Natter.jl library. Review for correctness, sufficient coverage of the most important/most used features, using idiomatic Julia with clean APIs and naming. Using type stable, type annotated and efficient code with few allocations and only required locking. Compare to other major NATS client libraries like from Go, Python, Rust and C to judge the quality of this one you can find the repos of those client libraries inside folder /mnt/data/repos/. Be critical. Object Store, WebSockets and Services/Micro features are expicitliy not wanted currently, so skip those and do not mention them.

---

Proceed with implementation. use clean code, clear separation of concerns, type stable functions and data structures. do not maintain backwards compatibility in the Natter.jl public API, just refactor to the best solution.
