# A1 - TODO

**Discussion**: What networking/socket API(s) do you think `NewClient(...)` call corresponds to or is similar to? (For reference, you can find example socket APIs [here](https://en.wikipedia.org/wiki/Berkeley_sockets#Socket_API_functions) or in the slides for Lecture 3.) Include your responses under a heading **A1** in `discussions.md`.

# A2 - TODO

**Discussion**: In what cases do you think the `NewClient()` call will fail? What status code do you think you should return for these cases? Include your responses and reasoning (why you picked the particular code) under a heading **A2** in `discussions.md`.

# A3 - TODO

**Discussion**: What networking/system calls do you think will be used under the hood when you call `GetUser()`? What cases do you think this call / these calls can return an error? You may find the slides from Lecture 3 helpful. Could `GetUser` return errors in cases where the network calls succeed? Include your responses under a heading **A3** in `discussions.md`.

# ExtraCredit1 - TODO

**ExtraCredit1**: How are these errors detected? Include your responses under a heading `ExtraCredit1` in `discussions.md`.

# A4

It would not work because TCP connections are tied to a single destination address. In this case, UserService and VideoService servers have different addresses, so one connection cannot support both. While one connection object can support multiplexing, if we tried to do this here, every request to the VideoService server will arrive at the UserService server's process, which wouldn't know how to respond (grpc would just return codes.Unimplemented).

# A6

Name: Lueilwitz9267
Top Recommended Video: Video id=1163, title="glamorous off", author=Baron Langworth, url=https://video-data.localhost/blob/1163

# A8

Sending batched requests concurrently can be useful by providing lower latency per request. For this service, however, the documentation says that the ranker code dominates latency, so the complexity that comes with handling errors in concurrent batches far outweighs the benefits. Additionally, sending batches concurrently means that each server now gets N simultaneous requests instead of 1 request at a time, which increases pressure on the servers under load spikes and partly undoes the benefit of capping the max batch size. For those reasons, I chose to stick with the sequential implementation.

# ExtraCredit2 - TODO

**ExtraCredit2**: Assume that handling one batch request is cheap up to the batch size limit. How could you reduce the total number of requests to `UserService` and `VideoService` using batching, assuming you have many incoming requests to `VideoRecService`? Describe how you would implement this at a high-level (bullet points and pseudocode are fine) but you do not need to implement it in your service. Include your responses under a heading `ExtraCredit2` in `discussions.md`.

# B2 + ExtraCredit3

|now_us|total_requests|total_errors|active_requests|user_service_errors|video_service_errors|average_latency_ms|p99_latency_ms|stale_responses|
|------|--------------|------------|---------------|-------------------|--------------------|------------------|--------------|---------------|
|1790544636245285|        0|       0|       0|       0|       0|       0.00|    0.00|    0|
|1790544637245805|        15|      0|       1|       0|       0|       66.78|   96.57|   0|
|1790544638245172|        25|      0|       1|       0|       0|       56.82|   96.57|   0|
|1790544639245545|        35|      0|       1|       0|       0|       52.82|   96.57|   0|
|1790544640245017|        45|      0|       1|       0|       0|       56.04|   96.57|   0|
|1790544641245438|        55|      0|       1|       0|       0|       59.62|   96.57|   0|
|1790544642244995|        65|      0|       1|       0|       0|       60.55|   96.57|   0|
|1790544643245550|        75|      0|       1|       0|       0|       61.13|   96.56|   0|
|1790544644245113|        85|      0|       1|       0|       0|       60.80|   96.56|   0|
|1790544645245252|        95|      0|       1|       0|       0|       62.16|   96.55|   0|
|1790544646245709|        105|     0|       1|       0|       0|       62.95|   96.55|   0|
|1790544647244976|        115|     0|       1|       0|       0|       63.41|   96.54|   0|

# C1 - TODO

**Discussion**: Why might retrying be a bad option? In what cases should you *not* retry a request? Add your response under heading **C1** in `discussions.md`.

# C2 - TODO

**Discussion**: What should you do if `VideoService` is still down and your responses are past expiration? Return expired responses or an error? What are the tradeoffs?

For this lab, prefer to return expired responses over an error if you can cache at least one
successful set of trending videos. Note down the tradeoffs of the strategies under heading **C2**
in `discussions.md`.

# C3 - TODO

Name at least one additional strategy you could use to improve the reliability
of `VideoRecService` (successful, and useful responses) in the event of failures of `UserService`
or `VideoService`. What tradeoffs would you make and why? Add your response under heading **C3** in `discussions.md`.

# C4 - TODO