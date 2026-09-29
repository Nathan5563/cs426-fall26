# A1

The `NewClient(...)` call seems to correspond to `socket()` and address resolution setup, but I doubt it does any network I/O by itself, as it would be more efficient to just set this up and defer connection until the RPC is called. After some research, I found that this is indeed true.

# A2

Since `NewClient(...)` does not do any network I/O, it can only fail on configuration issues. For example, malformed addresses, no transport credentials nor insecure credentials, etc., which are all errors caused by internal failures and have nothing to do with the client RPC caller. Thus, I chose `codes.Internal`.

# A3

Since `GetUser(...)` is a gRPC call, it likely uses the previously-setup connection object, `connect`s to it if not already connected, then `send`s and `recv`s data through the socket. These can return errors if the connection fails to open, if the send/recv fail to write/read, the server isn't listening, etc. Even if all network calls succeed, `GetUser(...)` may fail if the network calls return data of a user who doesn't exist, at which point the gRPC call fails with a specific error code.

# ExtraCredit1

Failing syscalls return -1 and put the failure cause in the thread-local global variable `errno`, which Go provides as an `error` type. At the gRPC level, failures are easily detected by the status code.

# A4

It would not work because TCP connections are tied to a single destination address. In this case, UserService and VideoService servers have different addresses, so one connection cannot support both. While one connection object can support multiplexing, if we tried to do this here, every request to the VideoService server will arrive at the UserService server's process, which wouldn't know how to respond (grpc would just return codes.Unimplemented).

# A6

Name: Lueilwitz9267
Top Recommended Video: Video id=1163, title="glamorous off", author=Baron Langworth, url=https://video-data.localhost/blob/1163

# A8

Sending batched requests concurrently can be useful by providing lower latency per request. For this service, however, the documentation says that the ranker code dominates latency, so the complexity that comes with handling errors in concurrent batches far outweighs the benefits. Additionally, sending batches concurrently means that each server now gets N simultaneous requests instead of 1 request at a time, which increases pressure on the servers under load spikes and partly undoes the benefit of capping the max batch size. For those reasons, I chose to stick with the sequential implementation.

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

# C1

If a backend is failing because it's overwhelmed, retrying only compounds this effect. Moreover, retrying when the backend services return errors corresponding to non-retryable failures is not helpful. One example of a retryable error code is `codes.Unavailable`, whereas an example of a non-retryable error code is `codes.PermissionDenied`.

# C2

As described in the instructions, I chose to keep serving the expired trending videos, and log a stale response in the statistics, as long as at least one refresh has ever succeeded (i.e., the cache is non-empty). A list of former trending videos, whose age is O(downtime), is still a more useful set of recommendations than an error. On the other hand, if the outage is very long, the data can be arbitrarily out-of-date and the videos may have been deleted. Simply returning errors is truthful, so it has its benefits there. A mitigation to the stale response case may be to set a timer and start returning errors once the cache's contents are considered too old.

# C3

If `UserService` fails, there's not much we can do since we know nothing about the user and the videos they would like, so resorting to the trending videos from the cache is preferred. If `VideoService` fails, we can try more interesting strategies. For example, we can cache (userCoefficients, videoInfos) pairs from previous responses, then return the videos recommended for the most recent user who is most similar to the calling user, per the ranking algorithm. This is helpful since it provides more useful responses than globally trending videos. Of course, the tradeoff here is more complexity, storage, and latency.

# C4

Since `NewClient(...)` is lazy in connecting/doing any network I/O, in high-throughput cases we create many connections and all of them incur a high cost on the first RPC (TCP handshake, HTTP setup, new file descriptor, new port). To avoid this, we can create shared connections once and store them in the server, and use them for every incoming request. The downside of this is that we are connected to single instances of the servers, whereas creating connections on each request allows the backend to route the connections to the appropriate instances for load balancing.
