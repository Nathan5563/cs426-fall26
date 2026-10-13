# UUID

A UUID is a 128 bit integer typically represented by 32 hex digits. Four bits are reserved for the version number and two for the variant, leaving 122 bits for content. Standard libraries in Go, Python, and Java default to version 4, whose RFC says implementors should fill the 122 bits from a cryptographically secure random source and set the version/variant bits. The "unique" in UUID is probabilistic, however. With 2^122 possible values, about 2^61 UUIDs must be generated before a collision becomes 50 percent likely, so it's statistically insignificant relative to other causes of failure in a system.

# B1

I figured that deleting the pod should trigger a graceful shutdown via respectful signals like SIGTERM rather than an immediate SIGKILL. Because the pod is owned by a deployment with `replicas: 1`, Kubernetes should bring up a replacement soon after.

```
$ kubectl delete pod video-rec-79b89cd977-b4m2c
pod "video-rec-79b89cd977-b4m2c" deleted from naa67 namespace

# Immediately after
$ kubectl get pod
NAME                         READY   STATUS              RESTARTS   AGE
video-rec-79b89cd977-2mhbt   0/1     ContainerCreating   0          0s

# After a few seconds
$ kubectl get pod
NAME                         READY   STATUS    RESTARTS   AGE
video-rec-79b89cd977-2mhbt   1/1     Running   0          3s
```

Kubernetes did indeed bring up a replacement relatively quickly. The deployment's controller saw zero live pods and a desired count of one, so it created a new one. The matching hash shows it came from the same controller.

# B4

Welcome! You have chosen user ID 201514 (Hauck4457/vickyweimann@reilly.io)

Their recommended videos are:
 1. disturbed Sugar Snap Peas by Toy Orn
 2. amused upstairs by Hardy Wunsch
 3. unusual in by Cornelius Moore
 4. The terse serval's trust by Ashtyn Zulauf
 5. Sharksew: bypass by Geraldine Grady

# C3

With `--client-pool-size=4`, both backing pods of each service received traffic, but happened to receive it unevenly: about 3/4 of calls went to one pod and 1/4 to the other for both services. Each gRPC connection is a TCP connection, and kube-proxy picks a backend independently at random when each connection is opened, so 4 connections over 2 pods is binomially distributed. A 2-2 split happens 6/16 of the time, 3-1 happens 8/16, and 4-0 happens 2/16.

With 1 client, 100 percent of the traffic goes to a single random pod per service for the life of the connection. With 8 clients, the expected split is closer to even, but individual draws are still random: I saw UserService come out 4-4, while VideoService came out 7-1. The key is that a connection pool only spreads load statistically, and per-request dynamic load balancing sits on L7-aware logic.

# D1

- In iptables mode, kube-proxy turns the service into a chain of jumps in the nat table. `KUBE-SERVICES` matches the ClusterIP and port and jumps to the service's `KUBE-SVC-*` chain.
- That chain lists one rule per endpoint, each guarded by `-m statistic --mode random --probability p`.
  - With two endpoints, the first rule fires with probability 0.5 and the second is unconditional, so each packet that reaches the chain is sent to a uniformly random `KUBE-SEP-*` chain.
  - The relevant `KUBE-SEP-*` chain then DNATs the destination from ClusterIP:80 to that pod's IP:8080. The rules are stateless, but DNAT is a conntrack operation, and the nat table is only consulted for the first packet of a connection. Once that packet has been DNATed, conntrack records the (src, dst, ports) mapping and silently applies the same rewrite to every later packet of the flow without re-evaluating the chain.
  - The random choice is therefore made once per connection, not per packet, and the connection stays pinned to that pod until it closes. This is why one gRPC `ClientConn` in part C always talked to one backend pod, and why new connections spread across both pods.
- The nftables backend expresses the same decision with lookups instead of walking the chain of rules. The `services` chain does one verdict-map lookup of `ip daddr . meta l4proto . th dport vmap @service-ips`.
- The service chain is a single rule: `dnat ip to numgen random mod 2 map { 0 : 10.244.0.2 . 8080, 1 : 10.244.0.6 . 8080 }`.
  - A random number generator picks an index, an inline map converts it to an endpoint, and a DNAT follows immediately.
  - Backend selection is still random per connection and conntrack still saves and sends additional packets to the same pod, so the observable behaviour is identical.
  - In comparison to iptables, this only decreases the number of rules evaluated per packet and makes rule changes more efficient if we want to e.g., add more pods.


# D2

- `resolv.conf`
  - `nameserver 34.118.224.10`: this is the cluster DNS server every pod queries.
  - `search naa67.svc.cluster.local svc.cluster.local cluster.local us-central1-b.c.cs-426-fall-2026.internal c.cs-426-fall-2026.internal google.internal`: these are suffixes the resolver tries adding to the short name, in order separated by spaces, before trying the name as-is.
  - `options ndots:5`: a name with fewer than 5 dots counts as short and thus the resolver tries suffixes before trying the name alone.
- `user-service.default` is short (fewer than 5 dots by above rule), so the resolver tries `user-service.default.naa67.svc.cluster.local` (fails), then `user-service.default.svc.cluster.local`, which works because it is the real name of the service in `default`.
- `example.com` is also short (fewer than 5 dots by above rule), so all six endings are tried and fail before `example.com` itself, which works. This causes up to 7 queries for one lookup.
- For SRV, we get `_grpc._tcp.video-rec.naa67.svc.cluster.local. 30 IN SRV 10 100 80 video-rec.naa67.svc.cluster.local.`:
  - The first field is port name `grpc` + protocol `tcp` + service `video-rec.naa67.svc.cluster.local`.
  - The second field is cache seconds `30` + priority `10` + weight `100` + port `80`, then the hostname `video-rec.naa67.svc.cluster.local` to connect to. It names the service, not the pods, because a ClusterIP service hands out one virtual IP and kube-proxy picks the pod per connection by the above rules in iptables/nftables.
- `dig -x 10.120.1.112` returns `10-120-1-112.video-rec.naa67.svc.cluster.local.`: the pod IP, its service, and its namespace. A monitor that only sees a caller's IP can reverse it to find the caller's namespace, which is how Grafana attributes traffic to `naa67`.

# D3

1. My pod resolves `user-service.default.svc.cluster.local` via the search path to the ClusterIP `34.118.237.27`. It opens a TCP connection to that IP on port 8081. On my node, kube-proxy's nat rules match the ClusterIP and port, pick one UserService pod at random, and DNAT the destination to that pod's IP. Conntrack records the mapping so every later packet on that connection is rewritten the same way without re-running the rules. The choice is per connection, which is why pool size 1 pinned all traffic to one pod, pool size 4 gave a random split following the binomial distribution Bin(4, 0.5). Similarly, pool size 8 gave a random sample of Bin(8, 0.5).
2. The DNS names and search path, ClusterIPs, kube-proxy iptables rules, DNAT plus conntrack, and the per-connection behaviour all stay the same. However, the VPC CNI gives each pod a real VPC IP from the node's subnet instead of an overlay range, so a packet to a pod on another node is routed by the VPC itself with no encapsulation.
3. The ClusterIP to pod translation happens in eBPF programs attached at the socket layer (for pod-originated traffic, at `connect()` time) or at the network interface, not in netfilter. You would inspect it with `cilium service list` and `cilium bpf lb list` instead of `iptables-save`. The single-connection behavior does not change: the translation is still done once when the connection is opened, and the connection stays on that pod.
4. With `clusterIP: None`, the A query for the service returns every pod IP instead of one virtual IP, and the SRV query returns one record per pod. A gRPC client can use the `dns:///` resolver to get that list and apply its own round robin policy, opening one connection per pod and spreading requests across them, instead of relying on a pool of statistically distributed connections to a single IP.
5. iptables rules are a linear list, so lookup cost per new connection grows with the number of services and endpoints. Moreover, every endpoint change rewrites and reloads the whole ruleset. I imagine this can take several seconds at tens of thousands of rules and stalls traffic. nftables uses hash-map lookups so matching is O(1), and updates only change/reload single map entries. eBPF goes further by doing the lookup in a kernel hash map at the socket layer, so the packet never carries the ClusterIP at all and no NAT state is needed.
