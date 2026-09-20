# Pooled Host cloud example

`pooled-cloud.yaml` is the Host-owned part of R2.2. It defines a two-replica
Deployment, a bounded CPU HPA, and an internal headless Service. The Factory
cloud example owns the two Factory replicas and the shared SessionStore and
Harness journal backend. The dedicated controller owns its own fixed-session
Host template and RBAC; this pooled Deployment is not its template.

Use an image built by a product from `github.com/looprig/host` that supplies
the product Bootstrap: storage composite, journal readers for each binding,
Department registrar, checkpointer, tenant credential verifier, workspace
manager, and namespace layout. Replace the image placeholder with that
released image. The generic `cmd/host` binary intentionally refuses to run
without Bootstrap. Supply the referenced `product-hostlink-auth` Secret out of
band, scoped to the namespace and product verifier; adapt the
`PRODUCT_HOSTLINK_CREDENTIAL` name and key to that product. The example carries
no credential value and does not imply that Host itself reads this product
variable. Factory needs a credential that the verifier accepts.

Every Pod advertises `ws://$(POD_IP):7100` as its **bare** HostLink base.
`HOST_ID` is its Pod name and `HOST_GENERATION` is 1 because each Deployment
replacement gets a new Pod name. Factory derives `/hostlink/<tenant>` from
that base. Do not advertise the headless Service name: its DNS records select
multiple Pods and would make a Host ID route to the wrong process. The Service
is for internal discovery only; it does not provide a public endpoint. Keep
port 7100 reachable only within the private cluster network by Factory and
the platform's health probes. Apply the product's network policy and TLS
termination appropriate to its cluster. Host's listener also serves `/metrics`
on that same internal port; there is no Host metrics environment switch.

`/readyz` controls traffic readiness and `/healthz` tests process liveness.
The 90-second Pod termination grace exceeds `HOST_DRAIN_GRACE=60s` by 30
seconds so SIGTERM can finish Host's drain and exit. A Pod that cannot finish
draining before the platform deadline is crash equivalent; the shared store
leases and journal provide recovery. Do not set an arbitrary `preStop` sleep
that consumes this margin.

The example caps residency at eight sessions per Host, command queue at 64,
reconciliation batch at 32, and tenant links and bindings at eight. These are
starting bounds, not throughput guarantees. The HPA scales from two to six
Pods on CPU use; CPU alone does not measure resident capacity or backlog.
Observe Host's `/metrics` and tune capacity, requests, limits, and HPA from
load tests before claiming the 1,000–5,000 ClientLink target.

Run `GOWORK=off go test ./examples/deploy` from the Host module to validate
the manifest. The test parses YAML structure and checks the key security and
drain constraints, including negative mutations.
