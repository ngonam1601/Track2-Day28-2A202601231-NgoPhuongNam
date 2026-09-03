# Day 28 — Submission answers

## Architecture and ownership

The platform separates five layers: compute and serving (gateway, API, vLLM),
data ingestion (Kafka and Airflow), data/ML (Delta, Feast and MLflow),
retrieval (Qdrant), and operations (OpenTelemetry, Prometheus, Grafana and
Jaeger). The integration matrix is the source of truth for each boundary's
contract, owner, probe, metric and evidence file.

| Area | Owner | Responsibility |
|---|---|---|
| IP01–IP02 | team-ingestion | HTTP ingestion, Kafka, Airflow, DLQ/replay |
| IP03–IP04, IP06 | team-data | Delta merge, Feast materialisation, MLflow release |
| IP05, IP07 | team-serving | Retrieval, grounded serving, real vLLM identity |
| IP08–IP10 | team-platform | Envoy, telemetry, dashboards, manifests |

Individual contribution: **Ngo Phuong Nam** integrated, tested and documented
the end-to-end Day 28 platform; the five journeys, Kaggle-backed vLLM
verification, recovery evidence and submission artefacts are explained in this
file and the evidence pack.

## Technical choices and trade-offs

- Kafka is the durable boundary. Producers use an idempotency key and the
  Airflow batch performs Delta `MERGE`; replaying Kafka therefore does not
  duplicate lakehouse rows or Qdrant point identifiers.
- Qdrant is mandatory for grounded serving, so loss of Qdrant makes `/ready`
  return `not_ready` and removes the serving route from Envoy. Feast is
  degradable: an answer may still be served with an explicit degraded reason.
- Feedback ingestion has a dedicated liveness-routed upstream. It can continue
  accepting durable Kafka events during a retrieval outage; it does not claim
  that a retrieval-dependent answer is healthy.
- MLflow's `champion` alias selects the serving release. Promotion and rollback
  change the alias and provenance, rather than application code.
- The real vLLM endpoint runs on Kaggle GPU through a short-lived public
  tunnel. Its URL is supplied at runtime through environment variables only;
  neither the tunnel URL nor credentials belong in Git.

## Incident and recovery / no-data-loss proof

Failure injection stops Qdrant. The expected observation is direct API
`/ready = 503, status=not_ready`, while Envoy returns its own `503 no healthy
upstream`; this proves that traffic is removed from the serving route. When
Qdrant starts again, `/ready` becomes `ready` and Envoy admits the endpoint.

For malformed Kafka input, the pipeline parks the exact raw bytes in
`data.raw.dlq`, including original topic, partition, offset, key, category and
diagnostic. A valid record in the same batch reaches Delta. Replay validates
the envelope before reinjection; an unparseable payload remains parked, while
a valid payload is re-published and Delta idempotency prevents a duplicate.
This is the no-data-loss and safe-replay claim exercised by IT-J4.

## Performance and bottleneck analysis

The readiness load profile was run with 200 requests:

| Run | Result | P50 | P95 | P99 | Interpretation |
|---|---|---:|---:|---:|---|
| 8 workers | 200 × HTTP 200 | 1837 ms | 2555 ms | 2786 ms | Remote vLLM identity/readiness probe dominates latency. |
| 16 workers | 28 × HTTP 200, 172 client failures | 13 ms* | 4526 ms | 5175 ms | Saturation: the P50 is distorted by fast client failures; use P95/P99 and failure count. |

`*` The 16-worker P50 is not an SLO result because status `0` represents a
client-side timeout/connection failure. The profile targets `/ready`, which
checks remote dependencies, so it is a readiness-saturation measurement—not
an `/api/v1/ask` throughput claim. The final demo must retain the raw command
output and, with a warm vLLM tunnel, add a small `/api/v1/ask` profile.

The practical bottleneck is the public Kaggle tunnel plus cold-model/startup
latency, not Kafka or Delta. Mitigations are model cache/preflight, a stable
authenticated endpoint, request concurrency limits and a local/managed GPU
deployment for production.

## Production gaps and security

- Kaggle sessions and GPU quota can end mid-demo; the tunnel is public and
  adds latency and availability risk.
- Model download/cold start needs cache and a readiness preflight. Two T4 GPUs
  do not improve throughput without a tested tensor-parallel configuration.
- The local Compose stack is a demo environment. Production needs secret
  management, authenticated ingress, TLS, resource limits, persistent backing
  stores, alert routing, backup/restore tests and a managed GPU endpoint.
- No password, token, tunnel URL, database, cache, model weight or `.lab28/`
  runtime data is committed. `ports.template` is used rather than a secret env
  file.

## Kubernetes / GitOps

`deploy/kubernetes/base` and `gitops/application.yaml` are validated locally
with `scripts/validate_manifests.py`. The intended rollback is: build an
immutable image, commit the desired tag, sync through Argo CD, verify health
and smoke tests, then revert the desired Git revision/tag and verify gateway,
replica and trace recovery. See `runbooks/gitops-rollback.md`.

No Kubernetes/Argo CD cluster credential was available in this environment.
Therefore live drift/self-heal and an Argo sync are **UNVERIFIED**, not
fabricated. Local manifest validation is evidence of configuration correctness,
not evidence of a completed cluster deployment.

## Final verification record

Before submission, regenerate `evidence/integration-report.json` while the
real Kaggle vLLM is reachable, and retain the terminal outputs of the required
quality suite and five critical journeys. The submission is only final when
the report is `ready` and the final J5 trace contains all required spans.
