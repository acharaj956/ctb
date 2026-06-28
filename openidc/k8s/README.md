# OpenIDC on Kubernetes

These manifests deploy the **application tier** — the four stateless services
(`ingestion`, `station-processing`, `network-processing`, `dashboard`) — as
Deployments, plus a Service for the dashboard. They are the `docker-compose`
services translated to Kubernetes, with production-minded hardening.

```bash
# Build + push the images first (the manifests reference openidc/<svc>:latest):
#   docker build -t openidc/dashboard:latest -f services/dashboard/Dockerfile .
#   ... (one per service) ... then push to your registry.

kubectl apply -k k8s/
kubectl -n openidc get pods
kubectl -n openidc port-forward svc/dashboard 8000:80   # then open http://localhost:8000
```

## Deliberate scope: app tier here, infrastructure via operators

The stateful infrastructure — **Kafka, PostgreSQL, RabbitMQ** — is intentionally
**not** hand-rolled as StatefulSets here. On Kubernetes the correct, senior choice
is to run these through their operators/charts, which handle clustering, storage,
upgrades, and backups:

| Component | Production approach |
|-----------|---------------------|
| Kafka | **Strimzi** operator (`Kafka` / `KafkaTopic` CRDs) |
| PostgreSQL | **CloudNativePG** operator, or a managed service |
| RabbitMQ | **RabbitMQ Cluster Operator** |
| Prometheus + Grafana | **kube-prometheus-stack** Helm chart |

The app `ConfigMap` points at the in-cluster Service names (`kafka`, `postgres`,
`rabbitmq`) those operators expose, so the application tier is unchanged.

## Hardening applied
- Pods run as **non-root** (`runAsUser: 1000`, `runAsNonRoot: true`).
- `allowPrivilegeEscalation: false`, **all capabilities dropped**, `seccompProfile: RuntimeDefault`.
- CPU/memory **requests and limits** on every container.
- Credentials come from a **Secret** (here a demo placeholder; in production from
  External Secrets / Vault / sealed-secrets — never source control).

## PKI in Kubernetes
The Compose setup shares the signing key via a local volume; that does not work
across nodes. In Kubernetes, distribute the producer's **public** key as a Secret
mounted read-only into `station-processing`, and keep the **private** key in the
producer's own Secret. `VERIFY_FRAMES` is left `false` in `config.yaml` until that
Secret is wired up, so the manifests apply cleanly out of the box.

## Not included (future work)
Ingress/TLS for the dashboard, NetworkPolicies to restrict pod-to-pod traffic,
HorizontalPodAutoscalers, and PodDisruptionBudgets — all straightforward additions
once the platform target is known.
