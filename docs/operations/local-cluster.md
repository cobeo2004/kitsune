# Local Cluster

Kitsune local operations are defined in `deploy/local/compose.yaml`.

Use one Compose command variable for the examples below:

```bash
COMPOSE="docker compose -f deploy/local/compose.yaml"
```

Validate the Compose model:

```bash
$COMPOSE config
```

Expected services:

- `coordinator`
- `search-node-a`
- `search-node-b`
- `search-node-c`
- `etcd`
- `nats`
- `minio`

The app services define the intended local process topology. The current tree still needs `kitsune coordinator` and
`kitsune search-node` binary wiring before this startup path is expected to be fully green.

Planned startup command:

```bash
$COMPOSE up --build
```

The Compose image builds the repository `Dockerfile`, whose entrypoint is the `kitsune` binary. App services pass
`coordinator` and `search-node` as subcommands.

Run the REST smoke:

```bash
go run ./scripts/smoke/localcluster
```

The REST smoke creates a `books` index, publishes a document write, polls until `doc-1` appears in search results, and
checks that cluster status contains nodes, tablet views, and shard assignments.

Run the direct NATS smoke:

```bash
go run ./scripts/smoke/directnats
```

The direct NATS smoke publishes the same validated document event envelope used by the coordinator event bus.

Snapshot and failover command contract:

```bash
# pending operator command wiring
kitsune snapshot create --index books --shard 0 --replica replica-a
kitsune snapshot restore --index books --shard 0 --replica replica-a
$COMPOSE stop search-node-a
go run ./scripts/smoke/localcluster
$COMPOSE start search-node-a
```

The snapshot and failover commands are intentionally documented as pending because the current tree has the package
primitives but not the operator CLI or search-node event-consumer loop needed to make those flows green.

Reset local state:

```bash
$COMPOSE down -v
```

Runtime knobs live in `deploy/local/config/*.yaml`.

| Knob | Default | Where |
| --- | --- | --- |
| Coordinator HTTP port | `8080` | `compose.yaml`, `coordinator.yaml` |
| Search-node gRPC addresses | `:9001`, `:9002`, `:9003` | `search-node-*.yaml` |
| Document size limit | `1048576` bytes | `coordinator.yaml` |
| Tablet data path | `/data/kitsune` | `search-node-*.yaml`, named volumes |
| NATS URL | `nats://nats:4222` | `coordinator.yaml`, `search-node-*.yaml` |
| NATS document stream | `KITSUNE_DOCUMENTS` over `kitsune.index.*.shard.*.events` | coordinator startup and direct smoke |
| MinIO endpoint | `minio:9000` | `coordinator.yaml` |
| MinIO credentials | `minioadmin` / `minioadmin` | `compose.yaml`, `coordinator.yaml` |
| MinIO data path | `/data` | `compose.yaml` named volume |
