# Kitsune 11 Docker Compose Local Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a Docker Compose local cluster that demonstrates the full MVP path with coordinator, three search nodes, etcd, NATS JetStream, and S3-compatible object storage.

**Architecture:** Add Compose infrastructure and smoke scripts after application milestones are implemented. Healthchecks gate dependent service startup. Scripts exercise create-index, write, direct event publish, search, snapshot, restore, node-stop, and status flows.

**Tech Stack:** Docker Compose, Go binaries from this repository, etcd, NATS JetStream, S3-compatible object storage, Go smoke programs.

---

Design spec: [../specs/2026-05-23-kitsune-11-docker-compose-local-ops-design.md](../specs/2026-05-23-kitsune-11-docker-compose-local-ops-design.md)  
Roadmap spec: [../../roadmaps/11-docker-compose-local-ops.md](../../roadmaps/11-docker-compose-local-ops.md)

## File Structure

- Create: `deploy/local/compose.yaml` for local services.
- Create: `deploy/local/config/coordinator.yaml` for coordinator config.
- Create: `deploy/local/config/search-node-a.yaml`, `search-node-b.yaml`, `search-node-c.yaml`.
- Create: `scripts/smoke/localcluster/main.go` for end-to-end REST smoke.
- Create: `scripts/smoke/directnats/main.go` for direct event publish smoke.
- Create: `docs/operations/local-cluster.md` for developer commands.

### Task 1: Compose Service Skeleton

**Files:**
- Create: `deploy/local/compose.yaml`
- Test: `docs/operations/local-cluster.md`

- [ ] **Step 1: Write the failing verification note**

Create `docs/operations/local-cluster.md` with this command contract:

```markdown
# Local Cluster

Run:

```bash
docker compose -f deploy/local/compose.yaml config
```

Expected: Compose validates coordinator, three search nodes, etcd, NATS, and S3-compatible object storage services.
```

- [ ] **Step 2: Run validation to verify it fails**

Run: `docker compose -f deploy/local/compose.yaml config`

Expected: FAIL because `deploy/local/compose.yaml` does not exist.

- [ ] **Step 3: Write minimal Compose file**

```yaml
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.18
    command: ["etcd", "--advertise-client-urls=http://etcd:2379", "--listen-client-urls=http://0.0.0.0:2379"]
    ports:
      - "2379:2379"

  nats:
    image: nats:2.10
    command: ["-js", "-sd", "/data"]
    ports:
      - "4222:4222"

  s3:
    image: minio/minio:latest
    command: ["server", "/data", "--console-address", ":9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"

  coordinator:
    build:
      context: ../..
    command: ["kitsune", "coordinator", "--config", "/config/coordinator.yaml"]
    depends_on:
      - etcd
      - nats
      - s3
    volumes:
      - ./config:/config:ro
    ports:
      - "8080:8080"

  search-node-a:
    build:
      context: ../..
    command: ["kitsune", "search-node", "--config", "/config/search-node-a.yaml"]
    depends_on:
      - etcd
      - nats
      - s3
    volumes:
      - ./config:/config:ro

  search-node-b:
    build:
      context: ../..
    command: ["kitsune", "search-node", "--config", "/config/search-node-b.yaml"]
    depends_on:
      - etcd
      - nats
      - s3
    volumes:
      - ./config:/config:ro

  search-node-c:
    build:
      context: ../..
    command: ["kitsune", "search-node", "--config", "/config/search-node-c.yaml"]
    depends_on:
      - etcd
      - nats
      - s3
    volumes:
      - ./config:/config:ro
```

- [ ] **Step 4: Run validation to verify it passes**

Run: `docker compose -f deploy/local/compose.yaml config`

Expected: PASS and prints normalized Compose config.

- [ ] **Step 5: Commit**

```bash
git add deploy/local/compose.yaml docs/operations/local-cluster.md
git commit -m "Define local Compose cluster skeleton

Constraint: Local ops must run a coordinator, three search nodes, etcd, NATS, and S3-compatible object storage.
Confidence: medium
Scope-risk: moderate
Tested: docker compose -f deploy/local/compose.yaml config"
```

### Task 2: Local Configuration Files

**Files:**
- Create: `deploy/local/config/coordinator.yaml`
- Create: `deploy/local/config/search-node-a.yaml`
- Create: `deploy/local/config/search-node-b.yaml`
- Create: `deploy/local/config/search-node-c.yaml`

- [ ] **Step 1: Write coordinator config**

```yaml
httpAddress: ":8080"
etcdEndpoints:
  - "http://etcd:2379"
natsURL: "nats://nats:4222"
s3:
  endpoint: "s3:9000"
  bucket: "kitsune-snapshots"
  accessKey: "minioadmin"
  secretKey: "minioadmin"
  region: "us-east-1"
  secure: false
documentMaxBytes: 1048576
```

- [ ] **Step 2: Write search node A config**

```yaml
nodeID: "node-a"
grpcAddress: ":9001"
dataDir: "/data/kitsune"
etcdEndpoints:
  - "http://etcd:2379"
natsURL: "nats://nats:4222"
memberlistBind: ":7946"
```

- [ ] **Step 3: Write search node B config**

```yaml
nodeID: "node-b"
grpcAddress: ":9002"
dataDir: "/data/kitsune"
etcdEndpoints:
  - "http://etcd:2379"
natsURL: "nats://nats:4222"
memberlistBind: ":7946"
```

- [ ] **Step 4: Write search node C config**

```yaml
nodeID: "node-c"
grpcAddress: ":9003"
dataDir: "/data/kitsune"
etcdEndpoints:
  - "http://etcd:2379"
natsURL: "nats://nats:4222"
memberlistBind: ":7946"
```

- [ ] **Step 5: Run validation**

Run: `docker compose -f deploy/local/compose.yaml config`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add deploy/local/config
git commit -m "Add local cluster service configuration

Constraint: Local cluster settings must expose runtime knobs explicitly.
Confidence: medium
Scope-risk: narrow
Tested: docker compose -f deploy/local/compose.yaml config"
```

### Task 3: Smoke Program Contract

**Files:**
- Create: `scripts/smoke/localcluster/main.go`
- Create: `scripts/smoke/directnats/main.go`
- Modify: `docs/operations/local-cluster.md`

- [ ] **Step 1: Write REST smoke program**

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	baseURL := os.Getenv("KITSUNE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	must(request(http.MethodPost, baseURL+"/v1/indexes", `{"name":"books","shardCount":3,"replicationFactor":2,"mappingVersion":1}`))
	must(request(http.MethodPut, baseURL+"/v1/indexes/books/documents/doc-1", `{"documentId":"doc-1","fields":{"title":"Bleve distributed search"}}`))
	must(request(http.MethodGet, baseURL+"/v1/indexes/books/search?q=Bleve&limit=10", ""))
	must(request(http.MethodGet, baseURL+"/v1/cluster/status", ""))
}

func request(method, url, body string) error {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return err
	}
	if body != "" {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s returned %s: %s", method, url, resp.Status, string(data))
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Run smoke program before cluster is up**

Run: `go run ./scripts/smoke/localcluster`

Expected: FAIL with connection error when the local cluster is not running.

- [ ] **Step 3: Write direct NATS smoke program**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	must(err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "KITSUNE_DOCUMENTS", Subjects: []string{"kitsune.documents.>"}})
	must(err)
	data, err := json.Marshal(map[string]any{"id": "smoke-direct-1", "operation": "upsert", "indexName": "books", "shardId": 0, "documentId": "direct-1", "fields": map[string]any{"title": "Direct NATS publish"}})
	must(err)
	_, err = js.Publish(ctx, "kitsune.documents.books.0", data)
	must(err)
	fmt.Println("published direct NATS document event")
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Document green path**

Add to `docs/operations/local-cluster.md`:

```markdown
Run:

```bash
docker compose -f deploy/local/compose.yaml up --build
go run ./scripts/smoke/localcluster
go run ./scripts/smoke/directnats
```

Expected: the script creates an index, upserts a document, searches, and prints cluster status.
```

- [ ] **Step 5: Commit**

```bash
git add scripts/smoke docs/operations/local-cluster.md
git commit -m "Document local cluster smoke workflow

Constraint: The final milestone must prove the full MVP path locally.
Confidence: medium
Scope-risk: moderate
Tested: go run ./scripts/smoke/localcluster fails before cluster startup as expected"
```
