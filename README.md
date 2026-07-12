# Distributed Image Processing Pipeline

A fault-tolerant pipeline that processes large images in parallel across multiple
worker machines, coordinated through Apache Kafka. The master splits an image into
tiles, fans them out as independent tasks, and reassembles the processed tiles into
the final result. Adding a worker adds throughput — no code or config changes to the
rest of the system.

```
        upload                tasks topic                    results topic
 client ─────▶  ┌────────┐  (2 partitions)  ┌──────────┐   ┌────────┐
                │ MASTER │ ───────────────▶  │ Worker 1 │──▶│        │
                │        │                   ├──────────┤   │ MASTER │──▶ reassembled
                │ split  │ ───────────────▶  │ Worker 2 │──▶│ collect│    image
                │ collect│                   └──────────┘   └────────┘
                └────────┘  ◀── heartbeats topic ── workers announce liveness
```

Kafka partitions the `tasks` topic across the workers' shared consumer group, so
each tile is delivered to exactly one worker and the load spreads automatically.

## Why this is interesting

Processing a 4K image with a single process is slow and doesn't scale. This project
treats image processing as a **distributed data-parallel problem**: tiles are
independent units of work, Kafka is the durable task queue and load balancer, and
workers are stateless and horizontally scalable. The same shape applies to video
transcoding, ETL, or any embarrassingly-parallel batch workload.

## Key design decisions

| Decision | Why | Trade-off |
|---|---|---|
| **Kafka consumer groups for load balancing** | Workers share one group; Kafka assigns partitions, so tiles distribute with zero coordination code. | Parallelism is capped by partition count — scaling past *N* workers means adding partitions. |
| **Tile-based data parallelism** | 512×512 tiles are independent, so work is embarrassingly parallel and a slow worker never blocks others. | Transforms needing cross-tile context (e.g. large-radius blur) could seam at edges; the included transforms are pixel-local, so seams are negligible. |
| **Synchronous upload request** | Simple client contract: `POST /upload` returns the finished result. | Holds the connection open for the whole job. The `/status` + `/result` endpoints exist for the async job-polling path when jobs get large. |
| **At-most-once delivery** | Workers auto-commit offsets; if one crashes mid-tile the master reports `504 Incomplete` instead of hanging forever. | A crashed tile is dropped rather than retried — fail-fast visibility was chosen over exactly-once complexity. |
| **Dedicated heartbeat topic** | Liveness is decoupled from work; the master tracks workers via a separate `latest`-offset consumer. | Heartbeats are advisory (monitoring), not used for task routing. |
| **base64-over-JSON tile payloads** | Debuggable and language-agnostic messages. | ~33% size overhead vs. raw bytes — the first thing to change if throughput matters. |

## How it works

1. Client uploads an image to the master (`POST /upload?transformation=grayscale`).
2. Master validates size, splits it into 512×512 tiles, and publishes each tile to
   the `tasks` topic keyed by tile ID.
3. Workers in the `image-processors` consumer group pull tiles, apply the requested
   OpenCV transformation, and publish results to the `results` topic.
4. Master collects results for the job, reconstructs the full image, and returns it.
5. Throughout, workers emit heartbeats; `GET /api/workers` reports who's alive.

**Transformations:** grayscale, Gaussian blur, Canny edge detection, sharpen,
brightness increase.

## Quickstart

Requires Python 3.8+ and Docker (for the bundled single-node Kafka).

```bash
# 1. Start Kafka (KRaft mode, no ZooKeeper). Topics auto-create with 2 partitions.
docker compose up -d

# 2. Master node
cd master
pip install -r requirements.txt
python run.py                      # serves http://localhost:8000

# 3. Two workers (separate terminals). Same group ⇒ Kafka load-balances tiles.
cd worker
pip install -r requirements.txt
WORKER_ID=worker-1 python run_worker.py
WORKER_ID=worker-2 python run_worker.py
```

Open <http://localhost:8000>, upload an image (min 1024×1024), and pick a
transformation. Everything reads its broker address from `KAFKA_BROKER`
(default `localhost:9092`) — see [`.env.example`](.env.example). To run workers on
separate machines, point `KAFKA_BROKER` at the broker host's IP.

## API

| Endpoint | Description |
|---|---|
| `POST /upload?transformation=<name>` | Upload an image; returns job ID, tile count, and result path. |
| `GET /status/{job_id}` | `processing` or `completed`. |
| `GET /result/{job_id}` | Result path for a finished job (404 if not ready). |
| `GET /api/workers` | Active workers and their last-heartbeat age. |
| `GET /health` | Liveness check. |

## Tests

Unit tests cover the pure logic — tiling round-trips and every transformation's
shape/dtype invariants — without needing a running Kafka:

```bash
pip install pillow numpy opencv-python-headless pytest
pytest tests/ -q
```

CI runs the suite on every push ([`.github/workflows/tests.yml`](.github/workflows/tests.yml)).

## Project structure

```
.
├── docker-compose.yml          # single-node Kafka (KRaft)
├── master/
│   ├── app/
│   │   ├── main.py             # FastAPI app + lifespan
│   │   ├── config.py           # env-driven config
│   │   ├── routes/upload.py    # upload → split → publish → collect → reconstruct
│   │   ├── services/           # image_processor, kafka_producer/consumer, heartbeat_monitor
│   │   └── static/index.html   # web UI
│   ├── requirements.txt
│   └── run.py
├── worker/
│   ├── worker/
│   │   ├── worker_main.py      # consume → process → publish loop
│   │   ├── config.py           # WORKER_ID / broker from env
│   │   └── services/           # image_processor, kafka_consumer/producer, heartbeat_sender
│   ├── requirements.txt
│   └── run_worker.py
└── tests/                      # pytest unit tests
```

A single worker codebase runs every worker instance; identity comes from the
`WORKER_ID` environment variable rather than duplicated directories.

## Possible extensions

- Async job model (`202 Accepted` + polling) for very large images.
- Exactly-once processing via manual offset commits after result publish.
- Prometheus metrics on tiles/sec and per-worker throughput.
- Container the master and workers for a fully `docker compose`-able stack.

## Team

Originally built as a university distributed-systems project by a team of four
(master node, Kafka broker, and two worker nodes). This repository is a refactor
focused on the worker/pipeline architecture — consolidating the duplicated worker
code into one config-driven service, fixing the consumer-group setup so load
balancing works as designed, externalizing configuration, and adding tests and CI.
