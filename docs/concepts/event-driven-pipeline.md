# Event-Driven Pipeline

## What is it

An event-driven pipeline runs in response to something happening — a file arriving, a message on a queue, a webhook — rather than on a fixed schedule. The contrast is a **scheduled pipeline**, which wakes up at a set interval (e.g., 2am every day) regardless of whether there's new data.

**Scheduled pipeline problems:**
- If data arrives at 3am, it sits unprocessed until the next 2am run — up to 23 hours of latency
- If no data arrives, the pipeline runs anyway — wasted compute
- If data arrives twice in one day, one batch gets skipped until the next window

**Event-driven pipeline properties:**
- Processes data as soon as it arrives — latency tied to arrival, not schedule
- Only runs when there's actual work to do
- Naturally handles variable arrival frequency — one file or ten files, each triggers independently

Common event sources: file drop (SFTP/S3), message queue (Kafka, SQS), webhook (HTTP callback), database CDC event.

---

## Why we use it here

Healthcare claims don't arrive on a schedule — clients send them when they're ready. A payer might send batches three times a day, or once a week. A schedule-based pipeline either:
- Runs too frequently and wastes resources most of the time, or
- Runs too infrequently and adds unnecessary latency

Event-driven eliminates both problems. The moment a client drops a file, the pipeline starts. `schedule=None` on the Airflow DAG makes this explicit — the DAG does not self-trigger, it only runs when told to.

---

## How it's implemented

The event chain has three parts:

**1. File Watcher (`scripts/file_watcher.py`)**

Uses the `watchdog` library to monitor `data/inbound/` for new CSV files:

```python
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ClaimsFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        process_file(Path(event.src_path))

observer = Observer()
observer.schedule(ClaimsFileHandler(), str(INBOUND_DIR), recursive=False)
observer.start()
```

`watchdog` uses OS-native file system events (FSEvents on macOS, inotify on Linux) — not polling. No CPU cost while waiting.

**Partial write guard** — files being transferred can be detected before they're complete. We wait until the file size stabilizes:

```python
def wait_for_file_stable(path: Path, stable_secs: float = 1.0) -> None:
    prev_size = -1
    while True:
        curr_size = path.stat().st_size
        if curr_size == prev_size:
            return       # size unchanged — write complete
        prev_size = curr_size
        time.sleep(stable_secs)
```

**Startup scan** — files that arrived while the watcher was down are processed immediately on startup:

```python
existing = sorted(INBOUND_DIR.glob("*.csv"))
for f in existing:
    process_file(f)
```

**2. Upload to MinIO → DAG trigger**

After validation, the file is uploaded to `healthcare-raw/` and then the Airflow REST API is called:

```python
def trigger_dag(meta: FileMeta) -> str:
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    response = requests.post(
        url,
        json={"conf": {"file_name": meta.file_name}},
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("dag_run_id", "unknown")
```

The `conf` dict carries the filename into the DAG run. Every task reads it back with `context["dag_run"].conf.get("file_name")`.

**3. Airflow DAG — `schedule=None`**

```python
with DAG(
    dag_id="claims_pipeline",
    schedule=None,        # never self-triggers
    catchup=False,
    ...
) as dag:
```

`schedule=None` means Airflow will never start this DAG on its own. It only runs when triggered externally — via the REST API, the UI, or the CLI. `catchup=False` means if the scheduler restarts it won't try to backfill missed runs.

**Idempotency guard** — the watcher checks `file_registry` before triggering. If a file is already `CURATED`, it skips re-processing:

```python
def is_already_processed(file_name: str) -> bool:
    cursor.execute(
        "SELECT ingestion_status FROM file_registry WHERE file_name = %s ORDER BY received_at DESC LIMIT 1",
        (file_name,),
    )
    row = cursor.fetchone()
    return row is not None and row[0] == "CURATED"
```

---

## Interview Q&A

**Q: Why event-driven instead of scheduled?**

> Claims don't arrive on a schedule — clients send when ready. A scheduled pipeline either adds unnecessary latency or wastes compute running with no data. Event-driven processes each file the moment it arrives, uses zero resources when idle, and scales naturally to variable arrival frequency.

**Q: How do you handle files that arrive while the watcher is down?**

> Two mechanisms. First, on startup the watcher scans `data/inbound/` for any CSVs already sitting there and processes them immediately. Second, there's a recovery CLI tool — `recover_unprocessed.py` — that cross-references `data/inbound/` against `file_registry` and re-triggers any files not yet in a terminal state. So no file is permanently lost even if the watcher was down for hours.

**Q: What prevents duplicate processing if the same file is dropped twice?**

> Two layers. The filename convention includes a sequence number — `_001`, `_002` — so a re-send is a different filename by convention, not a duplicate. For true duplicates, the watcher checks `file_registry` before triggering: if the file is already `CURATED`, it skips. If it's `FAILED` or mid-pipeline, you can force re-trigger with `--force` on the recovery tool.

**Q: How does the file watcher know the file is fully written before processing?**

> It waits until the file size stops changing — checks every second until two consecutive reads return the same size. This guards against partial writes when files are being transferred in via SFTP or large copy operations. Without this guard, you'd read a half-written file and get corrupt data or a parse error.

**Q: What's the difference between event-driven and streaming?**

> Event-driven here means each file triggers an independent batch pipeline run — one file in, one pipeline execution. Streaming (Kafka + Spark Structured Streaming) means a continuous process that processes records as they arrive, typically with micro-batch or true record-at-a-time semantics. The distinction matters for latency: event-driven has latency proportional to file arrival + pipeline runtime (minutes). Streaming can achieve sub-second latency. For claims files that arrive a few times per day, event-driven is the right fit — streaming would be over-engineering.
