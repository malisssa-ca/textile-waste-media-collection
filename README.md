# Textile-waste media collection

`textile_waste_p2_scrape.py` performs the live three-source collection. Its
three modes are:

```bash
python textile_waste_p2_scrape.py --mode complete
python textile_waste_p2_scrape.py --mode live-only
python textile_waste_p2_scrape.py --mode archive-only
```

The completed large live-only dataset should use the separate
`archive_parallel.py` recovery runner. It never calls Requests or Playwright.
On COSMOS, submit it from the repository with:

```bash
sbatch run_archive_parallel.sh
```

The first invocation freezes `trafilatura_scraped.parquet` plus any unmerged
root `_ckpt_*.parquet` files into an immutable base. It splits only
`needs_archive=True` rows into 64 deterministic shards, runs up to eight
isolated processes at once with four HTTP-waiting threads each, and retains one
aggregate Wayback request limit. The supervisor owns each shard assignment, so
a crashed process is restarted without losing its claimed work. Re-running the
same job resumes incomplete shard files.

Inspect a run without changing anything:

```bash
python archive_parallel.py status \
  --output-dir /home/me8260ca/textile_waste_outputs \
  --run-name archive_parallel_v9
```

Only a fully completed and validated reduction replaces the canonical
Parquet. The previous canonical file, immutable base, shard results, and root
checkpoints are retained. Provider-deferred rows remain `needs_archive=True`;
to make a later independent retry pass, submit a new run name:

```bash
sbatch --export=ALL,ARCHIVE_RUN_NAME=archive_parallel_retry_2 run_archive_parallel.sh
```

The archive route is direct replay first, then the Availability API and its
snapshot. CDX is used only if Availability is unavailable; a valid empty
Availability response is not repeated through CDX. URL variants are exact,
HTTP-for-HTTPS, then query-free, and a later variant is queried only after a
valid no-capture response. At most two archived-page downloads are attempted.

The launcher defaults to two aggregate logical Wayback requests per second,
the fastest tested rate that did not reduce recovery. Processes and threads
hide response latency but do not multiply this rate. Replay, Availability and
CDX have independent circuit breakers. Startup logs print the route, timeouts,
rate and breaker settings; one-minute heartbeats report per-endpoint attempts,
successes, timeouts, average network latency, rows and current throughput.
Override the rate only after another endpoint probe or Internet Archive
approval, for example:

```bash
sbatch --export=ALL,WAYBACK_REQUEST_RATE=2 run_archive_parallel.sh
```

The read-only probe command is:

```bash
python tests/run_archive_parallel_network_probe.py \
  --source /home/me8260ca/textile_waste_outputs/trafilatura_scraped.parquet \
  --controls 12 --pending 12 --request-rate 2 \
  --output /home/me8260ca/textile_waste_outputs/archive-network-probe.json
```

The current COSMOS launcher and archive run use
`/home/me8260ca/textile_waste_outputs`.
