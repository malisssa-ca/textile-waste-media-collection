#!/bin/bash
#SBATCH -J textile_archive
#SBATCH -t 48:00:00
# One COSMOS allocation runs a supervisor plus eight isolated Python processes.
# Eight allocated cores provide about 42 GB under COSMOS's default memory/core.
# HTTP threads wait on remote I/O and remain globally rate-limited.
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --signal=B:TERM@300
#SBATCH -o /home/me8260ca/textile_waste_outputs/archive-slurm-%j.out
#SBATCH -e /home/me8260ca/textile_waste_outputs/archive-slurm-%j.err
#SBATCH --mail-user=melissa.cardona@keg.lu.se
#SBATCH --mail-type=FAIL,END

set -euo pipefail

cd "$SLURM_SUBMIT_DIR"

module purge
module load GCCcore/14.3.0
module load Python/3.13.5
source textile_waste_env/bin/activate

OUTPUT_DIR="/home/me8260ca/textile_waste_outputs"
# The 2/s default is the fastest rate that improved both throughput and success
# in the real probes.  Override only after a new endpoint benchmark or explicit
# Internet Archive approval for a higher sustained rate.
WAYBACK_REQUEST_RATE="${WAYBACK_REQUEST_RATE:-2.0}"
ARCHIVE_RUN_NAME="${ARCHIVE_RUN_NAME:-archive_parallel_v9}"
mkdir -p "$OUTPUT_DIR"

# Avoid nested numerical-library thread pools competing with the eight archive
# processes.  SNIC_TMP is used only for disposable library temporary files;
# every archive checkpoint is written directly to persistent OUTPUT_DIR.
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export POLARS_MAX_THREADS=8
if [[ -n "${SNIC_TMP:-}" ]]; then
  export TMPDIR="$SNIC_TMP/textile_archive_${SLURM_JOB_ID}"
  mkdir -p "$TMPDIR"
fi

python -X faulthandler -u archive_parallel.py all \
  --output-dir "$OUTPUT_DIR" \
  --run-name "$ARCHIVE_RUN_NAME" \
  --shards 64 \
  --processes 8 \
  --threads-per-process 4 \
  --save-every 1000 \
  --checkpoint-max-age 300 \
  --request-rate "$WAYBACK_REQUEST_RATE" &

ARCHIVE_PID=$!
trap 'kill -TERM "$ARCHIVE_PID" 2>/dev/null || true; wait "$ARCHIVE_PID" || true; exit 143' TERM INT
wait "$ARCHIVE_PID"
