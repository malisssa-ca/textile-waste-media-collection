#!/bin/bash
#SBATCH -J textile_scrape
#SBATCH -t 12:00:00
# A balanced half COSMOS node. The default 5300 MB/core allocation provides
# about 127 GB, sufficient for twelve persistent Chromium workers.
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH -o /home/me8260ca/textile_waste_outputs/slurm-%j.out
#SBATCH -e /home/me8260ca/textile_waste_outputs/slurm-%j.err
#SBATCH --mail-user=melissa.cardona@keg.lu.se
#SBATCH --mail-type=FAIL,END

set -euo pipefail

cd "$SLURM_SUBMIT_DIR"

module purge
# Required prerequisite for the current Python module on COSMOS.
module load GCCcore/14.3.0
module load Python/3.13.5

source textile_waste_env/bin/activate

INPUT_DIR="$HOME/textile_waste_input"
OUTPUT_DIR="$HOME/textile_waste_outputs"
mkdir -p "$OUTPUT_DIR"

# Change only --mode below; inactive worker groups are not started.
python -X faulthandler -u textile_waste_p2_scrape.py \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --mode live-only \
  --workers-l1 24 \
  --workers-l3 12 \
  --workers-l4 12
