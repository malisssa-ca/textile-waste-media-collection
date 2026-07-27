#!/bin/bash
#SBATCH -J textile_scrape
#SBATCH -t 12:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH -o /home/me8260ca/textile_waste_outputs/slurm-%j.out
#SBATCH -e /home/me8260ca/textile_waste_outputs/slurm-%j.err

cd "$SLURM_SUBMIT_DIR"

module purge
# Use the exact module names returned by:
# module spider Python
module load GCCcore/11.3.0
module load Python/3.13.5

source textile_waste_env/bin/activate

INPUT_DIR="$HOME/textile_waste_input"
OUTPUT_DIR="$HOME/textile_waste_outputs"

python -u textile_waste_p2_scrape.py \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --workers-l1 16 \
  --workers-l2 8 \
  --workers-l3 2 \
  --workers-l4 2 \
  --workers-l5 2 \
