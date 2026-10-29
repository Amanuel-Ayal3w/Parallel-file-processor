#!/usr/bin/env bash
# Sweep worker counts and write docs/bench.csv plus docs/bench.png.
set -euo pipefail
cd "$(dirname "$0")/.."

lines="${LINES:-1000000}"
mkdir -p tmp docs bin

go build -o bin/gofetl ./cmd/gofetl

if [[ ! -f tmp/bench.jsonl ]]; then
  ./bin/gofetl gen --lines "$lines" --output tmp/bench.jsonl --format jsonl
fi

echo "mode,workers,seconds,records_per_sec" > docs/bench.csv

run_one() {
  local mode="$1"
  local workers="$2"
  local start end elapsed rate
  start=$(date +%s%N)
  if [[ "$mode" == "sequential" ]]; then
    ./bin/gofetl run --input tmp/bench.jsonl --output /dev/null --sequential --quiet
  else
    ./bin/gofetl run --input tmp/bench.jsonl --output /dev/null --workers "$workers" --quiet
  fi
  end=$(date +%s%N)
  elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.4f", (e - s) / 1000000000 }')
  rate=$(awk -v n="$lines" -v sec="$elapsed" 'BEGIN { if (sec > 0) printf "%.0f", n / sec; else print 0 }')
  echo "$mode,$workers,$elapsed,$rate" | tee -a docs/bench.csv
}

run_one sequential 1
for workers in 1 2 4 8 16; do
  run_one parallel "$workers"
done

py=python3
if ! python3 -c 'import matplotlib' >/dev/null 2>&1; then
  python3 -m venv tmp/plotenv
  tmp/plotenv/bin/pip install --quiet matplotlib
  py=tmp/plotenv/bin/python
fi
"$py" scripts/plot.py docs/bench.csv docs/bench.png
echo "wrote docs/bench.csv and docs/bench.png"
