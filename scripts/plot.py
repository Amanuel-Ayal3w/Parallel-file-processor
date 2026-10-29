#!/usr/bin/env python3
"""Plot docs/bench.csv as a bar chart of records/s by worker count."""

import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> None:
    src, dest = sys.argv[1], sys.argv[2]
    labels = []
    rates = []
    with open(src, newline="") as f:
        for row in csv.DictReader(f):
            if row["mode"] == "sequential":
                labels.append("sequential")
            else:
                labels.append(f"{row['workers']} workers")
            rates.append(float(row["records_per_sec"]))

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(labels, [r / 1000 for r in rates], color="#3d5a80")
    ax.set_ylabel("thousand records/s")
    ax.set_title("gofetl throughput")
    ax.bar_label(bars, fmt="%.0f")
    fig.tight_layout()
    fig.savefig(dest, dpi=140)
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()
