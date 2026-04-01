#!/usr/bin/env python3
"""
tools/gen_bench_graph.py

Parse the benchmark table from README.md (between <!-- BENCH_START --> and
<!-- BENCH_END --> markers) and produce docs/bench.png — a two-panel
grouped horizontal bar chart comparing msgpack, JSON, and JSONB.

Usage:
    python3 tools/gen_bench_graph.py                   # reads README.md, writes docs/bench.png
    python3 tools/gen_bench_graph.py --readme PATH     # custom README location
    python3 tools/gen_bench_graph.py --out PATH        # custom output PNG path

This script is also invoked by the CMake `bench_graph` target.
"""

import re
import sys
import argparse
from pathlib import Path

# ---------------------------------------------------------------------------
# Parse args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Generate bench.png from README bench table")
parser.add_argument("--readme", default=None,
                    help="Path to README.md (default: <script-dir>/../README.md)")
parser.add_argument("--out", default=None,
                    help="Output PNG path (default: <script-dir>/../docs/bench.png)")
args = parser.parse_args()

script_dir = Path(__file__).parent
repo_root  = script_dir.parent
readme_path = Path(args.readme) if args.readme else repo_root / "README.md"
out_path    = Path(args.out)    if args.out    else repo_root / "docs" / "bench.png"

# ---------------------------------------------------------------------------
# Read & extract bench table from README
# ---------------------------------------------------------------------------
text = readme_path.read_text(encoding="utf-8")
m = re.search(r"<!-- BENCH_START -->(.*?)<!-- BENCH_END -->", text, re.DOTALL)
if not m:
    sys.exit("ERROR: Could not find <!-- BENCH_START --> ... <!-- BENCH_END --> in README")

bench_section = m.group(1)

# Extract platform line for subtitle
platform_m = re.search(r"> Platform: (.+)", bench_section)
platform   = platform_m.group(1).strip() if platform_m else ""

# Parse every data row  (skip header and separator)
row_re = re.compile(
    r"^\|\s*(?P<op>[^|]+?)\s*\|\s*(?P<mp>[0-9.]+)\s*\|\s*(?P<js>[0-9.n/a]+)\s*\|\s*(?P<jb>[0-9.n/a]+)\s*\|",
    re.MULTILINE,
)

def parse_val(s):
    s = s.strip()
    try:
        return float(s)
    except ValueError:
        return None  # "n/a"

rows = []
for match in row_re.finditer(bench_section):
    op = match.group("op").strip()
    mp = parse_val(match.group("mp"))
    js = parse_val(match.group("js"))
    jb = parse_val(match.group("jb"))
    if mp is not None:
        rows.append((op, mp, js, jb))

if not rows:
    sys.exit("ERROR: No data rows found in bench table")

# ---------------------------------------------------------------------------
# Split into two panels by magnitude
#   Panel A – scalar ops  (<= 2 000 ns/op)
#   Panel B – aggregate ops (> 2 000 ns/op)
# ---------------------------------------------------------------------------
scalar_rows = [(op, mp, js, jb) for op, mp, js, jb in rows if mp <= 2000]
heavy_rows  = [(op, mp, js, jb) for op, mp, js, jb in rows if mp  > 2000]

# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------
import matplotlib
matplotlib.use("Agg")            # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# Colour palette (accessible)
C_MP   = "#2563EB"   # blue   – msgpack
C_JSON = "#D97706"   # amber  – JSON text
C_JSONB= "#059669"   # green  – JSONB binary

BAR_H  = 0.25        # height of each bar
ALPHA  = 0.88

def draw_panel(ax, panel_rows, title, xlabel):
    ops  = [r[0] for r in panel_rows]
    mps  = [r[1] for r in panel_rows]
    jss  = [r[2] for r in panel_rows]
    jbs  = [r[3] for r in panel_rows]

    n = len(ops)
    y = np.arange(n)

    # Replace None with 0 for bar plotting (bars won't appear for n/a entries)
    mps_p  = [v if v is not None else 0 for v in mps]
    jss_p  = [v if v is not None else 0 for v in jss]
    jbs_p  = [v if v is not None else 0 for v in jbs]

    all_vals = [v for v in mps + jss + jbs if v is not None]
    xmax = max(all_vals) if all_vals else 1

    # Three bars per group, centred
    ax.barh(y + BAR_H,  mps_p, BAR_H, color=C_MP,    alpha=ALPHA, label="msgpack")
    ax.barh(y,          jss_p, BAR_H, color=C_JSON,  alpha=ALPHA, label="JSON text")
    ax.barh(y - BAR_H,  jbs_p, BAR_H, color=C_JSONB, alpha=ALPHA, label="JSONB")

    # Draw actual bar values
    for i, (mp, js, jb) in enumerate(zip(mps, jss, jbs)):
        ax.text(mp + xmax*0.01, i + BAR_H, f"{mp:.0f}", va="center",
                fontsize=7.5, color=C_MP)
        if js is not None:
            ax.text(js + xmax*0.01, i,        f"{js:.0f}", va="center",
                    fontsize=7.5, color=C_JSON)
        else:
            ax.text(xmax*0.02, i, "n/a", va="center",
                    fontsize=7, color=C_JSON, style="italic")
        if jb is not None:
            ax.text(jb + xmax*0.01, i - BAR_H, f"{jb:.0f}", va="center",
                    fontsize=7.5, color=C_JSONB)
        else:
            ax.text(xmax*0.02, i - BAR_H, "n/a", va="center",
                    fontsize=7, color=C_JSONB, style="italic")

    ax.set_yticks(y)
    ax.set_yticklabels(ops, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_title(title, fontsize=10, fontweight="bold", pad=6)
    ax.tick_params(axis="x", labelsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlim(left=0, right=xmax * 1.25)

# Figure sizing: taller panel for more rows
n_s = len(scalar_rows)
n_h = len(heavy_rows)
fig_h = max(5, (n_s + n_h) * 0.55 + 2.5)

fig, axes = plt.subplots(
    2, 1,
    figsize=(10, fig_h),
    gridspec_kw={"height_ratios": [n_s, max(n_h, 1)]},
)

draw_panel(axes[0], scalar_rows,
           "Scalar operations  (ns/op, lower is better)",
           "nanoseconds per operation")

if heavy_rows:
    draw_panel(axes[1], heavy_rows,
               "Aggregate operations  (ns/op, lower is better)",
               "nanoseconds per operation")
else:
    axes[1].set_visible(False)

# Shared legend
legend_handles = [
    mpatches.Patch(color=C_MP,    alpha=ALPHA, label="msgpack"),
    mpatches.Patch(color=C_JSON,  alpha=ALPHA, label="JSON text"),
    mpatches.Patch(color=C_JSONB, alpha=ALPHA, label="JSONB binary"),
]
fig.legend(handles=legend_handles, loc="upper right",
           fontsize=9, framealpha=0.9, ncol=3,
           bbox_to_anchor=(0.98, 0.99))

fig.suptitle(f"sqlite-msgpack vs JSON vs JSONB — {platform}",
             fontsize=12, fontweight="bold", y=1.01)

plt.tight_layout(rect=[0, 0, 1, 0.99])

out_path.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"Saved: {out_path}")
