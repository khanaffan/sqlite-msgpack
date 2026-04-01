#!/usr/bin/env python3
"""
tools/gen_size_graph.py

Parse the size-comparison table from README.md (between <!-- SIZE_START -->
and <!-- SIZE_END --> markers) and produce docs/size_comparison.png —
a two-panel grouped horizontal bar chart comparing serialised byte-sizes
for msgpack, JSON text, and JSONB binary.

Usage:
    python3 tools/gen_size_graph.py
    python3 tools/gen_size_graph.py --readme PATH --out PATH
"""

import re, sys, argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--readme", default=None)
parser.add_argument("--out",    default=None)
args = parser.parse_args()

script_dir  = Path(__file__).parent
repo_root   = script_dir.parent
readme_path = Path(args.readme) if args.readme else repo_root / "README.md"
out_path    = Path(args.out)    if args.out    else repo_root / "docs" / "size_comparison.png"

# ---------------------------------------------------------------------------
# Parse README
# ---------------------------------------------------------------------------
text = readme_path.read_text(encoding="utf-8")
m = re.search(r"<!-- SIZE_START -->(.*?)<!-- SIZE_END -->", text, re.DOTALL)
if not m:
    sys.exit("ERROR: <!-- SIZE_START --> ... <!-- SIZE_END --> not found in README")

section = m.group(1)

platform_m = re.search(r"> Platform: (.+)", section)
platform   = platform_m.group(1).strip() if platform_m else ""

# Parse rows: label | mp | json | jsonb | ... (ignore ratio columns)
row_re = re.compile(
    r"^\|\s*(?P<label>[^|]+?)\s*\|\s*(?P<mp>[0-9]+)\s*\|\s*(?P<js>[0-9]+)\s*\|\s*(?P<jb>[0-9n/a]+)",
    re.MULTILINE,
)

def parse_int(s):
    s = s.strip()
    try:
        return int(s)
    except ValueError:
        return None

rows = []
for match in row_re.finditer(section):
    label = match.group("label").strip()
    mp = parse_int(match.group("mp"))
    js = parse_int(match.group("js"))
    jb = parse_int(match.group("jb"))
    if mp is not None:
        rows.append((label, mp, js, jb))

if not rows:
    sys.exit("ERROR: No data rows found in size table")

# ---------------------------------------------------------------------------
# Split into two panels
#   Panel A – scalars and small containers (mp <= 32 B)
#   Panel B – complex / large payloads    (mp >  32 B)
# ---------------------------------------------------------------------------
panel_a = [(l, mp, js, jb) for l, mp, js, jb in rows if mp <= 32]
panel_b = [(l, mp, js, jb) for l, mp, js, jb in rows if mp  > 32]

# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

C_MP    = "#2563EB"   # blue
C_JSON  = "#D97706"   # amber
C_JSONB = "#059669"   # green
ALPHA   = 0.88
BAR_H   = 0.25

def draw_panel(ax, panel_rows, title):
    labels = [r[0] for r in panel_rows]
    mps    = [r[1] for r in panel_rows]
    jss    = [r[2] if r[2] is not None else 0 for r in panel_rows]
    jbs    = [r[3] if r[3] is not None else 0 for r in panel_rows]
    jbs_raw= [r[3] for r in panel_rows]

    n  = len(labels)
    y  = np.arange(n)
    xmax = max(mps + [v for v in jss + jbs if v > 0]) * 1.3

    ax.barh(y + BAR_H, mps, BAR_H, color=C_MP,    alpha=ALPHA, label="msgpack")
    ax.barh(y,         jss, BAR_H, color=C_JSON,  alpha=ALPHA, label="JSON text")
    ax.barh(y - BAR_H, jbs, BAR_H, color=C_JSONB, alpha=ALPHA, label="JSONB binary")

    pad = xmax * 0.012
    for i, (mp, js, jb_raw) in enumerate(zip(mps, jss, jbs_raw)):
        ax.text(mp + pad, i + BAR_H, f"{mp} B", va="center", fontsize=7.5, color=C_MP)
        if js:
            ax.text(js + pad, i,        f"{js} B", va="center", fontsize=7.5, color=C_JSON)
        if jb_raw is not None and jb_raw > 0:
            ax.text(jb_raw + pad, i - BAR_H, f"{jb_raw} B", va="center",
                    fontsize=7.5, color=C_JSONB)
        else:
            ax.text(pad, i - BAR_H, "n/a", va="center",
                    fontsize=7, color=C_JSONB, style="italic")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel("bytes (lower = more compact)", fontsize=9)
    ax.set_title(title, fontsize=10, fontweight="bold", pad=6)
    ax.tick_params(axis="x", labelsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlim(left=0, right=xmax)

n_a, n_b = len(panel_a), max(len(panel_b), 1)
fig, axes = plt.subplots(
    2, 1,
    figsize=(11, (n_a + n_b) * 0.52 + 3.0),
    gridspec_kw={"height_ratios": [n_a, n_b]},
)

draw_panel(axes[0], panel_a, "Scalars & small containers  (bytes, lower = more compact)")

if panel_b:
    draw_panel(axes[1], panel_b, "Complex & large payloads  (bytes, lower = more compact)")
else:
    axes[1].set_visible(False)

legend_handles = [
    mpatches.Patch(color=C_MP,    alpha=ALPHA, label="msgpack"),
    mpatches.Patch(color=C_JSON,  alpha=ALPHA, label="JSON text"),
    mpatches.Patch(color=C_JSONB, alpha=ALPHA, label="JSONB binary"),
]
fig.legend(handles=legend_handles, loc="upper right",
           fontsize=9, framealpha=0.9, ncol=3,
           bbox_to_anchor=(0.99, 1.00))

fig.suptitle(f"Serialised-size comparison — {platform}",
             fontsize=12, fontweight="bold", y=1.01)

plt.tight_layout(rect=[0, 0, 1, 0.99])
out_path.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"Saved: {out_path}")
