# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict
from cycler import cycler
import fileinput
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker

COLORS = ["#0173B2", "#DE8F05", "#029E73", "#D55E00", "#CC78BC",
          "#CA9161", "#FBAFE4", "#949494", "#ECE133", "#56B4E9"]


def main() -> None:
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("filename", nargs="?", default="-")
    args = parser.parse_args()

    # Set global configs for plotting
    plt.style.use("seaborn-v0_8-paper")
    plt.rc("font", family="serif")
    plt.rc("axes", prop_cycle=cycler("color", COLORS))

    # File reading
    with fileinput.input(files=args.filename) as f:
        # Ignore the the line containing the seed
        _ = f.readline()

        # Read the data for the experiment with similar replicas and symmetric channels
        data = defaultdict(list)
        download, upload = None, None
        while line := f.readline():
            parts = line.rstrip().split(maxsplit=6)

            # read the values for the links
            if download is None or upload is None:
                download, upload = int(parts[3]), int(parts[4])
            assert download == int(parts[3]) and upload == int(parts[4])

            data[parts[0]].append((int(parts[5]), float(parts[6])))

    assert download is not None and upload is not None
    assert all(len(v) == 11 for v in data.values())
    similarity = np.arange(0, 101, 10)[::-1]

    percent_formatter = ticker.PercentFormatter()
    bytes_formatter = ticker.EngFormatter(unit="B")

    figsize = (6.4 * 2, 4.8)
    _, ax = plt.subplots(ncols=2, figsize=figsize, layout="constrained")

    ax[0].xaxis.set_major_formatter(percent_formatter)
    ax[0].yaxis.set_major_formatter(bytes_formatter)
    ax[0].grid(linestyle="--", linewidth=0.5)
    ax[0].set(
        xlabel="Similarity Ratio",
        xmargin=0,
        ylabel="Bytes",
    )

    ax[1].xaxis.set_major_formatter(percent_formatter)
    ax[1].grid(linestyle="--", linewidth=0.5)
    ax[1].set(
        xlabel="Similarity Ratio",
        xmargin=0,
        ylabel=f"Sync Time (s)\n{bytes_formatter(upload)}/s up, {bytes_formatter(download)}/s down",
    )

    # Actual plotting
    for proto, metrics in data.items():
        transferred = np.array([v[0] for v in metrics], dtype=int)
        ax[0].plot(similarity, transferred, marker="o", label=proto)

        durations = np.array([v[1] for v in metrics], dtype=np.float64)
        ax[1].plot(similarity, durations, marker="o", label=proto)

    # Print the labels for each proto before finish
    ax[0].legend(loc="lower right", fontsize="small")
    ax[1].legend(loc="lower right", fontsize="small")

    plt.show()

if __name__ == "__main__":
    main()
