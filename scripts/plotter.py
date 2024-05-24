# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict, namedtuple
from cycler import cycler
import fileinput
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker

COLORS = ["#0173B2", "#DE8F05", "#029E73", "#D55E00", "#CC78BC",
          "#CA9161", "#FBAFE4", "#949494", "#ECE133", "#56B4E9"]

def main() -> None:
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("filename", nargs="?")
    args = parser.parse_args()

    # File reading
    file = args.filename if args.filename else None
    with fileinput.input(files=file) as f:
        lines = [line.rstrip() for line in f]

    # Build the config from the experiment
    # NOTE: The download and upload values should be change if they are changed in the experiment.
    Config = namedtuple(
        "Config",
        ["item_count", "item_size", "seed", "download", "upload"],
        defaults=[0, 0, 42, 1, 1],
    )
    config_params = (int(p) for p in lines[0].split(" ", 5))
    config = Config(*config_params)
    print(config)

    # Similarities
    start, stop, step = tuple(int(p) for p in lines[1].split(" ", 3))
    dissimilarity = np.arange(start, stop + 1, step)

    # Create the plot environment
    plt.style.use("seaborn-v0_8-paper")
    plt.rc("font", family="serif")
    plt.rc("axes", prop_cycle=cycler("color", COLORS))
    figsize = (6.4 * 2, 4.8)

    fig, ax = plt.subplots(ncols=2, figsize=figsize, layout="constrained")
    percentage_formatter = ticker.PercentFormatter(stop)
    bytes_formatter = ticker.EngFormatter(unit="B")

    ax[0].xaxis.set_major_formatter(percentage_formatter)
    ax[0].yaxis.set_major_formatter(bytes_formatter)
    ax[0].grid(linestyle="--", linewidth=0.5)

    ax[0].set(
        xlabel="Dissimilarity Ratio",
        xmargin=0,
        ylabel="Bytes",
    )

    ax[1].xaxis.set_major_formatter(percentage_formatter)
    up, down = bytes_formatter(config.upload), bytes_formatter(config.download)
    ax[1].grid(linestyle="--", linewidth=0.5)

    ax[1].set(
        xlabel="Dissimilarity Ratio",
        xmargin=0,
        ylabel=f"Sync Time (s)\n{up}/s up, {down}/s down",
    )

    # Plot the size of a replica to give a reference
    replica_size = config.item_count * config.item_size
    ax[0].axhline(replica_size, linestyle="--", color="grey", alpha=0.4)
    ax[0].annotate(
        "Replica Size",
        xy=(82.5, replica_size * 1.025),
        xycoords=(ax[0].get_xaxis_transform(), ax[0].get_yaxis_transform()),
    )

    # Data retrieaval from the experiment file
    exchanged_bytes = defaultdict(list)
    durations = defaultdict(list)
    hops = defaultdict(list)

    for run in lines[2:]:
        run_params = run.split(" ", 5)

        proto = run_params[0]
        exchanged_bytes[proto].append(int(run_params[4]))
        durations[proto].append(float(run_params[3]))
        hops[proto].append(int(run_params[2]))

    for proto, b in exchanged_bytes.items():
        ax[0].plot(dissimilarity, b, "o:", label=proto)

    for proto, d in durations.items():
        ax[1].plot(dissimilarity, d, "o:", label=proto)

    for proto, h in hops.items():
        print(f"{proto} avg. {np.mean(h)} hops")

    # Print the labels for each proto before finish
    ax[0].legend(loc="lower right", fontsize="small")
    ax[1].legend(loc="lower right", fontsize="small")

    plt.show()


if __name__ == "__main__":
    main()
