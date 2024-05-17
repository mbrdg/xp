# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict, namedtuple
import fileinput
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker


def main() -> None:
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("filename", nargs="?")
    args = parser.parse_args()

    # File reading
    file = args.filename if args.filename else None
    with fileinput.input(files=file) as f:
        lines = [line.rstrip() for line in f]

    # Build the config from the experiment
    Config = namedtuple("Config", ["item_count", "item_size", "seed"])
    config_params = (int(p) for p in lines[0].split(" ", 3))
    config = Config(*config_params)
    print(config)

    # Similarities
    start, stop, step = tuple(int(p) for p in lines[1].split(" ", 3))
    similarity = np.arange(start, stop + 1, step)

    # Create the plot environment
    fig, ax = plt.subplots(2)
    fig.suptitle("Synchronization Metrics for GSets")

    ax[0].xaxis.set_major_formatter(ticker.PercentFormatter())
    ax[0].set(xticks=similarity, xlabel="similarity", ylabel="transmitted (bytes)")

    ax[1].xaxis.set_major_formatter(ticker.PercentFormatter())
    ax[1].set(xticks=similarity, xlabel="similarity", ylabel="time (s)")

    # Plot the size of a replica to give a reference
    replica_size = config.item_count * config.item_size
    ax[0].axhline(replica_size, color="xkcd:grey", linestyle="-")

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
        ax[0].plot(similarity, b, label=proto)

    for proto, d in durations.items():
        ax[1].plot(similarity, d, label=proto)

    for proto, h in hops.items():
        print(f"{proto} avg. {np.mean(h)} hops")

    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
