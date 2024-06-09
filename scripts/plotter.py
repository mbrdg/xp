# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict
import fileinput
from typing import NamedTuple
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker


class Metrics(NamedTuple):
    transmitted: int
    duration: float


class Experiment(NamedTuple):
    data: dict[str, list[Metrics]]
    avg_size_of_local: int
    avg_size_of_remote: int
    download: int
    upload: int


similarity = np.arange(0, 101, 10)
percent_formatter = ticker.PercentFormatter()
byte_formatter = ticker.EngFormatter(unit="B", places=2)


def read_experiment(input: fileinput.FileInput) -> Experiment:
    """
    Reads experiments from the input source.

    The input is assumed to be formatted with the following columns separated by whitespaces.
    > protocol | size_of_local | size_of_remote | download | upload | transferred | duration
    """
    data = defaultdict(list[Metrics])
    sizes_of_local = []
    sizes_of_remote = []
    upload = None
    download = None

    while parts := input.readline().rstrip().split(maxsplit=6):
        # Read the replica sizes
        sizes_of_local.append(int(parts[1]))
        sizes_of_remote.append(int(parts[2]))

        # Read values for the links
        if download is None or upload is None:
            download = int(parts[3])
            upload = int(parts[4])
        assert download == int(parts[3]) and upload == int(parts[4])

        # Collect relevant metrics
        metrics = Metrics(int(parts[5]), float(parts[6]))
        data[parts[0]].append(metrics)

    assert download is not None and upload is not None
    assert all(len(v) == np.size(similarity) for v in data.values())

    avg_size_of_local = int(sum(sizes_of_local) / len(sizes_of_local))
    avg_size_of_remote = int(sum(sizes_of_remote) / len(sizes_of_remote))
    return Experiment(data, avg_size_of_local, avg_size_of_remote, download, upload)


def plot_transmitted(experiment: Experiment):
    """Produce the plot containing the bytes transferred between similar replicas"""
    _, ax = plt.subplots(layout="constrained")

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.yaxis.set_major_formatter(byte_formatter)
    ax.grid(linestyle="--", linewidth=.5, alpha=.5)
    ax.set(xlabel="Similarity Ratio", xmargin=0, ylabel="Transmitted (Bytes)")

    for protocol, metrics in experiment.data.items():
        transmitted = np.array([v.transmitted for v in metrics], dtype=np.uint64)
        ax.plot(similarity, transmitted, marker="o", label=protocol)

    ax.legend(fontsize="x-small")
    plt.show()


def plot_similar_time(experiments: tuple[Experiment, Experiment, Experiment]):
    """Produce the plot containing the sync times among different bandwidths for similar replicas"""
    _, axes = plt.subplots(1, 3, figsize=(6.4 * 3, 4.8), layout="constrained")

    for exp, ax in zip(experiments, axes):
        upload = byte_formatter(exp.upload)
        download = byte_formatter(exp.download)
        label = f"Sync Time (s)\n{upload}/s up, {download}/s down"

        ax.xaxis.set_major_formatter(percent_formatter)
        ax.grid(linestyle="--", linewidth=.5, alpha=.5)
        ax.set(xlabel="Similarity Ratio", xmargin=0, ylabel=label)

        for protocol, metrics in exp.data.items():
            sync_time = np.array([v.duration for v in metrics], dtype=np.float64)
            ax.plot(similarity, sync_time, marker="o", label=protocol)

        ax.legend(fontsize="x-small")

    plt.show()


def main():
    """Script that extracts relevant data from logs and produces the plots for each experiment"""
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("filename", nargs="?", default="-")
    args = parser.parse_args()

    # Set global configs for plotting
    plt.style.use("seaborn-v0_8-colorblind")
    plt.rc("font", family="serif")

    # File reading
    with fileinput.input(files=args.filename) as input:
        # Ignore the the line containing the seed and the next empty line
        _ = input.readline()
        _ = input.readline()

        # Replicas with similarity
        gsets_eq = read_experiment(input)
        assert gsets_eq.upload == gsets_eq.download, gsets_eq
        gsets_lt = read_experiment(input)
        assert gsets_lt.upload < gsets_lt.download, gsets_lt
        gsets_gt = read_experiment(input)
        assert gsets_gt.upload > gsets_gt.download, gsets_gt

    plot_transmitted(gsets_eq)
    plot_similar_time((gsets_lt, gsets_eq, gsets_gt))


if __name__ == "__main__":
    main()
