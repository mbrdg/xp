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
    transferred: int
    duration: float


class Experiment(NamedTuple):
    data: dict[str, list[Metrics]]
    size_of_local: int | None
    size_of_remote: int | None
    download: int
    upload: int


similarity = np.arange(0, 101, 10)
percent_formatter = ticker.PercentFormatter()
byte_formatter = ticker.EngFormatter(unit="B")


def read_experiment(input: fileinput.FileInput, *, data_points: int) -> Experiment:
    """
    Reads experiments from the input source.

    The input is assumed to be formatted with the following columns separated by whitespaces.
    > protocol | size_of_local | size_of_remote | download | upload | transferred | duration
    """
    data = defaultdict(list[Metrics])
    upload, download, size_of_local, size_of_remote = None, None, None, None

    while parts := input.readline().rstrip().split(maxsplit=6):
        # Read the replica sizes
        if data_points == 1:
            if size_of_local is None or size_of_remote is None:
                size_of_local = int(parts[1])
                size_of_remote = int(parts[2])
            assert size_of_local == int(parts[1]) and size_of_remote == int(parts[2])

        # Read values for the links
        if download is None or upload is None:
            download = int(parts[3])
            upload = int(parts[4])
        assert download == int(parts[3]) and upload == int(parts[4])

        # Collect relevant metrics
        data[parts[0]].append(Metrics(int(parts[5]), float(parts[6])))

    assert download is not None and upload is not None
    assert all(len(v) == data_points for v in data.values())
    return Experiment(data, size_of_local, size_of_remote, download, upload)


def plot_similar_transferred(experiment: Experiment):
    """Produce the plot containing the bytes transferred between similar replicas"""
    _, ax = plt.subplots(layout="constrained")

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.yaxis.set_major_formatter(byte_formatter)
    ax.grid(linestyle="--", linewidth=0.5)
    ax.set(xlabel="Similarity Ratio", xmargin=0, ylabel="Transferred")

    for protocol, metrics in experiment.data.items():
        transferred = np.array([v[0] for v in metrics], dtype=np.uint64)
        ax.plot(similarity, transferred, marker="o", label=protocol)

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
        ax.grid(linestyle="--", linewidth=0.5)
        ax.set(xlabel="Similarity Ratio", xmargin=0, ylabel=label)

        for protocol, metrics in exp.data.items():
            sync_time = np.array([v[1] for v in metrics], dtype=np.float64)
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
        data_points = np.size(similarity)

        sim_eq = read_experiment(input, data_points=data_points)
        assert sim_eq.upload == sim_eq.download

        sim_lt = read_experiment(input, data_points=data_points)
        assert sim_lt.upload < sim_lt.download

        sim_gt = read_experiment(input, data_points=data_points)
        assert sim_gt.upload > sim_gt.download

        # Replicas with distinct cardinalities
        # len(local) >> len(remote)
        big_local_eq = read_experiment(input, data_points=1)
        assert big_local_eq.upload == big_local_eq.download
        assert big_local_eq.size_of_local is not None and big_local_eq.size_of_remote is not None
        assert big_local_eq.size_of_local > big_local_eq.size_of_remote

        big_local_lt = read_experiment(input, data_points=1)
        assert big_local_lt.upload < big_local_lt.download
        assert big_local_lt.size_of_local is not None and big_local_lt.size_of_remote is not None
        assert big_local_lt.size_of_local > big_local_lt.size_of_remote

        big_local_gt = read_experiment(input, data_points=1)
        assert big_local_gt.upload > big_local_gt.download
        assert big_local_gt.size_of_local is not None and big_local_gt.size_of_remote is not None
        assert big_local_gt.size_of_local > big_local_gt.size_of_remote

        # len(local) << len(remote)
        big_remote_eq = read_experiment(input, data_points=1)
        assert big_remote_eq.upload == big_remote_eq.download
        assert big_remote_eq.size_of_local is not None and big_remote_eq.size_of_remote is not None
        assert big_remote_eq.size_of_local < big_remote_eq.size_of_remote

        big_remote_lt = read_experiment(input, data_points=1)
        assert big_remote_lt.upload < big_remote_lt.download
        assert big_remote_lt.size_of_local is not None and big_remote_lt.size_of_remote is not None
        assert big_remote_lt.size_of_local < big_remote_lt.size_of_remote

        big_remote_gt = read_experiment(input, data_points=1)
        assert big_remote_gt.upload > big_remote_gt.download
        assert big_remote_gt.size_of_local is not None and big_remote_gt.size_of_remote is not None
        assert big_remote_gt.size_of_local < big_remote_gt.size_of_remote

    plot_similar_transferred(sim_eq)
    plot_similar_time((sim_lt, sim_eq, sim_gt))


if __name__ == "__main__":
    main()
