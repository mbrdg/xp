# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict, namedtuple
import fileinput
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker

Exp = namedtuple("Exp", ["data", "download", "upload"])

similarity = np.arange(0, 101, 10)
percent_formatter = ticker.PercentFormatter()
byte_formatter = ticker.EngFormatter(unit="B")


def read_experiment(input: fileinput.FileInput, expected_data_points: int = 11) -> Exp:
    """
    Reads experiments from the input source.

    The input is assumed to be formatted with the following columns separated by whitespaces.
    > protocol | locsz | rmtsz | download | upload | transferred | duration
    """
    data = defaultdict(list)
    upload = None
    download = None

    while parts := input.readline().rstrip().split(maxsplit=6):
        # Read values for the links
        if download is None or upload is None:
            download = int(parts[3])
            upload = int(parts[4])

        assert download == int(parts[3]) and upload == int(parts[4])
        data[parts[0]].append((int(parts[5]), float(parts[6])))

    assert download is not None and upload is not None
    assert all(len(v) == expected_data_points for v in data.values())
    return Exp(data, download, upload)


def plot_transferred_bytes_with_similarity(experiment: Exp):
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


def plot_time_to_sync_with_similarity(experiments: tuple[Exp, Exp, Exp]):
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
        sim_eq = read_experiment(input)
        assert sim_eq.upload == sim_eq.download

        sim_lt = read_experiment(input)
        assert sim_lt.upload < sim_lt.download

        sim_gt = read_experiment(input)
        assert sim_gt.upload > sim_gt.download

    plot_transferred_bytes_with_similarity(sim_eq)
    plot_time_to_sync_with_similarity((sim_lt, sim_eq, sim_gt))


if __name__ == "__main__":
    main()
