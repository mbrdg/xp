# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict
from io import TextIOWrapper
from typing import NamedTuple
import matplotlib.pyplot as plt
from matplotlib import ticker


class Header(NamedTuple):
    size_of_local: int
    upload: int
    size_of_remote: int
    download: int


class Metrics(NamedTuple):
    state: int
    metadata: int
    duration: float


class Experiment(NamedTuple):
    env: Header
    runs: dict[str, list[Metrics]]


similarities = range(0, 101, 10)
percent_formatter = ticker.PercentFormatter()
byte_formatter = ticker.EngFormatter(unit="B")
bit_formatter = ticker.EngFormatter(unit="b")


def read_experiments(f: TextIOWrapper) -> list[Experiment]:
    """
    Reads an experiment from the input source.
    This function assumes that the input is not malformed.
    """
    # Ignore the first empty line
    _ = f.readline()

    envs = set()
    collector = [
        defaultdict(list[Metrics]),
        defaultdict(list[Metrics]),
        defaultdict(list[Metrics]),
    ]

    for _ in similarities:
        for m in collector:
            env = Header(*map(int, f.readline().rstrip().split()))
            envs.add(env)

            while parts := f.readline().rstrip().split():
                proto, *metrics = parts
                state, metadata, duration = (
                    int(metrics[0]),
                    int(metrics[1]),
                    float(metrics[2]),
                )
                m[proto].append(Metrics(state, metadata, duration))

    return [Experiment(*p) for p in zip(envs, collector)]


def plot_transmitted_data(experiment: Experiment):
    """Plots the transmitted data (total and metadata) over the network for each protocol"""
    _, axes = plt.subplots(ncols=2, figsize=(6.4 * 2, 4.8), layout="constrained")

    for ax in axes:
        ax.xaxis.set_major_formatter(percent_formatter)
        ax.yaxis.set_major_formatter(byte_formatter)
        ax.grid(linestyle="--", linewidth=0.5, alpha=0.5)
        ax.set(xlabel="Similarity", xmargin=0)

    axes[0].set(ylabel="Total Transmitted (Bytes)")
    axes[1].set(ylabel="Metadata Transmitted (Bytes)")

    for proto, metrics in experiment.runs.items():
        transmitted = [m.state + m.metadata for m in metrics]
        axes[0].plot(similarities, transmitted, marker="o", linewidth=.75, label=proto)

        metadata = [m.metadata for m in metrics]
        axes[1].plot(similarities, metadata, marker="o", linewidth=.75, label=proto)

    axes[0].legend(fontsize="x-small")
    axes[1].legend(fontsize="x-small")

    plt.show()


def plot_time_to_sync(experiments: list[Experiment]):
    """Plots the time to sync on different link configurations"""
    assert len(experiments) == 3
    _, axes = plt.subplots(ncols=3, figsize=(6.4 * 3, 4.8), layout="constrained")

    for (env, runs), ax in zip(experiments, axes):
        up, down = bit_formatter(env.upload), bit_formatter(env.download)
        ylabel = f"Time to Sync (s)\n{up}/s up, {down}/s down"

        ax.xaxis.set_major_formatter(percent_formatter)
        ax.grid(linestyle="--", linewidth=0.5, alpha=0.5)
        ax.set(xlabel="Similarity", xmargin=0, ylabel=ylabel)

        for proto, metrics in runs.items():
            time = [m.duration for m in metrics]
            ax.plot(similarities, time, marker="o", label=proto)

        ax.legend(fontsize="x-small")

    plt.show()


def main():
    """Script that extracts relevant data from logs and produces the plots for each experiment"""
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument(
        "filenames", nargs="*", default=("-"), type=argparse.FileType("r")
    )
    args = parser.parse_args()

    # Set global configs for plotting
    plt.style.use("seaborn-v0_8-colorblind")
    plt.rc("font", family="serif")

    # File reading
    for file in args.filenames:
        exps = read_experiments(file)

        plot_transmitted_data(exps[1])
        plot_time_to_sync(exps)


if __name__ == "__main__":
    main()
