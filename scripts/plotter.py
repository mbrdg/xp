# plotter.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict
from io import TextIOWrapper
import pathlib
from typing import NamedTuple
from matplotlib.figure import Figure
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


def fmt_label(label: str) -> str:
    """Simple label format to be displayed in legend"""
    return label.replace("[", " [").replace(",", ", ")


def plot_transmitted_data(experiment: Experiment) -> Figure:
    """Plots the transmitted data (total and metadata) over the network for each protocol"""
    fig, axes = plt.subplots(ncols=2, figsize=(6.4 * 2, 4.8), layout="constrained")

    for ax in axes:
        ax.xaxis.set_major_formatter(percent_formatter)
        ax.yaxis.set_major_formatter(byte_formatter)
        ax.grid(linestyle="--", linewidth=0.5, alpha=0.5)
        ax.set(xlabel="Similarity", xmargin=0.02)

    axes[0].set(ylabel="Total Transmitted (Bytes)")
    axes[1].set(ylabel="Metadata Transmitted (Bytes)")

    for proto, metrics in experiment.runs.items():
        transmitted = [m.state + m.metadata for m in metrics]
        label = fmt_label(proto)
        p = axes[0].plot(similarities, transmitted, marker="o", label=label)

        if proto != "Baseline":
            metadata = [m.metadata for m in metrics]
            color = p[-1].get_color()
            axes[1].plot(similarities, metadata, marker="o", label=label, color=color)

    axes[0].legend()
    axes[1].legend()

    return fig


def plot_time_to_sync(experiments: list[Experiment]) -> Figure:
    """Plots the time to sync on different link configurations"""
    assert len(experiments) == 3
    fig, axes = plt.subplots(ncols=3, figsize=(6.4 * 3, 4.8), layout="constrained")
    for (env, runs), ax in zip(experiments, axes):
        up, down = bit_formatter(env.upload), bit_formatter(env.download)
        ylabel = f"Time to Sync (s)\n{up}/s up, {down}/s down"

        ax.xaxis.set_major_formatter(percent_formatter)
        ax.grid(linestyle="--", linewidth=0.5, alpha=0.5)
        ax.set(xlabel="Similarity", ylabel=ylabel)

        for proto, metrics in runs.items():
            time = [m.duration for m in metrics]
            label = fmt_label(proto)
            ax.plot(similarities, time, marker="o", label=label)

        ax.legend()

    return fig


def main():
    """Script that extracts relevant data from logs and produces the plots for each experiment"""
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("files", nargs="*", default=("-"), type=argparse.FileType("r"))
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()

    # Set global configs for plotting
    plt.style.use("seaborn-v0_8-paper")
    plt.rc("font", family="serif")

    # Setup the out directory
    if args.save:
        out_dir = pathlib.Path("plots/")
        out_dir.mkdir(parents=True, exist_ok=True)

    # File reading
    for file in args.files:
        exps = read_experiments(file)

        transmitted = plot_transmitted_data(exps[1])
        if args.save:
            name = f"{pathlib.Path(file.name).stem}_transmitted.svg"
            out = out_dir / pathlib.Path(name)
            transmitted.savefig(out, dpi=600)
        else:
            plt.show()

        time = plot_time_to_sync(exps)
        if args.save:
            name = f"{pathlib.Path(file.name).stem}_time.svg"
            out = out_dir / pathlib.Path(name)
            time.savefig(out, dpi=600)
        else:
            plt.show()


if __name__ == "__main__":
    main()
