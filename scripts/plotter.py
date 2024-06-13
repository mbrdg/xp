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
    size: int
    upload: int
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

    headers = []
    collector = [
        defaultdict(list[Metrics]),
        defaultdict(list[Metrics]),
        defaultdict(list[Metrics]),
    ]

    for s in similarities:
        for i, m in enumerate(collector):
            header = Header(*map(int, f.readline().rstrip().split()))
            if s == 0:
                headers.append(header)
            assert headers[i] == header

            while parts := f.readline().rstrip().split():
                proto, *metrics = parts
                metrics = Metrics(int(metrics[0]), int(metrics[1]), float(metrics[2]))
                m[proto].append(metrics)

    assert len(headers) == 3
    assert all(all(len(v) == 11 for v in c.values()) for c in collector)
    return [Experiment(*p) for p in zip(headers, collector)]


def fmt_label(label: str) -> str:
    """Simple label format to be displayed in legend"""
    return label.replace("[", " [").replace(",", ", ")


def plot_transmitted(exp: Experiment, *, what: str) -> Figure:
    """Plots the transmitted data (total and metadata) over the network for each protocol"""
    fig, ax = plt.subplots(1, layout="constrained")

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.yaxis.set_major_formatter(byte_formatter)
    ax.grid(linestyle="--", linewidth=0.5, alpha=0.75)
    ax.set(xlabel="Similarity", xmargin=0.02, ylabel=f"{what.title()} (Bytes)")

    for proto, metrics in exp.runs.items():
        label = fmt_label(proto)

        if what == "total":
            transmitted = [m.state + m.metadata for m in metrics]
            ax.plot(similarities, transmitted, "o-", label=label)
        elif what == "metadata":
            transmitted = [m.metadata for m in metrics]
            ax.plot(similarities, transmitted, "o-", label=label)
        elif what == "redundant":
            base_pts = [2 * (1 - (s / 100)) * exp.env.size for s in similarities]
            transmitted = [m.state - nr for m, nr in zip(metrics, base_pts)]
            ax.plot(similarities, transmitted, "o-", label=label)
        else:
            raise ValueError(f"Unknown param {what} for what")

    # remove the unnecessary line for Baseline which is not considered to send extra metadata
    if what == "metadata":
        ax.lines[0].remove()

    ax.legend(title="Protocol")
    return fig


def plot_time_to_sync(exp: Experiment) -> Figure:
    """Plots the time to sync on different link configurations"""
    fig, ax = plt.subplots(layout="constrained")

    up, down = bit_formatter(exp.env.upload), bit_formatter(exp.env.download)
    ylabel = f"Time to Sync (s)\n{up}/s up, {down}/s down"

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.grid(linestyle="--", linewidth=0.5, alpha=0.75)
    ax.set(xlabel="Similarity", xmargin=0.02, ylabel=ylabel)

    for proto, metrics in exp.runs.items():
        label = fmt_label(proto)
        time = [m.duration for m in metrics]
        ax.plot(similarities, time, "o-", label=label)

    ax.legend(title="Protocols")
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
    out_dir = pathlib.Path("plots/")
    if args.save:
        out_dir.mkdir(parents=True, exist_ok=True)

    # File reading
    # TODO: Compute the % of metadata and redundancy over the total sent over the network
    for file in args.files:
        exps = read_experiments(file)

        for k in ["total", "metadata", "redundant"]:
            transmitted = plot_transmitted(exps[1], what=k)
            name = f"{pathlib.Path(file.name).stem}_transmitted_{k}.svg"
            out = out_dir / pathlib.Path(name)
            transmitted.savefig(out, dpi=600) if args.save else plt.show()

        for e, k in zip(exps, ["up", "symm", "down"]):
            time = plot_time_to_sync(e)
            name = f"{pathlib.Path(file.name).stem}_time_{k}.svg"
            out = out_dir / pathlib.Path(name)
            time.savefig(out, dpi=600) if args.save else plt.show()


if __name__ == "__main__":
    main()
