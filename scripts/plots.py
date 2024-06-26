# plots.py
# Plots the data gathered from experiements

import argparse
from collections import defaultdict
from io import TextIOWrapper
from pathlib import Path
from typing import NamedTuple
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from matplotlib import ticker
from matplotlib.typing import ColorType


class Header(NamedTuple):
    avg_size: int
    upload: int
    download: int


class Metrics(NamedTuple):
    state: int
    metadata: int
    duration: float


class Algorithm(NamedTuple):
    name: str
    params: dict[str, str]

    def __hash__(self) -> int:
        return hash((self.name, frozenset(self.params.items())))

    @property
    def lf(self) -> float:
        return float(self.params["f_{ld}"])

    def is_blbu(self) -> bool:
        return self.name == "Bloom+Bucketing"


class Experiment(NamedTuple):
    env: Header
    runs: dict[Algorithm, list[Metrics]]


similarities = range(0, 101, 5)
percent_formatter = ticker.PercentFormatter()
byte_formatter = ticker.EngFormatter(unit="B")
bit_formatter = ticker.EngFormatter(unit="b")


def read_algorithm(k: str) -> Algorithm:
    """
    Parses an algorithm key.
    This function assumes that the input is not malformed.
    """
    name, *params = k.replace("[", " ").replace(",", " ").removesuffix("]").split()
    formatted = {}

    for param in params:
        pname, value = param.split("=")
        if pname == "fpr":
            formatted["\\epsilon"] = value.replace("%", "\\%")
        elif pname == "lf":
            formatted["f_{ld}"] = value

    return Algorithm(name, formatted)


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
            assert headers[i].upload == header.upload
            assert headers[i].download == header.download

            while parts := f.readline().rstrip().split():
                algo, *metrics = parts
                algo = read_algorithm(algo)
                metrics = Metrics(int(metrics[0]), int(metrics[1]), float(metrics[2]))

                m[algo].append(metrics)

    assert len(headers) == 3
    assert all(
        all(len(v) == len(list(similarities)) for v in c.values()) for c in collector
    )
    return [Experiment(*p) for p in zip(headers, collector)]


def fmt_label(label: Algorithm) -> str:
    """Simple label format to be displayed in legend"""
    if not label.params:
        return label.name

    name = "".join(p[:2] for p in label.name.split("+"))
    params = f"[{", ".join(f"${k} = {v}$" for k, v in label.params.items())}]"
    return f"{name} {params}"


def plot_transmitted(
    exp: Experiment, what: str, colors: dict[Algorithm, ColorType]
) -> Figure:
    """Plots the transmitted data (total and metadata) over the network for each protocol"""
    fig, ax = plt.subplots(1, layout="constrained")

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.yaxis.set_major_formatter(byte_formatter)
    ax.grid(linestyle="--", linewidth=0.5, alpha=0.75)
    ax.set(xlabel="Similarity", xmargin=0, ylabel=f"{what.title()} (Bytes)")

    for algo, metrics in exp.runs.items():
        color = colors[algo]
        label = fmt_label(algo)

        if what == "total":
            transmitted = [m.state + m.metadata for m in metrics]
            ax.plot(similarities, transmitted, "o-", c=color, lw=0.8, label=label)
        elif what == "metadata":
            transmitted = [m.metadata for m in metrics]
            ax.plot(similarities, transmitted, "o-", c=color, lw=0.8, label=label)
        elif what == "redundancy":
            base_pts = [2 * (1 - (s / 100)) * exp.env.avg_size for s in similarities]
            transmitted = [max(m.state - nr, 0) for m, nr in zip(metrics, base_pts)]
            ax.plot(similarities, transmitted, "o-", c=color, lw=0.8, label=label)
        else:
            raise ValueError(f"Unknown param {what} for what")

    ax.legend(title="Algorithms")
    return fig


def print_transmission_ratios(exp: Experiment, what: str):
    """Prints the ratios of metadata and redundancy against the total transmitted."""
    for algo, metrics in exp.runs.items():
        label = fmt_label(algo)
        total = [m.state + m.metadata for m in metrics]

        if what == "metadata":
            collected = [m.metadata for m in metrics]
        elif what == "redundancy":
            base_points = [2 * (1 - (s / 100)) * exp.env.avg_size for s in similarities]
            collected = [max(m.state - nr, 0) for m, nr in zip(metrics, base_points)]
        else:
            raise ValueError(f"Unknown value parameter {what} for what")

        rts = [f"{m / t:.1%}" for m, t in zip(collected, total)]
        print(f"{what} {label}", " ".join(rts), sep="\n")


def plot_time_to_sync(exp: Experiment, colors: dict[Algorithm, ColorType]) -> Figure:
    """Plots the time to sync on different link configurations"""
    fig, ax = plt.subplots(layout="constrained")

    up, down = bit_formatter(exp.env.upload), bit_formatter(exp.env.download)
    ylabel = f"Time to Sync (s)\n{up}/s up, {down}/s down"

    ax.xaxis.set_major_formatter(percent_formatter)
    ax.grid(linestyle="--", linewidth=0.5, alpha=0.75)
    ax.set(xlabel="Similarity", xmargin=0, ylabel=ylabel)

    for algo, metrics in exp.runs.items():
        color = colors[algo]
        label = fmt_label(algo)
        time = [m.duration for m in metrics]
        ax.plot(similarities, time, "o-", c=color, lw=0.8, label=label)

    ax.legend(title="Algorithms")
    return fig


def main():
    """Script that extracts relevant data from logs and produces the plots for each experiment"""
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("files", nargs="*", default=("-"), type=argparse.FileType("r"))
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--quiet", "-q", action="store_true")
    args = parser.parse_args()

    # Set global configs for plotting
    plt.style.use("seaborn-v0_8-paper")
    plt.rc("font", family="serif")

    # Setup the out directory
    out_dir = Path("results/")
    if args.save:
        out_dir.mkdir(parents=True, exist_ok=True)

    def save_or_show(fig: Figure, fname: str):
        if args.save:
            fig.savefig(out_dir / fname, dpi=600)
            plt.close(fig)
        if args.show:
            plt.show()

    for file in args.files:
        # File reading
        exps = read_experiments(file)
        colors = {
            a: p["color"]
            for a, p in zip(exps[1].runs.keys(), plt.rcParams["axes.prop_cycle"])
        }

        # Display the ratios
        if not args.quiet:
            for k in ("metadata", "redundancy"):
                print_transmission_ratios(exps[1], k)

        for k in ("total", "metadata", "redundancy"):
            # Plot the core experiments
            runs = {
                k: v
                for k, v in exps[1].runs.items()
                if (not k.is_blbu()) or k.lf == 1.0
            }
            core = Experiment(exps[1].env, runs)

            transmitted = plot_transmitted(core, k, colors)
            name = f"{Path(file.name).stem}_transmitted_{k}.pdf"
            save_or_show(transmitted, name)

            # Plot the blbu experiments
            runs = {k: v for k, v in exps[1].runs.items() if k.is_blbu()}
            blbu = Experiment(exps[1].env, runs)

            transmitted = plot_transmitted(blbu, k, colors)
            name = f"{Path(file.name).stem}_blbu_transmitted_{k}.pdf"
            save_or_show(transmitted, name)

        for exp, k in zip(exps, ["up", "symm", "down"]):
            # Plot the core time experiments
            runs = {
                k: v for k, v in exp.runs.items() if (not k.is_blbu()) or k.lf == 1.0
            }
            core = Experiment(exp.env, runs)

            time = plot_time_to_sync(core, colors)
            name = f"{Path(file.name).stem}_time_{k}.pdf"
            save_or_show(time, name)

            # Plot the blbu time experiments
            runs = {k: v for k, v in exp.runs.items() if k.is_blbu()}
            blbu = Experiment(exp.env, runs)

            time = plot_time_to_sync(blbu, colors)
            name = f"{Path(file.name).stem}_blbu_time_{k}.pdf"
            save_or_show(time, name)


if __name__ == "__main__":
    main()
