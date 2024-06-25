# tables.py
# Reads tables and makes tables from it
import argparse
from io import TextIOWrapper


def read(f: TextIOWrapper, *, name: str) -> dict[str, list[str]]:
    """Reads the ratios from the input file"""
    values = {}
    while True:
        fptr = f.tell()
        parts = f.readline().rstrip().split(maxsplit=1)

        if not parts:
            return values

        ctx, algo = parts
        if ctx.lower() != name.lower():
            f.seek(fptr)
            return values

        values[algo] = [p.replace("%", "\\%") for p in f.readline().rstrip().split()]


def textable(
    name: str, points: list[int], values: dict[str, list[str]], *, baseline: bool
) -> str:
    assert name in ["metadata", "redundancy"]
    assert all(0 <= x <= 100 for x in points)

    indexes = [p // 5 for p in points]
    cols = "l" + "c" * len(points)

    def bold(s: str) -> str:
        return f"\\textbf{{{s}}}"

    percentages = " &".join(bold(f"{p}\\%") for p in points)
    algorithm = bold("Algorithm")
    header = f"\t\t{algorithm} & {percentages} \\\\"

    rows = []
    for a, v in values.items():
        if a == "Baseline" and not baseline:
            continue

        vals = [p for i, p in enumerate(v) if i in indexes]
        assert len(points) == len(vals)

        rows.append(f"\t\t{a} & {" &".join(vals)} \\\\")

    hline = "\t\t\\hline"
    centering = "\t\\centering"
    caption = "\t\\caption{}"
    label = f"\t\\label{{tab:{name}}}"

    tabular = (
        [f"\t\\begin{{tabular}}{{{cols}}}", header, hline] + rows + ["\t\\end{tabular}"]
    )
    table = (
        ["\\begin{table}[ht]", centering] + tabular + [label, caption, "\\end{table}"]
    )
    return "\n".join(table)


def main():
    """Produces tables in tex format"""
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("file", nargs="?", default=("-"), type=argparse.FileType("r"))
    args = parser.parse_args()

    percentages = [0, 25, 50, 75, 90, 95, 100]

    metadata = read(args.file, name="metadata")
    metadata_table = textable("metadata", percentages, metadata, baseline=False)
    print(metadata_table)

    redundancy = read(args.file, name="redundancy")
    redundancy_table = textable("redundancy", percentages, redundancy, baseline=True)
    print(redundancy_table)


if __name__ == "__main__":
    main()
