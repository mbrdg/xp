# tables.py
# Reads tables and makes tables from it
import argparse
from io import TextIOWrapper


def read(f: TextIOWrapper, *, name: str) -> dict[str, list[str]]:
    def escape(s: str) -> str:
        return s.replace("%", "\\%")

    values = {}

    while True:
        fptr = f.tell()
        parts = f.readline().rstrip().split(maxsplit=1)

        if not parts:
            return values

        if parts[0].lower() != name.lower():
            f.seek(fptr)
            return values

        algo = escape(parts[1])
        values[algo] = [escape(p) for p in f.readline().rstrip().split()]


def table(
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
    parser = argparse.ArgumentParser(prog="plotter")
    parser.add_argument("files", nargs="*", default=("-"), type=argparse.FileType("r"))
    args = parser.parse_args()

    points = [0, 25, 50, 75, 90, 95, 100]

    for file in args.files:
        metadata = read(file, name="metadata")
        metadata_table = table("metadata", points, metadata, baseline=False)
        print(metadata_table)

        # redundancy = read(file, name="redundant")
        # redundancy_table = table("redundancy", points, redundancy, baseline=True)
        # print(redundancy_table)


if __name__ == "__main__":
    main()
