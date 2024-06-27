# xp

A framework for testing state-based CRDTs sync protocols.

## Usage

Run the binary using Cargo.
Either opt for run the simulation with GSets or AWSets.

```bash
$ cargo run -q -r -- awset
$ cargo run -q -r -- gset
```

Such output can be fed into [`scrips/plots.py`](./scripts/plots.py)
to produce plots using matplotlib by using the following command:

```bash
$ python scripts/plotter.py --help
```

If `--quiet` is not enables in the script above,
then its output can be used to produce tables in TeX format by
[`scripts/tables.py`](./scripts/tables.py).
The command is the following:

```bash
$ python scripts/tables.py --help
```

> [mbrdg](mailto:migb.rodrigues+github@gmail.com)
