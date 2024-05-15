import numpy as np
import matplotlib.pyplot as plt

def main() -> None:
    t = np.arange(0.0, 2.0, 0.01)
    s = 1 + np.sin(2 * np.pi * t)

    fig, ax = plt.subplots()
    ax.plot(t, s)
    ax.set(xlabel='time (s)', ylabel='voltage (mV)')
    ax.grid()

    plt.show()

if __name__ == "__main__":
    main()
