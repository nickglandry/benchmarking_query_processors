import pandas as pd
import csv
import matplotlib.pyplot as plt
import os

def read_csv(csv_path):
    """
    Open a CSV file and return its rows as a list of lists (each row is a list of strings).
    NOTE: Row looks like 'threads', 'memory', 'joins', 'mean', 'std_dev', 'mad', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8', 'r9', 'r10'
    """
    with open(csv_path, newline='', encoding='utf-8') as f:
        return [row for row in csv.reader(f)]

def analyze_threads():
    rows = read_csv(os.path.join('analysis', 'inputs', 'results_sf30.csv'))

    data = []
    for row in rows:
        if row[0] not in ('threads', 'N/A'):
            data.append({
                "threads": int(row[0]),
                "joins": int(row[2]),    
                "mean": float(row[3]),
                "mad": float(row[5])
            })

    df = pd.DataFrame(data)

    # Sort threads for plotting
    df = df.sort_values("threads")

    # Pivot so rows=threads, columns=joins, values=mean
    df_pivot = df.pivot(index="threads", columns="joins", values="mean")
    df_err = df.pivot(index="threads", columns="joins", values="mad")

    # Plot grouped bar chart
    ax = df_pivot.plot(
        kind="bar",
        yerr=df_err,
        figsize=(15, 7),
        capsize=4
    )
    ax.set_xlabel("Threads")
    ax.set_ylabel("Mean Value (Seconds) log scale")
    ax.set_yscale("log")
    ax.set_ylim(bottom=1e-3)
    ax.set_title("Mean Performance by Threads across Joins")
    ax.tick_params(axis='x', rotation=0)
    ax.legend(title="Joins")
    plt.tight_layout()
    plt.show()


def analyze_memory():
    rows = read_csv(os.path.join('analysis', 'inputs', 'results_sf30.csv'))

    data = []
    for row in rows:
        if row[1] not in ('memory', 'N/A'):
            data.append({
                "memory": row[1],
                "joins": int(row[2]),    
                "mean": float(row[3]),
                "mad": float(row[5])
            })

    df = pd.DataFrame(data)

    # Sort threads for plotting
    desired_order = ['1GB', '2GB', '3GB', '4GB', '5GB', '6GB', '7GB', '8GB', '16GB']
    df['memory'] = pd.Categorical(df['memory'], categories=desired_order, ordered=True)


    # Pivot so rows=threads, columns=joins, values=mean
    df_pivot = df.pivot(index="memory", columns="joins", values="mean")
    df_err = df.pivot(index="memory", columns="joins", values="mad")

    # Plot grouped bar chart
    ax = df_pivot.plot(
        kind="bar",
        yerr=df_err,
        figsize=(15, 7),
        capsize=4
    )
    ax.set_xlabel("Memory (GB)")
    ax.set_ylabel("Mean Value (Seconds) log scale")
    ax.set_yscale("log")
    ax.set_ylim(bottom=1e-3)
    ax.set_title("Mean Performance by Memory across Joins")
    ax.tick_params(axis='x', rotation=0)
    ax.legend(title="Joins")
    plt.tight_layout()
    plt.show()



analyze_threads()
analyze_memory()

