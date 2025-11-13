import pandas as pd
import matplotlib.pyplot as plt

data = {
    'threads': [
        '1', '1', '1', '1', '1',
        '2', '2', '2', '2', '2',
        '4', '4', '4', '4', '4',
        '8GB', '8GB', '8GB', '8GB', '8GB',
        '16GB', '16GB', '16GB', '16GB', '16GB'
    ],
    'joins': [
        1, 2, 3, 5, 10,
        1, 2, 3, 5, 10,
        1, 2, 3, 5, 10,
        1, 2, 3, 5, 10,
        1, 2, 3, 5, 10
    ],
    'mean': [
        0.0432728052139282, 0.22057147, 0.232001066, 0.226364398002624, 6.05628068447113,
        0.043203902, 0.21938014, 0.23505702, 0.228758526, 3.342936778,
        0.044715381, 0.228406096, 0.245189905, 0.238081217, 1.846649885,
        0.047244811, 0.229198265, 0.243429565, 0.235215068, 1.808090854,
        0.047089624, 0.229299402, 0.24388411, 0.236220169, 1.807951903
    ],
    'stdev': [
        0.00084743, 0.0032204, 0.00377442, 0.0069156, 0.48678213,
        0.00054401, 0.00283988, 0.00193468, 0.00192255, 0.08748205,
        0.00087987, 0.00126324, 0.00160419, 0.00087075, 0.03266959,
        0.00026537, 0.00075738, 0.00219965, 0.00083518, 0.00531291,
        0.00011024, 0.0010028, 0.00216626, 0.00085363, 0.0050515
    ],
    'mad': [
        0.000263453, 0.00166595, 0.001013637, 0.000319004, 0.363178611,
        0.000225186, 0.001873612, 0.00042963, 0.000295877, 0.057175994, 
        0.000476003, 0.000859976, 0.000679016, 0.000325441, 0.013843417,
        0.0000895261764526367, 0.000367403, 0.000504375, 0.000627995, 0.003386378,
        0.0000674724578857421, 0.000220776, 0.000327468, 0.00060904, 0.003790379
    ]
}
df = pd.DataFrame(data)
df_mean = df.pivot(index='memory', columns='joins', values='mean')
df_stdev = df.pivot(index='memory', columns='joins', values='stdev')
df_mad = df.pivot(index='memory', columns='joins', values='mad')

memory_order = ['1GB', '2GB', '4GB', '8GB', '16GB']
df_mean = df_mean.reindex(memory_order)
df_stdev = df_stdev.reindex(memory_order)
df_mad = df_mad.reindex(memory_order)

ax = df_mean.plot(
    kind='bar',
    yerr=df_mad,
    figsize=(15, 7),
    title="Mean Performance by Memory and Joins",
    capsize=4
)

ax.set_ylabel("Mean Value (log scale)")
ax.set_yscale('log')
ax.set_xlabel("Memory")
ax.legend(title="Joins")
ax.tick_params(axis='x', rotation=0)
plt.tight_layout()
plt.show()