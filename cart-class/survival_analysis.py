#survival without palliative care python framework

import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.plotting import add_at_risk_counts

def get_first_event(row):
    events = {'fail_death_dtsort': row['fail_death_dtsort'], 
              'fail_soin_palliatif_dtsort': row['fail_soin_palliatif_dtsort']}
    events = {k: v for k, v in events.items() if v != 0}
    if not events:
        return pd.Series([0, 0])
    first_event_column = min(events, key=events.get)
    return pd.Series([events[first_event_column], first_event_column])

#compare 2 sites survival, easily generalizable to more
def survival_curves(df, df hop1, hop2,  time_steps, title) :
    kmf_1 = KaplanMeierFitter()
    kmf_2 = KaplanMeierFitter()

    kmf_1.fit(df.loc[df['lib_hop'] == hop1, 'time'], df.loc[df['lib_hop'] == hop1, 'event'], label=hop1)
    kmf_2.fit(df.loc[df['lib_hop'] == hop2, 'time'], df.loc[df['lib_hop'] == hop2, 'event'], label=hop2)

    plt.figure(figsize=(10, 5))
    ax = kmf_1.plot_survival_function(show_censors=True, ci_alpha = 0.2)
    kmf_2.plot_survival_function(show_censors=True, ci_alpha = 0.2)

    # Customize the plot
    plt.grid()
    plt.title(title)
    plt.legend(loc='best')

    plt.xlim(0, time_steps[-1])
    plt.xlabel("years")
    add_at_risk_counts(kmf_1, kmf_2, xticks=time_steps)
    plt.savefig("img/survival_curves.png")

    return