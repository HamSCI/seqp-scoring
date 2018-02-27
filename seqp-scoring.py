#!/usr/bin/env python3
from datetime import datetime as dt
import numpy as np
import pandas as pd

# Return a difference of two datetime entries.
def tdelta(t1, t2):
    directive = '%Y-%m-%d %H:%M:%S'
    delta_hms = dt.strptime(t1, directive) - dt.strptime(t2, directive)
    return delta_hms.total_seconds() / -60.0

# Sift through the list of entries and determine if the entry is either valid
# or invalid because of similar entries within 10 minutes on the same mode.
# If the tdelta is between 0 to 10 minutes, the (similar) entry is removed;
# otherwise, the entry is valid.
def is_valid(mode, call_0, call_1, band, time):
    for idx, row in csv_in.iterrows():
        print('is_valid check', idx, tdelta(row['datetime'], time))
        if tdelta(row['datetime'], time) >= 10.0:
            continue
        elif (row['mode'] == mode and
        row['call_0'] == call_0 and
        row['call_1'] == call_1 and
        row['band'] == band and
        tdelta(row['datetime'], time) > 0.01):
            return False
            csv_in.drop(idx, inplace = True)
            break
        elif tdelta(row['datetime'], time) > 0.01:
            continue
        else:
            return True
            break

def df_ini(call):
    csv_out.loc[loc_counter] = [call, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

# Check to see if the callsign exists in the output Pandas DataFrame.
# If it does not exist, or the DataFrame is empty, load a new row;
# otherwise, call a previously used index in the DataFrame.
def load(call):
    if csv_out.empty:
        df_ini(call)
        csv_out
        global loc_counter
        loc_counter += 1
        return 0
    else:
        for idx, row in csv_out.iterrows():
            if (row['call'] == call):
                return idx
            elif (row['call'] != call and
            idx + 1 == csv_out.shape[0]):
                df_ini(call)
                loc_counter += 1
                return idx + 1
                break
            else:
                continue

# Read in a CSV through Pandas.
df = pd.read_csv('seqp_all_ctyChecked.csv.bz2',parse_dates=['datetime'])

tf      = df['source'] == 'seqp_logs'
df_seqp = df[tf].copy()
calls   = df_seqp['call_0'].unique()

#array(['CW', 'FT', 'PH', 'RY', 'JT', 'PK', 'OT', 'PS', 'DI', 'PSK31', 'DG',
#           'JT65', 'FM', 'HE', 'FT8', 'BP', 'US', 'RT', 'SSB', 'DA', 'SS', 'VO'], dtype=object)

valid_phone = ['PH','SSB']
valid_cwdig = ['PSK31','CW','FT8','JT65']

score_list  = []
for call in calls:
    tf  = df_seqp['call_0'] == call
    qs  = df_seqp[tf].copy()

    import ipdb; ipdb.set_trace()
