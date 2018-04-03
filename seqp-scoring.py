#!/usr/bin/env python3
# import ipdb; ipdb.set_trace()

from datetime import datetime as dt
import numpy as np
import pandas as pd
import re
import time

# -----------------------------------------------------------------------------
# Define a grid square (regex) matching function to confirm valid grid squares.
# -----------------------------------------------------------------------------

def grid_nomatch(gs):
    pattern = re.compile("^[A-z]{2}[0-9]{2}")
    if pattern.match(gs):
        return False
    else:
        return True

# -----------------------------------------------------------------------------
# Read in a CSV, remove any QSOs not from seqp_logs, then sort the QSOs.
# -----------------------------------------------------------------------------

df      = pd.read_csv('seqp_all_ctyChecked.csv.bz2', parse_dates = ['datetime'])
tf      = df['source'] == 'seqp_logs'
df_seqp = df[tf].copy().sort_values(by = ['call_0', 'call_1', 'mode', 'band', 'datetime']).reset_index(drop = True)

print('CSV read in complete...')

# -----------------------------------------------------------------------------
# Remove any QSOs that are missing/invalid for the following: call_0, call_1,
# mode, band, datetime, grid_0, grid_1. Then, reset the indexes.
# -----------------------------------------------------------------------------

for idx, row in df_seqp.iterrows(): # @here, pd.isnull()
    if idx + 1 == df_seqp.shape[0]:
        break
    elif (pd.isnull(row['call_0']) or
    pd.isnull(row['call_1']) or
    pd.isnull(row['mode']) or
    pd.isnull(row['band']) or
    pd.isnull(row['datetime']) or
    pd.isnull(row['grid_0']) or
    pd.isnull(row['grid_1']) or
    len(row['grid_0']) < 4 or
    len(row['grid_1']) < 4 or
    grid_nomatch(row['grid_0']) or
    grid_nomatch(row['grid_1'])):
        df_seqp.drop(df_seqp.index[idx], inplace = True)

df_seqp.reset_index(drop = True, inplace = True)

print('NaN removal complete...')

# -----------------------------------------------------------------------------
# Check to see if the next QSO has similar callsigns, a similar band and mode,
# and if the next entry is within 10 minutes of the last. If any of those
# DO NOT apply to the next QSO, continue with the next QSO. If all of the above
# apply, cycle through the next/similar QSOs, dropping them from the DataFrame.
# -----------------------------------------------------------------------------

drop    = True
last_a  = 0

while drop == True:
    df_seqp.reset_index(drop = True, inplace = True)
    for idx_a, row_a in df_seqp.iloc[last_a:].iterrows():
        last_a = idx_a
        if idx_a + 1 == df_seqp.shape[0]:
            drop = False
            break
        elif (row_a['call_0'] != df_seqp.at[idx_a + 1, 'call_0'] or
        row_a['call_1'] != df_seqp.at[idx_a + 1, 'call_1'] or
        row_a['mode'] != df_seqp.at[idx_a + 1, 'mode'] or
        row_a['band'] != df_seqp.at[idx_a + 1, 'band'] or
        (df_seqp.at[idx_a + 1, 'datetime'] - \
        row_a['datetime']).seconds >= 600):
            continue
        else:
            for idx_b, row_b in df_seqp.iloc[idx_a + 1:].iterrows():
                if (row_a['call_0'] == row_b['call_0'] and
                row_a['call_1'] == row_b['call_1'] and
                row_a['mode'] == row_b['mode'] and
                row_a['band'] == row_b['band'] and
                (row_b['datetime'] - row_a['datetime']).seconds < 600):
                    df_seqp.drop(df_seqp.index[idx_b], inplace = True)
                else:
                    break
            break

print('Duplicate removal complete...')

# -----------------------------------------------------------------------------
# Make sure everything is sorted neatly before proceeding to the scoring code,
# then, create a new DataFrame with the unique callsigns from column call_0.
# -----------------------------------------------------------------------------

df_seqp.sort_values(by = ['call_0', 'call_1', 'datetime']).reset_index(drop = True, inplace = True)
unique_calls = df_seqp['call_0'].unique()
df_out = pd.DataFrame(columns = ['call', 'qso_cw', 'qso_ph', 'gs_1.8', 'gs_3.5', 'gs_7', 'gs_14', 'gs_21', 'gs_28'])

for call in unique_calls:
    df_out = df_out.append({
        'call':     call,
        'qso_cw':   0,
        'qso_ph':   0,
        'gs_1.8':   0,
        'gs_3.5':   0,
        'gs_7':     0,
        'gs_14':    0,
        'gs_21':    0,
        'gs_28':    0,
    }, ignore_index = True)

print('Output DataFrame created...')

# -----------------------------------------------------------------------------
# RULE 1: Add 1 point for a Phone QSO. Add 2 points for a CW/Digital QSO.
# -----------------------------------------------------------------------------

last_b  = 0

for idx_a, row_a in df_out.iterrows():
    for idx_b, row_b in df_seqp.iloc[last_b:].iterrows():
        last_b = idx_b
        if row_a['call'] != row_b['call_0']:
            break
        elif row['mode'] == 'PH':
            df_out.ix[idx_a, 'qso_ph'] += 1
        else:
            df_out.ix[idx_a, 'qso_cw'] += 2

print('Completed scoring for Rule 1...')

# -----------------------------------------------------------------------------
# Parse grid_1 and create a new column: 'grid_4char' for use in Rule 2 scoring.
# -----------------------------------------------------------------------------

grid_4char = []

for idx, row in df_seqp.iterrows():
    s = str(row['grid_1'])[:4]
    grid_4char.append(s)

df_seqp['grid_4char'] = grid_4char

# -----------------------------------------------------------------------------
# RULE 2: 4-character grid squares are counted once per band.
# -----------------------------------------------------------------------------
# TO-DO:
# Above ~ isnull not working? see @here
# Check if next call is different.
# How many qsos per call-band.
# Use that as a range to check unique grids in that range and add the amount accordingly.

df_seqp.sort_values(by = ['call_0', 'band', 'grid_4char']).reset_index(drop = True, inplace = True)
'''
for idx_a, row_a in df_out.iterrows():
    for idx_b, row_b in df_seqp.iloc[last_c:].iterrows():
'''

df_out.sort_values(by = 'qso_cw').reset_index(drop = True, inplace = True)
print(df_out)
