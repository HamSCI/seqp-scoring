from collections import OrderedDict
from datetime import datetime as dt
import numpy as np
import pandas as pd
import re
import time

# -----------------------------------------------------------------------------
# Miscellaneous variable/function declarations go here...
# -----------------------------------------------------------------------------

bands       = [1, 3, 7, 14, 21, 28, 50]

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
# Create a new DataFrame with the unique callsigns from column call_0.
# Additionally, compute the number of submitted QSOs per callsign.
# -----------------------------------------------------------------------------

unique_calls = df_seqp['call_0'].unique()

df_list = []
for call in unique_calls:
    if pd.isnull(call): continue
    row_dct = OrderedDict()
    row_dct['call']             = call
    row_dct['cw_qso_pts']       = 0
    row_dct['ph_qso_pts']       = 0
    row_dct['cw_qso']           = 0
    row_dct['ph_qso']           = 0
    for band in bands:
        key = 'gs_{:d}'.format(band)
        row_dct[key]            = 0
    row_dct['qsos_submitted']   = np.count_nonzero(df_seqp['call_0'] == call)

    df_list.append(row_dct)
df_out = pd.DataFrame(df_list)

print('Output DataFrame created...')

# -----------------------------------------------------------------------------
# Remove any QSOs that are missing/invalid for the following: call_1, mode, 
# band, datetime, grid_0, grid_1. Add 1 to the df_out column "qsos_dropped" for
# each QSO that has been dropped. Then, reset the indexes.
# -----------------------------------------------------------------------------

for idx, row in df_seqp.iterrows():
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
# RULE 1: Add 1 point for a Phone QSO. Add 2 points for a CW/Digital QSO.
# -----------------------------------------------------------------------------

last_b  = 0

for idx_a, row_a in df_out.iterrows():
    for idx_b, row_b in df_seqp.iloc[last_b:].iterrows():
        last_b = idx_b
        if row_a['call'] != row_b['call_0']:
            break
        elif (row_b['mode'] == 'PH' or
            row_b['mode'] == 'SSB'):
            df_out.ix[idx_a, 'ph_qso_pts']  += 1
            df_out.ix[idx_a, 'ph_qso']      += 1
        else:
            df_out.ix[idx_a, 'cw_qso_pts']  += 2
            df_out.ix[idx_a, 'cw_qso']      += 1

print('Completed scoring for Rule 1...')

# -----------------------------------------------------------------------------
# Parse grid_1 and create a new column: 'grid_4char' for use in Rule 2 scoring.
# -----------------------------------------------------------------------------

grid_4char = []

for idx, row in df_seqp.iterrows():
    s = str(row['grid_1'])[:4]
    grid_4char.append(s)

df_seqp['grid_4char'] = grid_4char

print('Completed grid square parsing...')

# -----------------------------------------------------------------------------
# RULE 2: 4-character grid squares are counted once per band.
# -----------------------------------------------------------------------------

df_seqp.sort_values(by = ['call_0', 'band', 'grid_4char']).reset_index(drop = True, inplace = True)

for rinx, row in df_out.iterrows():
    call = row['call']
    if pd.isnull(call):
        continue
    for band in bands:
       tf = np.logical_and(df_seqp['call_0'] == call, df_seqp['band'] == band)
       df_tmp = df_seqp[tf]
       if len(df_tmp) == 0:
           continue

       gs = len(df_tmp['grid_4char'].unique())
       key = 'gs_{:d}'.format(band)
       df_out.loc[rinx,key] = gs

print('Completed scoring for Rule 2...')

# -----------------------------------------------------------------------------
# Finish calculating grand totals, reorganize columns, and export accordingly.
# -----------------------------------------------------------------------------

df_out['total_qso_pts'] = df_out['cw_qso_pts'] + df_out['ph_qso_pts']
df_out['qsos_valid']    = df_out['cw_qso'] + df_out['ph_qso']
df_out['total_gs']      = df_out['gs_1'] + df_out['gs_3'] + df_out['gs_7'] + \
                          df_out['gs_14'] + df_out['gs_21'] + \
                          df_out['gs_28'] + df_out['gs_50']
df_out['total']         = df_out['total_qso_pts'] * df_out['total_gs']
df_out['qsos_dropped']  = df_out['qsos_submitted'] - df_out['qsos_valid']

df_out = df_out[[
    'call', 'cw_qso_pts', 'ph_qso_pts', 'total_qso_pts', 'cw_qso', 'ph_qso',
    'gs_1', 'gs_3', 'gs_7', 'gs_14', 'gs_21', 'gs_28', 'gs_50',
    'total_gs', 'total', 'qsos_submitted', 'qsos_dropped', 'qsos_valid'
]]

df_out.to_csv('seqp_scores.csv')

print('Output CSV exported successfully!')
