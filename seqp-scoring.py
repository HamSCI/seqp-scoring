from collections import OrderedDict
from datetime import datetime as dt
import mysql.connector
import numpy as np
import pandas as pd
import re
import time

# -----------------------------------------------------------------------------
# Miscellaneous variable/function declarations go here...
# -----------------------------------------------------------------------------

bands = [1, 3, 7, 14, 21, 28, 50]
pd.set_option('display.width', 1000)

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
# Define an SQL query function to return the data in some row.
# -----------------------------------------------------------------------------

def query(row):
    qry = ("SELECT callsign, " + row + " FROM seqp_submissions ")
    crsr = db.cursor()
    crsr.execute(qry)
    return crsr.fetchall()
    crsr.close()

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
    row_dct['call']                 = call
    row_dct['cw_qso_pts']           = 0
    row_dct['ph_qso_pts']           = 0
    row_dct['cw_qso']               = 0
    row_dct['ph_qso']               = 0
    for band in bands:
        key = 'gs_{:d}'.format(band)
        row_dct[key]                = 0
    row_dct['qsos_submitted']       = np.count_nonzero(df_seqp['call_0'] == call)
    row_dct['operated_totality']    = 0
    row_dct['operated_outdoors']    = 0
    row_dct['operated_public']      = 0
    row_dct['ground_conductivity']  = 0

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
# BONUS 1-3: Add 100 * 3 points to any callsign listed in df_out.
# -----------------------------------------------------------------------------

for idx, row in df_out.iterrows():
    df_out.ix[idx, 'operated_totality'] = 100
    df_out.ix[idx, 'operated_outdoors'] = 100
    df_out.ix[idx, 'operated_public']   = 100

print('Completed scoring for Bonuses 1-3...')

# -----------------------------------------------------------------------------
# Load in the hamsci_rsrch database.
# -----------------------------------------------------------------------------

user        = 'hamsci'
password    = 'hamsci'
host        = 'localhost'
database    = 'hamsci_rsrch'
db          = mysql.connector.connect(user=user,password=password,host=host,database=database,buffered=True)

print('SQL database loaded...')

# -----------------------------------------------------------------------------
# BONUS 4: Add 50 points if the ground conductivity in the SQL table is not 0.
# -----------------------------------------------------------------------------

for idx, row in df_out.iterrows():
    for result in query('ground_conductivity'):
        if (result[0] == row['call'] and
            result[1] != 0.0):
            df_out.ix[idx, 'ground_conductivity'] = 50

print('Completed scoring for Bonus 4...')

# -----------------------------------------------------------------------------
# BONUS 5:
# Check dsn_fname for existance of DSN.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# BONUS 6:
# ERP+50 if != 0
# Per number of bands on all antennas.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Finish calculating grand totals.
# -----------------------------------------------------------------------------

df_out['total_qso_pts'] = df_out['cw_qso_pts'] + df_out['ph_qso_pts']
df_out['qsos_valid']    = df_out['cw_qso'] + df_out['ph_qso']
df_out['total_gs']      = df_out['gs_1'] + df_out['gs_3'] + df_out['gs_7'] + \
                          df_out['gs_14'] + df_out['gs_21'] + \
                          df_out['gs_28'] + df_out['gs_50']
df_out['total']         = df_out['total_qso_pts'] * df_out['total_gs'] + \
                          df_out['operated_totality'] + \
                          df_out['operated_outdoors'] + \
                          df_out['operated_public'] + \
                          df_out['ground_conductivity']
df_out['qsos_dropped']  = df_out['qsos_submitted'] - df_out['qsos_valid']

print('Completed scoring summations...')

# -----------------------------------------------------------------------------
# Reorganize columns for proper readability.
# -----------------------------------------------------------------------------

keys = []
keys.append('call')
keys.append('qsos_submitted')
keys.append('qsos_dropped')
keys.append('qsos_valid')
keys.append('cw_qso')
keys.append('cw_qso_pts')
keys.append('ph_qso',)
keys.append('ph_qso_pts')
keys.append('total_qso_pts')
keys.append('gs_1')
keys.append('gs_3')
keys.append('gs_7')
keys.append('gs_14')
keys.append('gs_21')
keys.append('gs_28')
keys.append('gs_50',)
keys.append('total_gs')
keys.append('operated_totality')
keys.append('operated_outdoors')
keys.append('operated_public')
keys.append('ground_conductivity')
keys.append('total')

print('Columns reorganized...')

# -----------------------------------------------------------------------------
# Export the DataFrame to a CSV file, 'seqp_scores.csv'.
# -----------------------------------------------------------------------------

df_out = df_out[keys].copy()
df_out.to_csv('seqp_scores.csv',index=False)

print('Output CSV exported successfully!')