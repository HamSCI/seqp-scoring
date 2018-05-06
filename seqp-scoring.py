#!/usr/bin/env python3
import os, shutil
from collections import OrderedDict
from datetime import datetime as dt
import datetime
import mysql.connector
import numpy as np
import pandas as pd
import re
import time
import tqdm

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

def query(qry):
    crsr = db.cursor()
    crsr.execute(qry)
    results = crsr.fetchall()
    crsr.close()
    return results

# -----------------------------------------------------------------------------
# Define a function that checks if a number exists and is greater than zero.
# -----------------------------------------------------------------------------

def num_gtz(n):
    try:
        if float(n) > 0:
            return True
        else:
            return False
    except ValueError:
        return False

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

unique_calls    = df_seqp['call_0'].unique()
df_list         = []
for call in unique_calls:
    if pd.isnull(call):
        continue

    # Get grid square.
    tf  = df_seqp['call_0'] == call
    df_tmp  = df_seqp[tf]
    grids   = df_tmp['grid_0'].unique()
    if len(grids) != 1:
        import ipdb; ipdb.set_trace()
    else:
        grid    = grids[0]
        if pd.isnull(grid):
            continue
        
        grid    = grid[:4].upper()

    row_dct = OrderedDict()
    row_dct['call']                 = call
    row_dct['grid']                 = grid
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
    row_dct['antenna_design']       = 0
    row_dct['erpd']                 = 0

    row_dct['skimmers']             = 0
    row_dct['iq_data']              = 0
    row_dct['spot_bonus']           = 0
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

drop = True
last = 0

while drop == True:
    df_seqp.reset_index(drop = True, inplace = True)
    for idx_a, row_a in df_seqp.iloc[last:].iterrows():
        last = idx_a
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
modes_path  = 'modes'
if os.path.exists(modes_path):
    shutil.rmtree(modes_path)
os.makedirs(modes_path)

df_mode_list    = []
modes           = df_seqp['mode'].unique()
modes.sort()
for mode in modes:
    dft = df_seqp[df_seqp['mode'] == mode]
    fname   = '{!s}.csv'.format(mode)
    fpath   = os.path.join(modes_path,fname)
    dft.to_csv(fpath,index=False)

    dct = OrderedDict()
    dct['mode']     = mode
    dct['count']    = len(dft)
    df_mode_list.append(dct)

df_mode = pd.DataFrame(df_mode_list)
fpath   = os.path.join(modes_path,'mode_summary.csv')
df_mode.to_csv(fpath,index=False)

#modes     =   ['CW', 'PH', 'RY', 'FT', 'PK', 'PS', 'JT', 'RT', 'US', 'JT65', 'DG', 'DI', 'FM', 'OT', 'FT8', 'HE', 'SSB', 'VO', 'DA', 'PSK31']
cw_modes   =   ['CW', 'RY', 'FT', 'PK', 'PS', 'JT', 'RT', 'US', 'JT65', 'DG', 'DI', 'OT', 'FT8', 'HE', 'DA', 'PSK31']
ph_modes   =   ['PH', 'FM', 'SSB', 'VO']

for rinx,row in df_out.iterrows():
    tf      = df_seqp['call_0'] == row['call']
    dft     = df_seqp[tf]

    ph_qso  = 0
    for ph_mode in ph_modes:
        tf       = dft['mode'] == ph_mode
        ph_qso  += np.sum(tf)

    cw_qso  = 0
    for cw_mode in cw_modes:
        tf       = dft['mode'] == cw_mode
        cw_qso  += np.sum(tf)

    df_out.ix[rinx,'ph_qso']        = ph_qso
    df_out.ix[rinx,'ph_qso_pts']    = ph_qso
    df_out.ix[rinx,'cw_qso']        = cw_qso
    df_out.ix[rinx,'cw_qso_pts']    = cw_qso*2

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
# Prepare a DataFrame derived from various tables in the hamsci_rsrch database.
# -----------------------------------------------------------------------------

df_list = []
last    = 0

for idx_a, result_a in enumerate(query('SELECT submitter_id, callsign, ground_conductivity, dsn_fname FROM seqp_submissions')):
    last = idx_a
    row_dct = OrderedDict()
    row_dct['call']         = result_a[1].upper()
    row_dct['g_con']        = result_a[2]
    row_dct['dsn_fname']    = result_a[3]
    # TODO: Make a for loop for this.
    row_dct['an_has_160']   = 0
    row_dct['an_has_80']    = 0
    row_dct['an_has_40']    = 0
    row_dct['an_has_20']    = 0
    row_dct['an_has_15']    = 0
    row_dct['an_has_10']    = 0
    row_dct['an_has_6']     = 0
    row_dct['sk_has_160']   = 0
    row_dct['sk_has_80']    = 0
    row_dct['sk_has_60']    = 0
    row_dct['sk_has_40']    = 0
    row_dct['sk_has_30']    = 0
    row_dct['sk_has_20']    = 0
    row_dct['sk_has_17']    = 0
    row_dct['sk_has_15']    = 0
    row_dct['sk_has_12']    = 0
    row_dct['sk_has_10']    = 0
    row_dct['sk_has_6']     = 0
    row_dct['wb_has_160']   = 0
    row_dct['wb_has_80']    = 0
    row_dct['wb_has_60']    = 0
    row_dct['wb_has_40']    = 0
    row_dct['wb_has_30']    = 0
    row_dct['wb_has_20']    = 0
    row_dct['wb_has_17']    = 0
    row_dct['wb_has_15']    = 0
    row_dct['wb_has_12']    = 0
    row_dct['wb_has_10']    = 0
    row_dct['wb_has_6']     = 0

    results = query('SELECT submitter_id, has_160, has_80, has_40, has_20, has_15, has_10, has_6, erp FROM seqp_antennas WHERE submitter_id={!s}'.format(result_a[0]))
    for result_b in results:
        if not num_gtz(result_b[8]):
            continue
        row_dct['an_has_160']  += result_b[1]
        row_dct['an_has_80']   += result_b[2]
        row_dct['an_has_40']   += result_b[3]
        row_dct['an_has_20']   += result_b[4]
        row_dct['an_has_15']   += result_b[5]
        row_dct['an_has_10']   += result_b[6]
        row_dct['an_has_6']    += result_b[7]

    results = query('SELECT submitter_id, mode, has_160, has_80, has_60, has_40, has_30, has_20, has_17, has_15, has_12, has_10, has_6 FROM seqp_skimmers WHERE submitter_id={!s}'.format(result_a[0]))
    for result_c in results:
        if not num_gtz(result_b[8]):
            continue
        row_dct['sk_has_160']  += result_c[2]
        row_dct['sk_has_80']   += result_c[3]
        row_dct['sk_has_60']   += result_c[4]
        row_dct['sk_has_40']   += result_c[5]
        row_dct['sk_has_30']   += result_c[6]
        row_dct['sk_has_20']   += result_c[7]
        row_dct['sk_has_17']   += result_c[8]
        row_dct['sk_has_15']   += result_c[9]
        row_dct['sk_has_12']   += result_c[10]
        row_dct['sk_has_10']   += result_c[11]
        row_dct['sk_has_6']    += result_c[12]

    results = query('SELECT submitter_id, doi, has_160, has_80, has_60, has_40, has_30, has_20, has_17, has_15, has_12, has_10, has_6 FROM seqp_wideband WHERE submitter_id={!s}'.format(result_a[0]))
    for result_d in results:
        if not num_gtz(result_b[8]):
            continue
        row_dct['wb_has_160']  += result_d[2]
        row_dct['wb_has_80']   += result_d[3]
        row_dct['wb_has_60']   += result_d[4]
        row_dct['wb_has_40']   += result_d[5]
        row_dct['wb_has_30']   += result_d[6]
        row_dct['wb_has_20']   += result_d[7]
        row_dct['wb_has_17']   += result_d[8]
        row_dct['wb_has_15']   += result_d[9]
        row_dct['wb_has_12']   += result_d[10]
        row_dct['wb_has_10']   += result_d[11]
        row_dct['wb_has_6']    += result_d[12]
    df_list.append(row_dct)
df_sub = pd.DataFrame(df_list)

df_sub.sort_values(by = ['call']).reset_index(drop = True, inplace = True)

keys = []
keys.append('sk_has_160')
keys.append('sk_has_80')
keys.append('sk_has_60')
keys.append('sk_has_40')
keys.append('sk_has_30')
keys.append('sk_has_20')
keys.append('sk_has_17')
keys.append('sk_has_15')
keys.append('sk_has_12')
keys.append('sk_has_10')
keys.append('sk_has_6')
sk = keys.copy()

keys = []
keys.append('wb_has_160')
keys.append('wb_has_80')
keys.append('wb_has_60')
keys.append('wb_has_40')
keys.append('wb_has_30')
keys.append('wb_has_20')
keys.append('wb_has_17')
keys.append('wb_has_15')
keys.append('wb_has_12')
keys.append('wb_has_10')
keys.append('wb_has_6')
wb = keys.copy()

keys = []
keys.append('an_has_160')
keys.append('an_has_80')
keys.append('an_has_40')
keys.append('an_has_20')
keys.append('an_has_15')
keys.append('an_has_10')
keys.append('an_has_6')
an = keys.copy()
print('Additional DataFrame created...')

# -----------------------------------------------------------------------------
# BONUS 4: Add 50 points if ground conductivity is greater than 0.
# -----------------------------------------------------------------------------
# BONUS 5: Add 100 points if a filename exists for a callsign in the SQL table.
# TODO: Blacklist?
# -----------------------------------------------------------------------------
# BONUS 6: Add 50 points per band for all submitted antennas that contain a
# submitted ERPD value which is greater than 0 (automatically done earlier).
# -----------------------------------------------------------------------------
# BONUS 7: Add 50 points per band and per mode for all submitted skimmers.
# -----------------------------------------------------------------------------
# BONUS 8: Add 50 points per band for all submitted Zenodo DOIs.
# -----------------------------------------------------------------------------

for idx_a, row_a in df_out.iterrows():
    for idx_b, row_b in df_sub.iterrows():
        if (row_a['call'] == row_b['call'] and
        num_gtz(row_b['g_con'])):
            df_out.ix[idx_a, 'ground_conductivity'] = 50
        if (row_a['call'] == row_b['call'] and
        row_b['dsn_fname'] != None):
            df_out.ix[idx_a, 'antenna_design'] = 100
        if (row_a['call'] == row_b['call']):
            df_out.ix[idx_a, 'erpd'] = (int(bool(row_b['an_has_160'])) + \
            int(bool(row_b['an_has_80'])) + int(bool(row_b['an_has_40'])) + \
            int(bool(row_b['an_has_20'])) + int(bool(row_b['an_has_15'])) + \
            int(bool(row_b['an_has_10'])) + int(bool(row_b['an_has_6']))) * 50
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            df_out.ix[idx_a, 'skimmers'] = (int(bool(row_b['sk_has_160'])) + \
            int(bool(row_b['sk_has_80'])) + int(bool(row_b['sk_has_60'])) + \
            int(bool(row_b['sk_has_40'])) + int(bool(row_b['sk_has_30'])) + \
            int(bool(row_b['sk_has_20'])) + int(bool(row_b['sk_has_17'])) + \
            int(bool(row_b['sk_has_15'])) + int(bool(row_b['sk_has_12'])) + \
            int(bool(row_b['sk_has_10'])) + int(bool(row_b['sk_has_6']))) * 50
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            df_out.ix[idx_a, 'iq_data'] = (int(bool(row_b['wb_has_160'])) + \
            int(bool(row_b['wb_has_80'])) + int(bool(row_b['wb_has_60'])) + \
            int(bool(row_b['wb_has_40'])) + int(bool(row_b['wb_has_30'])) + \
            int(bool(row_b['wb_has_20'])) + int(bool(row_b['wb_has_17'])) + \
            int(bool(row_b['wb_has_15'])) + int(bool(row_b['wb_has_12'])) + \
            int(bool(row_b['wb_has_10'])) + int(bool(row_b['wb_has_6']))) * 50

print('Completed scoring for Bonuses 4-8...')

            
# -----------------------------------------------------------------------------
# Bonus Rule Number 9
# -----------------------------------------------------------------------------
print('Working on Bonus 9 (Spot Bonus)')
tf_pskr = df['source'] == 'pskreporter'
tf_rbn  = df['source'] == 'rbn'
tf      = np.logical_or.reduce( (tf_pskr,tf_rbn) )
df_spot = df[tf].copy()
df_spot = df_spot.dropna(subset=['grid_0'])

grid_4char = []
print('    Converting to 4 char grids for df_spot')
for idx, row in tqdm.tqdm(df_spot.iterrows(),total=len(df_spot)):
    try:
        s = str(row['grid_0'])[:4]
        grid_4char.append(s)
    except:
        grid_4char.append(None)
df_spot['grid_0_4char'] = grid_4char
df_spot = df_spot.dropna(subset=['grid_0_4char'])

sTime   = datetime.datetime(2017,8,21,14)
print('   Computing Spot Bonus')
for idx_a, row_a in tqdm.tqdm(df_out.iterrows(),total=len(df_out)):
    spot_bonus  = 0
    call        = row_a['call']
    grid        = row_a['grid']
    for band in bands:
        tf              = np.logical_and(df_spot['call_1'] == call,df_spot['band'] == band)
        df_call_band    = df_spot[tf]

        for hour in range(8):
            t_0 = sTime + datetime.timedelta(hours=hour)
            t_1 = t_0 + datetime.timedelta(hours=1)

            tf  = np.logical_and(df_call_band['datetime'] >= t_0,df_call_band['datetime'] < t_1)
            if np.count_nonzero(tf) == 0:
                continue
            df_tmp  = df_call_band[tf]
            spotted_grids   = df_tmp.grid_0_4char.unique().tolist()
            if grid in spotted_grids:
                spotted_grids.remove(grid)

            spot_bonus += len(spotted_grids)

    df_out.ix[idx_a, 'spot_bonus'] = spot_bonus


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
                          df_out['ground_conductivity'] + \
                          df_out['antenna_design'] + df_out['erpd'] + \
                          df_out['skimmers'] + df_out['iq_data'] +\
                          df_out['spot_bonus']
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
keys.append('antenna_design')
keys.append('erpd')
keys.append('skimmers')
keys.append('iq_data')
keys.append('spot_bonus')
keys.append('total')

print('Columns reorganized...')

# -----------------------------------------------------------------------------
# Export the DataFrame to a CSV file, 'seqp_scores.csv'.
# -----------------------------------------------------------------------------

df_out = df_out[keys].copy()
df_out.to_csv('seqp_scores.csv',index=False)

print('Output CSV exported successfully!')

import ipdb; ipdb.set_trace()
