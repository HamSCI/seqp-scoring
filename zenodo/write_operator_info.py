#!/usr/bin/env python3
import string
from collections import OrderedDict
import shutil,os
import mysql.connector
import pandas as pd

import seqp

pd.set_option('display.max_rows', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 500)

def update_call(df,submitter_id,callsign):
    tf = df['submitter_id'] == submitter_id
    df.loc[tf,'callsign'] = [callsign]
    return df

def delete_station(df,submitter_id):
    tf = df['submitter_id'] != submitter_id
    df  = df[tf].copy()
    return df

def grid_case(grid):
    """
    Correct case of grid sqaures.
    """
    try:
        grid = grid[:2].upper() + grid[2:]
        grid = grid[:-2] + grid[-2:].lower()
    except:
        pass
    return grid


def format_filename(s):
    """Take a string and return a valid filename constructed from the string.
Uses a whitelist approach: any characters not present in valid_chars are
removed. Also spaces are replaced with underscores.
 
Note: this method may produce invalid filenames such as ``, `.` or `..`
When I use this method I prepend a date string like '2009_01_15_19_46_32_'
and append a file extension like '.txt', so I avoid the potential of using
an invalid filename.
 
"""
    s = s.replace('/','-')
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ','_') # I don't like spaces in filenames.
    return filename

def clean_call(callsign):
    call    = callsign.split()[0].replace('/','-')
    return call

log_dir = 'log_files'
if os.path.exists(log_dir):
    shutil.rmtree(log_dir)
os.makedirs(log_dir)

dsn_dir = 'station_descriptions'
if os.path.exists(dsn_dir):
    shutil.rmtree(dsn_dir)
os.makedirs(dsn_dir)

user        = 'hamsci'
password    = 'hamsci'
host        = 'localhost'
database    = 'hamsci_rsrch'
db          = mysql.connector.connect(user=user,password=password,host=host,database=database,buffered=True,use_pure=True)

#cols = []
#cols.append('submitter_id')
#cols.append('has_multi')
#cols.append('first_name')
#cols.append('last_name')
#cols.append('is_multi')
#cols.append('club_name')
#cols.append('callsign')
#cols.append('email')
#cols.append('per_gs')
#cols.append('radio_model')
#cols.append('power')
#cols.append('is_tot')
#cols.append('is_out')
#cols.append('is_pub')
#cols.append('ground_conductivity')
#cols.append('submitted_log')
#cols.append('submitted_dsn')
#cols.append('log_fname')
#cols.append('dsn_fname')
#cols.append('comment')
#cols.append('entered')
#qry     = ("SELECT {!s} FROM seqp_submissions;".format(','.join(cols)))

qry     = ("SELECT * FROM seqp_submissions;")
crsr    = db.cursor()
crsr.execute(qry)
results = crsr.fetchall()
columns = crsr.column_names
crsr.close()

cols = OrderedDict()
cols['submitter_id']    = 'submitter_id'
cols['callsign']        = 'callsign'
cols['per_gs']          = 'gridsquare_submitted'
cols['lat_calculated']  = 'lat_calculated'
cols['lon_calculated']  = 'lon_calculated'
cols['radio_model']     = 'radio_model'
cols['power']           = 'tx_power_watts'
cols['log_fname']       = 'log_filename'
cols['dsn_fname']       = 'station_description_filename'
#cols['entered']         = 'entered'

keys    = dict(zip(columns,range(len(columns))))
df_lst  = []

log_files   = {}
dsn_files   = {}
for result in results:
    tmp = {}
    for col in cols.keys():
        if col in ['lat_calculated','lon_calculated']:
            tmp[col] = 'NaN'
        else:
            tmp[col]    = result[keys[col]]
    df_lst.append(tmp)

    sId             = result[keys['submitter_id']]
    dsn_files[sId]  = result[keys['submitted_dsn']]
    log_files[sId]  = result[keys['submitted_log']]


df  = pd.DataFrame(df_lst)
df  = df[list(cols.keys())].copy()

# Remove log filenames for IDs <= 293 due to a collection bug.
tf  = df.submitter_id <= 293
df.loc[tf,'log_fname'] = None


# Capitalize
df['callsign']  = df['callsign'].apply(lambda x: x.upper())

# Drop duplicates based on callsign (keep last/most up-to-date entry)
df  = df.drop_duplicates('callsign',keep='last')

# Fix obvious errors.
update_call(df,382,'W0ECC')
update_call(df,968,'WG4FOC')
update_call(df,881,'AF0E7')
update_call(df,332,'AC0PR')
update_call(df,344,'N0UV')
update_call(df,542,'K0VH')
update_call(df,774,'WB0IXV')

# Delete ones we know are wrong and have no chance of being helpful.
df  = delete_station(df,171)
df  = delete_station(df,32)
df  = delete_station(df,785)
df  = delete_station(df,527)

# Calculate Grid Square
df_lst  = []
for rinx,row in df.iterrows():
    row['per_gs'] = grid_case(row['per_gs'])

    try:
        latlon  = seqp.locator.gridsquare2latlon(row['per_gs'])
        row['lat_calculated']   = '{:.04f}'.format(latlon[0])
        row['lon_calculated']   = '{:.04f}'.format(latlon[1])
    except:
        pass
    df_lst.append(row)
df  = pd.DataFrame(df_lst)

# Reset the index
df  = df.sort_values('callsign')
df.index    = range(len(df))
df_lst  = []
for rinx,row in df.iterrows():
    callsign    = clean_call(row['callsign'])
    pfx         = '{:03d}_{!s}'.format(rinx,callsign)
    sId             = row['submitter_id']
    log_fname       = row['log_fname']
    log_file        = log_files[sId]

    if log_fname is not None:
        fname   = format_filename('{!s}_{!s}'.format(pfx,log_fname))
        fpath   = os.path.join(log_dir,fname)
        row['log_fname'] = fname
        print(fpath)
        with open(fpath,'wb') as fl:
            fl.write(log_file)

    dsn_fname       = row['dsn_fname']
    dsn_file        = dsn_files[sId]

    if dsn_fname is not None:
        fname   = format_filename('{!s}_{!s}'.format(pfx,dsn_fname))
        fpath   = os.path.join(dsn_dir,fname)
        row['dsn_fname'] = fname
        print(fpath)
        with open(fpath,'wb') as fl:
            fl.write(dsn_file)
    df_lst.append(row)

df  = pd.DataFrame(df_lst)
df  = df[list(cols.keys())].copy()
df.index.name = 'index'

del df['submitter_id']
df  = df.rename(columns=cols)
df.to_csv('station_info.csv')
import ipdb; ipdb.set_trace()
