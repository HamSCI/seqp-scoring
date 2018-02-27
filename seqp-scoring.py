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
csv_in = pd.read_csv('seqp_all_ctyChecked.csv')

# Create an output Pandas DataFrame.
csv_out = pd.DataFrame(columns = [
    'call',
    'qso_vox', 'qso_dig', 'qso_all',
    'gs_1.8', 'gs_3.5', 'gs_7', 'gs_14', 'gs_21', 'gs_28', 'gs_all',
    'score'
])
print('Finished initializations...')

# Set the location (row/index) counter equal to 0.
global loc_counter
loc_counter = 0

# Calculate a score for Rule 1:
# Use the is_valid function to check if the entry is valid.
# Use the load function to check if the callsign has been loaded.
# Award points to the callsign accordingly.
for idx, row in csv_in.iterrows():
    if row['mode'] == 'PH':
        print('Starting judgement for', idx, row['mode'])
        if is_valid('PH', row['call_0'], row['call_1'],
        row['band'], row['datetime']):
            loc_call = load(row['call_0'])
            csv_out.ix[loc_call, 'qso_vox'] += 1
            csv_out.ix[loc_call, 'qso_all'] += 1
            print(csv_out)
    elif row['mode'] != 'WSPR':
        print('Starting judgement for', idx, row['mode'])
        if is_valid(row['mode'], row['call_0'], row['call_1'], 
        row['band'], row['datetime']):
            loc_call = load(row['call_0'])
            csv_out.ix[loc_call, 'qso_dig'] += 2
            csv_out.ix[loc_call, 'qso_all'] += 2
            print(csv_out)

# To-do:
    # Vectorize
    # Rule 2

# For loop for every submitted callsign or
# For loop for every band
# Group based on mode
# Duplicate QSOs ... timedelta between all duplicate qsos. Throw away the invalid QSOs.