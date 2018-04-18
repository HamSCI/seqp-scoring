#!/usr/bin/env python3
import string
import shutil,os
import mysql.connector

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

ant_dir = 'antenna_pdfs'
if os.path.exists(ant_dir):
    shutil.rmtree(ant_dir)
os.makedirs(ant_dir)

user        = 'hamsci'
password    = 'hamsci'
host        = 'localhost'
database    = 'hamsci_rsrch'
db          = mysql.connector.connect(user=user,password=password,host=host,database=database,buffered=True)

qry     = ("SELECT * FROM seqp_submissions ")
crsr    = db.cursor()
crsr.execute(qry)
results = crsr.fetchall()
columns = crsr.column_names
crsr.close()

keys    = dict(zip(columns,range(len(columns))))
for result in results:
    callsign        = result[keys['callsign']]
    dsn_fname       = result[keys['dsn_fname']]
    submitted_dsn   = result[keys['submitted_dsn']]
    if dsn_fname is not None:
        print(callsign,dsn_fname)

        
        fname   = format_filename('{!s}_{!s}'.format(callsign.upper(),dsn_fname))
        fpath   = os.path.join(ant_dir,fname)
        with open(fpath,'wb') as fl:
            fl.write(submitted_dsn)
