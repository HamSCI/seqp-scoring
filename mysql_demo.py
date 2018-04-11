#!/usr/bin/env python3

import mysql.connector
user        = 'hamsci'
password    = 'hamsci'
host        = 'localhost'
database    = 'hamsci_rsrch'
db          = mysql.connector.connect(user=user,password=password,host=host,database=database,buffered=True)

#mysql> describe seqp_submissions;                                                                     
#+---------------------+---------------------+------+-----+-------------------+----------------+
#| Field               | Type                | Null | Key | Default           | Extra          |
#+---------------------+---------------------+------+-----+-------------------+----------------+
#| submitter_id        | bigint(20) unsigned | NO   | PRI | NULL              | auto_increment |
#| has_multi           | tinyint(1)          | YES  |     | NULL              |                |
#| first_name          | varchar(255)        | YES  |     | NULL              |                |
#| last_name           | varchar(255)        | YES  |     | NULL              |                |
#| is_multi            | tinyint(1)          | YES  |     | NULL              |                |
#| club_name           | varchar(255)        | YES  |     | NULL              |                |
#| callsign            | varchar(30)         | YES  |     | NULL              |                |
#| email               | varchar(255)        | YES  |     | NULL              |                |
#| per_gs              | varchar(6)          | YES  |     | NULL              |                |
#| radio_model         | varchar(255)        | YES  |     | NULL              |                |
#| power               | int(11)             | YES  |     | 0                 |                |
#| is_tot              | tinyint(1)          | YES  |     | NULL              |                |
#| is_out              | tinyint(1)          | YES  |     | NULL              |                |
#| is_pub              | tinyint(1)          | YES  |     | NULL              |                |
#| ground_conductivity | float               | YES  |     | -1                |                |
#| submitted_log       | longblob            | YES  |     | NULL              |                |
#| submitted_dsn       | longblob            | YES  |     | NULL              |                |
#| log_fname           | varchar(255)        | YES  |     | NULL              |                |
#| dsn_fname           | varchar(255)        | YES  |     | NULL              |                |
#| comment             | mediumblob          | YES  |     | NULL              |                |
#| entered             | timestamp           | NO   |     | CURRENT_TIMESTAMP |                |
#+---------------------+---------------------+------+-----+-------------------+----------------+
#21 rows in set (0.00 sec)

qry     = ("SELECT callsign,ground_conductivity FROM seqp_submissions ")
           
crsr    = db.cursor()
crsr.execute(qry)
results = crsr.fetchall()
crsr.close()
for result in results:
    print(result)
