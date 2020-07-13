#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 19:36:52 2020

@author: lenakim
"""

import os
import mysql.connector
import csv
import sys

os.chdir('/Users/lenakim/Documents/USC/2020_Spring/INF_551/hw3')


#gloabl variables
HOST = 'localhost'
ID = 'inf551'
PW = 'inf551'
DB = 'world'

#connect to database
connect = mysql.connector.connect(

        host = HOST,
        user = ID,
        password = PW,
        database = DB
    )

cursor = connect.cursor(buffered = True)

#get table names
cursor.execute('show tables')
tables = []
for x in cursor:
    tables.append(str(x)[2 : -3])

#write to csv
for table in tables:
    header = []
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'")
    for c in cursor:
        header.append(str(c)[2: -3])

    file_path = table + '.csv'
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        cursor.execute('SELECT * FROM ' + table)
        for cur in cursor:
            writer.writerow(cur)
        print("exporting " + table + " completed")
