#!/bin/usr/python3

import requests
import getpass
import csv
import mariadb
import sys


data_URL='https://msss.gouv.qc.ca/professionnels/statistiques/documents/covid19/COVID19_Qc_RapportINSPQ_HistoVigie.csv'

# GET USER/PASS
#user  = input('Username: ')
#passw = getpass.getpass()

# FETCH DATA
try:
  r = requests.get(data_URL)
except requests.exceptions.RequestException as e:
  raise SystemExit(e)

# EXTRACT DATA
csv_text = r.content.decode('utf-8')
csv_data = csv.reader(csv_text.splitlines(), delimiter=',')
csv_list = list(csv_data)

conn = mariadb.connect(
  user='frufruboubou',
  password='Sbux0HE9ZMMZo3tyV4SoZ991U',
  host="localhost",
  database="openDB")
cur = conn.cursor()

# INSERT DATA
for index, row in enumerate(csv_list):
  try:
    year, month, day = map(int, row[0].split('-'))
  except ValueError as e:
    print('Invalid date at line {}'.format(index))
    continue
  print(row)
  try:
    cur.execute( \
      "INSERT INTO clone_table \
      ( \
        date, \
        nb_case_cumul, \
				nb_case_new, \
				nb_case_active, \
				nb_death_cumul, \
				nb_death_cumul_CHCHSLD, \
				nb_death_cumul_RPA, \
				nb_death_cumul_domInc, \
				nb_death_cumul_other, \
				nb_death_new, \
				nb_death_new_CHCHSLD, \
				nb_death_new_RPA, \
				nb_death_new_domInc, \
				nb_death_new_other \
      ) \
      VALUES \
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
      ( \
        str(row[0]), \
        int(row[1]), \
        int(row[2]), \
        int(row[3]), \
        int(row[4]), \
        int(row[5]), \
        int(row[6]), \
        int(row[7]), \
        int(row[8]), \
        int(row[9]), \
        int(row[10]), \
        int(row[11]), \
        int(row[12]), \
        int(row[13]) \
      ) \
    )
  except mariadb.Error as e:
    print(f"Error: {e}")
    cur.execute( \
      "SELECT * FROM clone_table WHERE date=?", \
      (row[0],) \
    )
    for date,nb_case_new in cur:
      print(f"Date is {date}")

conn.commit()
#cas_confirme_id = 'd2cf4211-5400-46a3-9186-a81e6cd41de9'


# vim: ts=2 sw=2 sts=2 et
