#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#-------------------------------------------------------------------------------
# Name:        ora2csv.py
# Purpose:     Export data from Oracle Database to plain file
# Author:      Popov Pavel (schmooser@gmail.com)
# Created:     06.03.2012
#-------------------------------------------------------------------------------

import sys
import time

def usage():
  print '''Tool for exporting data in CSV from Oracle Database.
Usage: ora2csv.py connection_string query_filename output_filename'''
  print 'Current parameters:'
  print sys.argv

def main():
  if len(sys.argv) <> 4:
    usage()
    return
  connection_string = sys.argv[1]
  query_filename = sys.argv[2]
  output_filename = sys.argv[3]
  start_time = time.time()
  export(connection_string, query_filename, output_filename)
  print 'Elapsed %d seconds' % round(time.time() - start_time)

def export(connection_string, query_filename, output_filename):
  import csv
  import cx_Oracle

  db = cx_Oracle.connect(connection_string)
  cursor = db.cursor()

  s = open(query_filename, 'r')
  f = open(output_filename, 'w')

  writer = csv.writer(f
                     , delimiter = '\t'
                     , lineterminator='\n'
                     , quotechar="'"
                     , quoting=csv.QUOTE_NONE)

  r = cursor.execute(s.read().strip(' ;\r\n'))

  column_names = []
  for i in range(0, len(cursor.description)):
    column_names.append(cursor.description[i][0])
  writer.writerow(column_names)

  i = 0
  for row in cursor:
    writer.writerow(row)
    i += 1
    if i%100000 == 0:
      print '%d rows downloaded' % i

  print 'Done: %d rows downloaded' % i

  f.close()
  s.close()

if __name__ == '__main__':
    main()
