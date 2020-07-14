#!/usr/bin/env python

import ConfigParser
import psycopg2
import sys
import os
import io
import re
from stat import *
from config import (ConfigSectionMap)
from database import (query_database, get_last_timestamp, record_datafile_status, get_datafile_status, create_database_connection)
from storage import (open_dataverse_file)
#from backup import (backup_file)
#from email_notification import (send_notification)
import argparse

def ls(args):
	exit(1)

def mv(args):
	exit(1)

def fsck(args):
	filepaths=get_all_filepaths()
	for f in filepaths:
		try:
			if not S_ISREG(os.stat(f['path']).st_mode):
				print f['path'] + " is not a normal file"
		except:
			print "cannot stat " + f['path'] + " id: " + f['id']

def get_storage_paths():
	out=os.popen("./list_storages.sh").read()
	print out
	result={}
	for line in out.splitlines():
		l=line.split(' ')
		result[l[0]]=l[1]+'/'
	return result

def get_records_for_query(query):
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	cursor.execute(query)
	records = cursor.fetchall()
	return records

def get_all_filepaths():
	storagepaths=get_storage_paths()

	dataverse_query="""SELECT f.id, REGEXP_REPLACE(f.storageidentifier,'^([^:]*)://.*','\\1'), REGEXP_REPLACE(s.storageidentifier,'^[^:]*://','') || '/' || REGEXP_REPLACE(f.storageidentifier,'^[^:]*://','')
	                   FROM dvobject f, dvobject s
	                   WHERE f.dtype='DataFile' AND f.owner_id=s.id"""
	records=get_records_for_query(dataverse_query)
	result=[]
	for r in records:
		result.append({'id':r[0],'path':storagepaths[r[1]]+r[2]})
	return result

def main():
	commands={
		"list" : ls,
		"move" : mv,
		"fsck" : fsck,
	}
#	print commands.keys()

	argv = sys.argv[2:]
	ap = argparse.ArgumentParser()
	ap.add_argument("command", choices=commands.keys(), help="what to do")
	ap.add_argument("-n", "--name", required=False, help="name of the object")
	ap.add_argument("-i", "--id", required=False, help="id of the object")
#	ap.add_argument("-f", "--from", required=False, help="only consider entries after this date")
	args = vars(ap.parse_args())
#	opts, args = getopt.getopt(argv, 'type:id:name:')

#	print args
	commands[args['command']](args)


if __name__ == "__main__":
	main()


