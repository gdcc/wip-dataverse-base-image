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

### list dataverses/datasets/datafiles in a storage
### also display some statistics
def ls(args):
	if args['type']=='dataverse':
		q="""SELECT id, alias, description FROM dataverse"""
		if args['storage'] is not None:
			q+=""" WHERE id IN
			       (SELECT DISTINCT owner_id FROM dataset NATURAL JOIN dvobject WHERE storageidentifier LIKE '"""+args['storage']+"""://%')"""
	elif args['type']=='dataset':
		q="""SELECT ds1.id, dvo1.identifier, sum(filesize) FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id
		     WHERE true"""
#		end=""
		end=" GROUP BY ds1.id,dvo1.identifier"
		if args['dataverseid'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id="+args['dataverseid']+")"
		elif args['dataversename'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id IN (SELECT id FROM dataverse WHERE alias='"+args['dataversename']+"'"+"))"
#		else:
#			end=" GROUP BY ds1.id,dvo1.identifier"
		if args['storage'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '"+args['storage']+"://%')"
		q+=end
	elif args['type']=='datafile':
		q="SELECT id, directorylabel, label, filesize, owner_id FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE true"
		if args['storage'] is not None:
			q+=" AND storageidentifier LIKE '"+args['storage']+"://%' ORDER BY owner_id"
	else:
		q=""
	print q
	records=get_records_for_query(q)
	for r in records:
		print r
	exit(1)

def mv(args):
	exit(1)

### this is for checking that the files in the database are all there on disk where they should be
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
	types=["dataverse","dataset","datafile"]
#	print commands.keys()

	argv = sys.argv[2:]
	ap = argparse.ArgumentParser()
	ap.add_argument("command", choices=commands.keys(), help="what to do")
	ap.add_argument("-n", "--name", required=False, help="name of the object")
	ap.add_argument("-d", "--dataversename", required=False, help="name of the containing dataverse")
	ap.add_argument("-i", "--id", required=False, help="id of the object")
	ap.add_argument("--dataverseid", required=False, help="id of the containing dataverse")
	ap.add_argument("-t", "--type", choices=types, required=False, help="type of objects to list/move")
	ap.add_argument("-s", "--storage", required=False, help="storage to list items from")
#	ap.add_argument("-f", "--from", required=False, help="only consider entries after this date")
	args = vars(ap.parse_args())
#	opts, args = getopt.getopt(argv, 'type:id:name:')

#	print args
	commands[args['command']](args)


if __name__ == "__main__":
	main()


