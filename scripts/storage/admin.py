#!/usr/bin/env python

import ConfigParser
import psycopg2
import sys
import os
import io
import re
import pprint
import argparse
from stat import *
from config import (ConfigSectionMap)
from database import (query_database, get_last_timestamp, record_datafile_status, get_datafile_status, create_database_connection)
from storage import (open_dataverse_file)
from shutil import copyfile

### list dataverses/datasets/datafiles in a storage
### TODO: display some statistics
def getList(args):
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
		if args['ownerid'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id="+args['ownerid']+")"
		elif args['ownername'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id IN (SELECT id FROM dataverse WHERE alias='"+args['ownername']+"'"+"))"
#		else:
#			end=" GROUP BY ds1.id,dvo1.identifier"
		if args['storage'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '"+args['storage']+"://%')"
		q+=end
	elif args['type']=='datafile' or args['type'] is None:
		q="SELECT id, directorylabel, label, filesize, owner_id FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE true"
		if args['ownerid'] is not None:
			q+=" AND owner_id="+args['ownerid']
#		elif args['ownername'] is not None:
#			q+=
		if args['storage'] is not None:
			q+=" AND storageidentifier LIKE '"+args['storage']+"://%' ORDER BY owner_id"
	print q
	records=get_records_for_query(q)
	return records

def ls(args):
	records=getList(args)
	for r in records:
		print r

def moveFile(row,path,destStoragePath,destStorageName):
#	print row
#	print path
	src=path[0]+path[1]
	dst=destStoragePath+path[1]
	if src==dst:
		print "skipping "+src+", as already in storage "+destStorageName
		return
	if not os.path.exists(src):
		print "skipping non-existent file is "+src
		return
	dstDir=re.sub('/[^/]*$','',dst)
	if not os.path.exists(dstDir):
		print "creating "+dstDir
		os.mkdir(dstDir)
	print "copying from "+src+" to "+dst
	copyfile(src, dst)
	query="UPDATE dvobject SET storageidentifier=REGEXP_REPLACE(storageidentifier,'^[^:]*://',%s||'://') WHERE id=%s"
	print "updating database: "+query+" "+str((destStorageName,row[0]))
	sql_update(query,(destStorageName,row[0]))
	print "removing original file "+src
	os.remove(src)

def mv(args):
	if args['to_storage'] is None:
		print "--to-storage is missing"
		exit(1)
	storagePaths=get_storage_paths()
	if args['to_storage'] not in storagePaths:
		print args['to_storage']+" is not a valid storage. Valid storages:"
		pprint.PrettyPrinter(indent=4,width=10).pprint(storagePaths)
		exit(1)
	filesToMove=getList(args)
	filePaths=get_filepaths(idlist=[str(x[0]) for x in filesToMove],separatePaths=True)
	for row in filesToMove:
		moveFile(row,filePaths[row[0]],storagePaths[args['to_storage']],args['to_storage'])

### this is for checking that the files in the database are all there on disk where they should be
def fsck(args):
	if args['ids'] is not None:
		filepaths=get_filepaths(args['ids'].split(','))
	elif args['storage'] is not None or args['ownerid'] is not None:
		filesToCheck=getList(args)
		filepaths=get_filepaths([str(x[0]) for x in filesToCheck])
	else:
		filepaths=get_filepaths()
	for f in filepaths:
		try:
			if not S_ISREG(os.stat(f['path']).st_mode):
				print f['path'] + " is not a normal file"
		except:
			print "cannot stat " + f['path'] + " id: " + str(f['id'])

def get_storage_paths():
	out=os.popen("./list_storages.sh").read()
#	print out
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
	dataverse_db_connection.close()
	return records

def sql_update(query, params):
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	cursor.execute(query, params)
	dataverse_db_connection.commit() 
	dataverse_db_connection.close()

def get_filepaths(idlist=None,separatePaths=False):
	storagepaths=get_storage_paths()

	query="""SELECT f.id, REGEXP_REPLACE(f.storageidentifier,'^([^:]*)://.*','\\1'), REGEXP_REPLACE(s.storageidentifier,'^[^:]*://','') || '/' || REGEXP_REPLACE(f.storageidentifier,'^[^:]*://','')
	         FROM dvobject f, dvobject s
	         WHERE f.dtype='DataFile' AND f.owner_id=s.id"""
	if idlist is not None:
		query+=" AND f.id IN("+','.join(idlist)+")"
	if not idlist:
		return {}
	records=get_records_for_query(query)
	result={}
	for r in records:
		if separatePaths:
			result.update({r[0] : (storagepaths[r[1]],r[2])})
		else:
			result.update({r[0] : storagepaths[r[1]]+r[2]})
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
	ap.add_argument("-d", "--ownername", required=False, help="name of the containing/owner object")
	ap.add_argument("-i", "--ids", required=False, help="id(s) of the object(s), comma separated")
	ap.add_argument("--ownerid", required=False, help="id of the containing/owner object")
	ap.add_argument("-t", "--type", choices=types, required=False, help="type of objects to list/move")
	ap.add_argument("-s", "--storage", required=False, help="storage to list/move items from")
#	ap.add_argument("-f", "--from", required=False, help="move only from the datastore of this name")
	ap.add_argument("--to-storage", required=False, help="move to the datastore of this name, required for move")
	args = vars(ap.parse_args())
#	opts, args = getopt.getopt(argv, 'type:id:name:')

	print args
	commands[args['command']](args)


if __name__ == "__main__":
	main()


