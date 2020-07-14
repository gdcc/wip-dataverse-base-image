#!/usr/bin/env python

import ConfigParser
import psycopg2
import sys
import io
import re
from config import (ConfigSectionMap)
from database import (query_database, get_last_timestamp, record_datafile_status, get_datafile_status)
from storage import (open_dataverse_file)
from backup import (backup_file)
from email_notification import (send_notification)
import argparse

def main():

#	command = sys.argv[1]

	commands={
		"list" : ls(),
		"move" : mv(),
		"fsck" : fsck(),
	}

	argv = sys.argv[2:]
	ap = argparse.ArgumentParser()
	ap.add_argument("command", required=True, choices=list(commands.keys()), help="what to do")
	ap.add_argument("-n", "--name", required=False, help="name of the object")
	ap.add_argument("-i", "--id", required=False, help="id of the object")
#	ap.add_argument("-f", "--from", required=False, help="only consider entries after this date")
	args = vars(ap.parse_args())
#	opts, args = getopt.getopt(argv, 'type:id:name:')

	commands[command]()


if __name__ == "__main__":
	main()

def fsck():
	print get_all_filepaths()

def get_all_filepaths():
	global dataverse_db_connection
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	dataverse_query="SELECT REGEXP_REPLACE(s.storageidentifier,'^[^:]*://','') || REGEXP_REPLACE(f.storageidentifier,'^[^:]*://','')
	                 FROM dvobject f, dvobject s
	                 WHERE f.dtype='DataFile' AND f.owner_id=s.id"
	cursor.execute(dataverse_query)
	records = cursor.fetchall()
	return records
