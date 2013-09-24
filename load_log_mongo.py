#!/usr/bin/env python -u

'''
Load tracking logs into mongo using pymongo driver.  Correctly
handles the "event" fields (unjson-ifying it) and creates the course_id
    Created on Nov 8, 2012
    @author: jm, edx.org

usage: ./load_log_mongo.py DB COLL f1 f2

Supports multiple file names, globbed file names, and gzipped files.
'''

from pymongo import Connection
import json
import glob
import sys
import gzip
import datetime

def get_course_id(event):
    """
    get course id from page url field of an event
    """
    course_id = None
    if event['event_source'] == 'server':
        # get course_id from event type
        if event['event_type'] == '/accounts/login/':
            s = event['event']['GET']['next'][0]
        else:
            s = event['event_type']
    else:
        s = event['page']
    if s:
        a = s.split('/')
        if 'courses' in a:
            i = a.index('courses')
            course_id = "/".join(map(str, a[i+1:i+4]))
    return course_id

def canonical_name(filepath):
    """
    Save only the filename and the subdirectory it is in, strip off all prior 
    paths.  If the file ends in .gz, remove that too.  Convert to lower case.
    """
    fname = '/'.join(filepath.lower().split('/')[-2:])
    if len(fname) > 3 and fname[-3:] == ".gz":
        fname = fname[:-3]
    return fname

def insert_imported(imp, filepath, error=0, good=0):
    """
    File named filepath was imported, so add it to the "imported" collection
    """
    rec = {}
    rec['_id'] = canonical_name(filepath)
    rec['date'] = datetime.datetime.utcnow()
    rec['error'] = error
    rec['good'] = good
    try:
        result = imp.update({'_id': rec['_id']}, rec, upsert=True, safe=True)
    except DuplicateKeyError:
        print ("File already imported: %s", fname)

# MAIN

if len(sys.argv) < 4:
    usage_message = """usage: %s db coll f1 [f2] [f3...]

For one or more files containing edx tracking logs, insert into the
collection given. Files ending .gz they are decompressed on the fly.
Files successfully loaded are tracked in coll_incremental. If already
loaded, skip.
"""
    sys.stderr.write(usage_message % sys.argv[0])
    sys.exit(1)

db_name = sys.argv[1]
collection_name = sys.argv[2]

# Get database connection and collections
connection = Connection('localhost', 27017)
db = connection[db_name]
events = db[collection_name]
imp = db[collection_name+"_imported"]

total_error = 0
total_success = 0

# collect all files from command line
files = []
for i in sys.argv[3::]:     # all remaining arguments are lists of files to process
    for j in glob.glob(i):
        files.append(j)

for logfile_path in sorted(files):
    # if this file has already been imported, skip
    if imp.find({'_id':canonical_name(logfile_path)}).count():
        print "skipping", logfile_path
        continue

    print "loading", logfile_path
    if logfile_path[-3:].lower() == ".gz":
        logfile = gzip.open(logfile_path)
    else:
        logfile = open(logfile_path)

    this_error = 0
    this_success = 0
    for event in logfile:
        try:
            record = json.loads(event)
            for attribute, value in record.iteritems():
                if (attribute == 'event' and value and not isinstance(value, dict)):
                    # hack to load the record when it is encoded as a string
                    record["event"] = json.loads(value)         
            course_id = get_course_id(record)
            if course_id:
                record['course_id'] = course_id
            res = events.insert(record)
        except Exception as e:
            # TODO: handle different types of exceptions
            this_error += 1
        else:
            this_success += 1

        if (this_error + this_success) % 500000 == 0:
            sys.stdout.write("\n")
        elif (this_error + this_success) % 10000 == 0:
            sys.stdout.write(".")

    insert_imported(imp, logfile_path, error=this_error, good=this_success)
    total_error += this_error
    total_success += this_success
    sys.stdout.write("\n")

print "total events read:     %d" % (total_error + total_success)
print "inserted events:       %d" % total_success
print "corrupt events:        %d" % total_error
