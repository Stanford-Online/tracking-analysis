#!/usr/bin/env python -u

'''
Load tracking logs into mongo using pymongo driver.  Correctly
handles the "event" fields (unjson-ifying it) and creates the
coures_id
    Created on Nov 8, 2012
    @author: jm, edx.org

usage: ./load_log_mongo.py DB COLL f1 f2

Supports multiple file names, globbed file names, and gzipped 
files.
'''

from pymongo import Connection
import json
import glob
import sys
import gzip

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

db_name = sys.argv[1]
collection_name = sys.argv[2]

# open connection to mongodb 
connection = Connection('localhost', 27017)

db = connection[db_name]
events = db[collection_name]

count = 0
error_count = 0
nb_lines = 0

# collect all files from command line
files = []
for i in sys.argv[3::]:     # all remaining arguments are lists of files to process
    for j in glob.glob(i):
        files.append(j)

for f in sorted(files):
    print "loading ", f
    if f[-3:].lower() == ".gz":
        logfile = gzip.open(f)
    else:
        logfile = open(f)

    for event in logfile:
        nb_lines += 1
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
            count += 1
            if count % 500000 == 0:
                sys.stdout.write("\n")
            if count % 10000 == 0:
                sys.stdout.write(".")
        except Exception as e:
            error_count += 1
    sys.stdout.write("\n")

print "total events read:     %s" % nb_lines
print "inserted events:       %s" % count
print "corrupt events:        %s" % error_count
