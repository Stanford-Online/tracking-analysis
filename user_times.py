#!/usr/bin/env python

"""
Assumes that the user_summary collection exists has been populated
by the user_session.js script.  user_session has records like this:

    {
        "_id" : {
            "course_id" : "Education/EDUC115N/How_to_Learn_Math",
            "username" : "<redacted>"
        },
        "num_events" : 594,
        "session_sec" : 26324.672,
        "sessions" : 9
    }


Writes rows like this to csv file:

     username,num_events,sessions,session_sec
     redacted1,97,7,6867.563
     redacted2,87,10,5493.694
     redacted3,111,5,4969.7017
"""

from pymongo import Connection
import csv

outfile = open('user_times.csv', 'w')
csvout = csv.writer(outfile, dialect='excel')
csvout.writerow(['username', 'num_events', 'sessions', 'session_sec'])

excludes = ['mtuttle', 'caesar2164', 'NicholasJenkins', 'kimhwrth', 'gbruhns']

conn = Connection('localhost', 27017)
db = conn['edx']
user_summary = db['user_summary']

curs = user_summary.find({'_id.course_id': "Stanford/2013/Three_Books"})\
        .sort('session_sec',-1)

for row in curs:
    if row['_id']['username'] in excludes:
        continue
    print row
    csvout.writerow([row['_id']['username'],
            int(row['num_events']),
            int(row['sessions']),
            float(row['session_sec']),
            ])
    
