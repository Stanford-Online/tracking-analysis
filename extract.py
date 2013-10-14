#!/usr/bin/env python

import yaml
import pymongo
from bson import json_util
import datetime
import csv
import json
import dateutil.parser
from xlwt import *

courses_file = open("classes.yml", "r")
courses = yaml.load(courses_file)

conn = pymongo.Connection("localhost", 27017)
db = conn["edxtest"]


# Writer Classes

class Writer:
    def __init__(self, course, dataname, formats):
        self.formats = formats
        self.header_backends = []
        self.row_backends = []
        self.final_backends = []

        if 'csv' in self.formats:
            self.csv_outfile = open(course + "-" + dataname + ".csv", "wb")
            self.csv_writer = csv.DictWriter(self.csv_outfile, self.fieldnames, 
                    dialect='excel', extrasaction='ignore')

            def csv_header_backend():
                self.csv_writer.writeheader()
            self.header_backends.append(csv_header_backend)

            def csv_row_backend(data):
                self.csv_writer.writerow(data)
            self.row_backends.append(csv_row_backend)

        if 'json' in self.formats:
            self.json_outfile = open(course + "-" + dataname + ".json", "wb")

            def json_row_backend(data):
                json.dump(data, self.json_outfile, default=json_util.default)
                self.json_outfile.write("\n")
            self.row_backends.append(json_row_backend)

        if 'xls' in self.formats:
            self.xls_workbook = Workbook()
            self.xls_worksheet = self.xls_workbook.add_sheet(dataname)
            self.row = 0

            def xls_header_backend():
                for c in range(len(self.fieldnames)):
                    self.xls_worksheet.write(self.row, c, label=self.fieldnames[c])
                self.row += 1
            self.header_backends.append(xls_header_backend)

            def xls_row_backend(data):
                for c in range(len(self.fieldnames)):
                    try:
                        celldata = data[self.fieldnames[c]]
                        if isinstance(celldata, dict):
                            celldata = str(celldata)
                        self.xls_worksheet.write(self.row, c, label=celldata)
                    except KeyError:
                        pass
                self.row += 1
            self.row_backends.append(xls_row_backend)

            def xls_final():
                self.xls_workbook.save(course + "-" + dataname + ".xls")
            self.final_backends.append(xls_final)

    def writeheader(self):
        for back in self.header_backends:
            back()

    def write(self, data):
        for back in self.row_backends:
            back(data)

    def final(self):
        for back in self.final_backends:
            back()


class TrackingWriter(Writer):
    fieldnames = ["username",
            "session",
            "course_id",
            "event_source",
            "event_type",
            "ip",
            "agent",
            "page",
            "host",
            "time",
            "event",
        ]

class SessionWriter(Writer):
    fieldnames = ["course_id",
            "session",
            "username",
            "first_time",
            "last_time",
            "num_events",
            "session_sec",
        ]


# Collection Handlers

def tracking(db, course, commands):
    writer = TrackingWriter(course, "tracking", commands['formats'])
    writer.writeheader()

    coll = db["tracking"]
    selector = {"course_id": commands['course_id']}
    curs = coll.find(selector)
    for rec in curs:
        del rec["load_date"]
        del rec["load_file"]
        # rec['time'] = str(dateutil.parser.parse(rec['time']))
        writer.write(rec)
    writer.final()

def session(db, course, commands):
    writer = SessionWriter(course, "session", commands['formats'])
    writer.writeheader()

    coll = db["session"]
    selector = {"_id.course_id": commands['course_id']}
    curs = coll.find(selector)
    for rec in curs:
        flatrec = {}
        for k,v in rec.iteritems():
            flatrec.update(v)
        # flatrec['first_time'] = str(flatrec['first_time'])
        # flatrec['last_time'] = str(flatrec['last_time'])
        writer.write(flatrec)
    writer.final()

# Main

for course, commands in courses.iteritems():
    if 'course_id' not in commands:
        commands['course_id'] = course
    
    tracking(db, course, commands)
    session(db, course, commands)

