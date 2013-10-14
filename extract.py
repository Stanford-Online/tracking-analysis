#!/usr/bin/env python

import yaml
import pymongo
from bson import json_util
import datetime
import csv
import json
import dateutil.parser
from xlwt import *
from collections import OrderedDict

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
            self.csv_writer = csv.DictWriter(self.csv_outfile, self.fieldnames.keys(), 
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
                c = 0
                for field in self.fieldnames.keys():
                    self.xls_worksheet.write(self.row, c, label=field)
                    c += 1
                self.row += 1
            self.header_backends.append(xls_header_backend)

            def xls_row_backend(data):
                c = 0;
                for field, conversion_func in self.fieldnames.iteritems():
                    try:
                        celldata = data[field]
                        if conversion_func is not None:
                            celldata = conversion_func(celldata)
                        self.xls_worksheet.write(self.row, c, label=celldata)
                    except KeyError:
                        # OK for a field to be missing
                        pass
                    c += 1
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


def flatten_date(d):
    return str(dateutil.parser.parse(d))

def iso_date(d):
    return dateutil.parser.parse(d)

class TrackingWriter(Writer):
    fieldnames = OrderedDict({
        "username": None,
        "session": None,
        "course_id": None,
        "event_source": None,
        "event_type": None,
        "ip": None,
        "agent": None,
        "page": None,
        "host": None,
        "time": None,
        "event": str,
        })

class SessionWriter(Writer):
    fieldnames = OrderedDict({
        "course_id": None,
        "session": None,
        "username": None,
        "first_time": None,
        "last_time": None,
        "num_events": None,
        "session_sec": None,
        })


# Collection Handlers

def tracking(db, course, commands):
    writer = TrackingWriter(course, "tracking", commands['formats'])
    writer.writeheader()

    coll = db["tracking"]
    selector = {"course_id": commands['course_id']}
    curs = coll.find(selector)
    for rec in curs:
        del rec["_id"]
        del rec["load_date"]
        del rec["load_file"]
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

