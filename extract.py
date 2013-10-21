#!/usr/bin/env python

import sys
import yaml
import pymongo
from bson import json_util
import datetime
import dateutil.parser
from collections import OrderedDict

import csv
import json
from xlwt import *

COURSES_FILENAME = "courses.yml"
MONGO_HOST = "localhost"
MONGO_PORT = "27017"

if len(sys.argv) <> 2 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
    sys.stderr.write("usage: %s dbname\n" % sys.argv[0])
    sys.exit(1)
dbname = sys.argv[1]

sys.stderr.write("Connectng to mongodb: %s:%s\n" % (MONGO_HOST, MONGO_PORT) )
conn = pymongo.Connection(MONGO_HOST, int(MONGO_PORT))
db = conn[dbname]

sys.stderr.write("Configuration file: %s\n" % COURSES_FILENAME)
courses_file = open(COURSES_FILENAME, "r")
courses = yaml.load(courses_file)


# Writer Classes

class Writer:
    def __init__(self, course, dataname, formats):
        self.formats = formats
        self.header_backends = []
        self.row_backends = []
        self.final_backends = []

        basename = course + "-" + dataname 
        sys.stderr.write("Writing " + basename + ": ")

        if 'csv' in self.formats:
            sys.stderr.write("csv ")
            self.csv_outfile = open(basename + ".csv", "wb")
            self.csv_writer = csv.DictWriter(self.csv_outfile, self.fieldnames.keys(), 
                    dialect='excel', extrasaction='ignore')

            def csv_header_backend():
                self.csv_writer.writeheader()
            self.header_backends.append(csv_header_backend)

            def csv_row_backend(data):
                self.csv_writer.writerow(data)
            self.row_backends.append(csv_row_backend)

        if 'json' in self.formats:
            sys.stderr.write("json ")
            self.json_outfile = open(basename + ".json", "wb")

            def json_row_backend(data):
                json.dump(data, self.json_outfile, default=json_util.default)
                self.json_outfile.write("\n")
            self.row_backends.append(json_row_backend)

        if 'xls' in self.formats:
            sys.stderr.write("xls ")
            self.xls_workbook = Workbook()
            self.xls_worksheet = self.xls_workbook.add_sheet(dataname)
            self.row = 0
            self.datestyle = XFStyle()
            self.datestyle.num_format_str = "M/D/YY h:mm"

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
                        style = Style.default_style
                        if isinstance(celldata, datetime.datetime):
                            style = self.datestyle
                        self.xls_worksheet.write(self.row, c, celldata, style)
                    except KeyError:
                        # OK for a field to be missing
                        pass
                    c += 1
                self.row += 1
            self.row_backends.append(xls_row_backend)

            def xls_final():
                self.xls_workbook.save(basename + ".xls")
            self.final_backends.append(xls_final)

        sys.stderr.write("\n")

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
    fieldnames = OrderedDict([
        ("username", None),
        ("session", None),
        ("course_id", None),
        ("event_source", None),
        ("event_type", None),
        ("ip", None),
        ("agent", None),
        ("page", None),
        ("host", None),
        ("time", None),
        ("event", str),
        ])

class SessionWriter(Writer):
    fieldnames = OrderedDict({
        ("course_id", None),
        ("session", None),
        ("username", None),
        ("first_time", None),
        ("last_time", None),
        ("num_events", None),
        ("session_sec", None),
        })

class CourseUserActivityWriter(Writer):
    fieldnames = OrderedDict([
        ("course_id", None),
        ("username", None),
        ("date", None),
        ("event_source", None),
        ("event_type", None),
        ("id", None),
        ("display_name", None),
        ("count", None),
        ("detail", None),
        ("detail_more", None),
        ])

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
        writer.write(flatrec)
    writer.final()

def course_user_activity(db, course, commands):
    writer = CourseUserActivityWriter(course, "activity", commands['formats'])
    writer.writeheader()

    coll = db["course_user_activity"]
    selector = {"_id.course_id": commands['course_id']}
    curs = coll.find(selector)
    for rec in curs:
        flatrec = {}
        flatrec.update(rec["_id"])
        flatrec["count"] = rec["value"]
        flatrec["display_name"] = display_name(db, rec["_id"]["id"])
        writer.write(flatrec)
    writer.final()

# Lookup 
def display_name(db, idstr):
    modulestore = db["modulestore"]
    curs = modulestore.find({"_id.name": idstr}, 
            fields={"_id": False, "metadata": True})
    for rec in curs:
        return rec["metadata"]["display_name"]


# Main

sys.stderr.write("ensureIndex on course_user_activity._id.course_id\n")
coll=db["course_user_activity"]
coll.ensure_index([("_id.course_id", pymongo.ASCENDING)], background=True)

for course, commands in courses.iteritems():
    if 'course_id' not in commands:
        commands['course_id'] = course
    tracking(db, course, commands)
    session(db, course, commands)
    course_user_activity(db, course, commands)

