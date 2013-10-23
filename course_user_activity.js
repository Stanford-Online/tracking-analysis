// course_user_activity.js 
//
// Aggregate set of edX tracking logs by course activity.
// WARNING: output collection is clobbered if exists.
//
// Produces records like this:
// {
//    "_id" : {
//        "course_id" : "Education/EDUC115N/How_to_Learn_Math",
//        "event_source" : "browser",
//        "username" : "ladybug",
//        "event_type" : "page_close",
//        "date" : ISODate("2013-10-09T07:00:00Z")
//    },
//    "value" : 10
// }
// 
// Value is the count of events of this type seen that day.
//

// settings
var log_collection_name = "tracking";
var log_collection = db.getCollection(log_collection_name);
var out_collection_name = "course_user_activity";
var out_collection = db.getCollection(out_collection_name);

// index
print("INFO: creating \"load_file\" index on \"" + log_collection_name + "\"");
log_collection.ensureIndex({load_file:1}, {background:1});

// map/reduce functions
var map = function() {

    // if loadfile parameter is specified, then restrict only to those events
    var realValue = function(obj) {
       return obj && obj !== null && obj !== undefined;
    };
    if (typeof(loadfile) != 'undefined' && this.load_file !== loadfile) {
        return;
    }
    var cid = "none";
    if (realValue(this.course_id)) {
        cid = this.course_id;
    };

    // Data Cleanup -- date
    var date_only = function(datetime) {
        if (datetime && datetime !== null && datetime !== undefined) {
            return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
        } else {
            return null;
        }
    };
    if (this.time instanceof Date) {
        timeobj = this.time;
    } else {
        timeobj = new Date(this.time);
    }

    // Data Cleanup -- event_type
    var get_id_from_string = function(s) {
        try {
            if (s !== undefined && s !== null) {
                var id_match = s.match(/[0-9a-f]{32}/i);
                if (Array.isArray(id_match)) {
                    return id_match[0];
                }
            }
        } catch(err) {
            // do nothing in case of error, just don't get an event_id
        }
        return "";
    }
    var detail = "";
    var detail_more = "";
    var id = "";
    if (this.event_type == "problem_graded") {
        detail = this.event[0];
        id = get_id_from_string(detail);
    } else if (this.event_type == "problem_check" || this.event_type == "problem_reset") {
        detail = this.event;
        id = get_id_from_string(detail);
    } else if (this.event_type.slice(0,7) == "problem") {
        detail = this.event.problem;
        id = get_id_from_string(detail);

    } else if (this.event_type.slice(0,3) == "seq") {
        detail = this.event.id;
        id = get_id_from_string(detail);

    } else if (this.event_type.substr(this.event_type.length-5) == "video") {
        detail = this.event.id;
        detail_more = this.event.code;
        id = get_id_from_string(detail);

    } else if (this.event_type[0] == "/" && this.event_source == "server") {
        detail = this.event_type;
        id = get_id_from_string(detail);
        this.event_type = "page_view";
    }

    emit({course_id: cid, 
          event_source: this.event_source,
          username: this.username, 
          id: id,
          detail: detail,
          detail_more: detail_more,
          event_type: this.event_type,
          date: date_only(timeobj)},
         1
        );
};

var reduce = function(key, events) {
    return Array.sum(events);
};

// empty out destination collection first
var before = out_collection.count();
if (before > 0) {
    print("INFO: removing " + before + " from \"" + out_collection_name + "\"");
    out_collection.remove();
};

// start the m/r
print("INFO: map/reduce on \"" + log_collection_name + "\", " +
                "results in \"" + out_collection_name + "\"");
out = log_collection.mapReduce(map, reduce, 
                         {out: out_collection_name,
                          sort: {load_file: 1}   /* use index */
                         });
printjson(out);

