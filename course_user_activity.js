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

    var realValue = function(obj) {
       return obj && obj !== null && obj !== undefined;
    };

    var date_only = function(datetime) {
        if (datetime && datetime !== null && datetime !== undefined) {
            return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
        } else {
            return null;
        }
    };

    // if loadfile parameter is specified, then restrict only to those events
    if (typeof(loadfile) != 'undefined' && this.load_file !== loadfile) {
        return;
    }
    var cid = "none";
    if (realValue(this.course_id)) {
        cid = this.course_id;
    };
    if (this.time instanceof Date) {
        timeobj = this.time;
    } else {
        timeobj = new Date(this.time);
    }
    emit({course_id: cid, 
          event_source: this.event_source,
          username: this.username, 
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

