// session.js 
//
// Aggregate set of edX tracking logs by session.  Creates a collection
// with basic per-session statistics, and then sums the session
// times to display the total time.  Looking at sessions means that
// only logged-in hits are considered.
// 
// WARNING: output collection is clobbered if exists.
//

// settings
//

// var log_collection_name = "tracking_sample";   /* small */
var log_collection_name = "tracking";          /* big */
var session_collection_name = "session";

// create index 
"INFO: creating index on \"" + log_collection_name + "\"";
var log_collection = db.getCollection(log_collection_name);
log_collection.ensureIndex({session:1,time:1}, {background:1});

// map/reduce on tracking logs
"INFO: map/reduce on \"" + log_collection_name + "\", " +
    "results in \"" + session_collection_name + "\"";
var map_valid_dates = function() {
    if (this.session && this.time) {
        emit(this.session, {events: [{date:new Date(this.time)}], 
                            username: this.username} );
    }
};

var reduce_sessions = function(session, events_list) {
    var combined = []
    for (var i=0; i < events_list.length; i++) {
        combined = combined.concat(events_list[i].events);
    }
    var date_ascending = function(a,b) { return a.date-b.date };
    return {events: combined.sort(date_ascending),
            username: events_list[0].username};
};

var finalize_get_duration = function(session, sorted) {
    var num_events = sorted.events.length;
    var session_sec = 0;
    var gap_sec = 0;
    for (var i=1; i < num_events; i++) {
        extent = (sorted.events[i].date - sorted.events[i-1].date)/1000;
        if (extent < 3600) {    /* >1 hour btw events doesnt count */
            session_sec += extent;
        } else {
            gap_sec += extent;
        }
    }
    return {username:sorted.events[0].username,
            first:sorted.events[0].date,
            last:sorted.events[num_events-1].date,
            events:num_events,
            session_sec:session_sec, 
            gap_sec:gap_sec};
};

log_collection.mapReduce(map_valid_dates, 
                         reduce_sessions,
                         {out: session_collection_name,
                          sort: {session:1, time:1},  /* use index */
                          finalize: finalize_get_duration
                         });


// Totalling up
"INFO: aggregating into \"" + session_collection_name + "\"" ;
var session_result = db.getCollection(session_collection_name);
session_result.aggregate([
               {$group: {_id: "total", 
                         session_secs: {$sum:"$value.session_sec"}}
               }
]);


// Times Per Day 
// doesn't work yet -- have to aggregate by y/m/d first

//result_collection.ensureIndex({"value.first":1});
//result_collection.aggregate([
//               {$group: {_id: "$value.first.toDateString()", 
//                         session_total: {$sum:"$value.session_sec"}}},
//               { $sort: {_id: 1}}
//]);


