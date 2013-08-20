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
var log_collection_name = "tracking";
var session_collection_name = "session";

// create index 
var log_collection = db.getCollection(log_collection_name);
"INFO: creating (session,time) index on \"" + log_collection_name + "\"";
log_collection.ensureIndex({session:1,time:1}, {background:1});
//"INFO: creating (time) index on \"" + log_collection_name + "\"";
//log_collection.ensureIndex({time:1}, {background:1});

// map/reduce functions
function map_valid_dates() {

    function realValue(obj) {
       return obj && obj !== null && obj !== undefined;
    };

    if (this.session && this.time) {
        var cid = "none";
        if (realValue(this.course_id)) {
            cid = this.course_id;
        };
        emit({course_id: cid, session: this.session},
             {username: this.username, events: [new Date(this.time)]});
    }
};

function reduce_sessions(key, session_events) {
    var combined = [];
    for (var i=0; i < session_events.length; i++) {
        combined = combined.concat(session_events[i].events);
    }
    var _ascending = function(a,b) { return a-b };
    return {username: session_events[0].username, events: combined.sort(_ascending)}
};

function finalize_get_duration(key, session_events) {
    var num_events = session_events.events.length;
    var session_sec = 0;
    for (var i=1; i < num_events; i++) {
        extent = (session_events.events[i] - session_events.events[i-1])/1000;
        if (extent < 3600) {    /* >1 hour btw events doesnt count */
            session_sec += extent;
        }
    }
    return {username: session_events.username,
            first_time: session_events.events[0],
            last_time: session_events.events[num_events-1],
            course_id: key.course_id,
            num_events: num_events,
            session_sec: session_sec};
};

// start the m/r
"INFO: map/reduce on \"" + log_collection_name + "\", " +
                "results in \"" + session_collection_name + "\"";
log_collection.mapReduce(map_valid_dates, 
                         reduce_sessions,
                         {out: session_collection_name,
                          sort: {session:1, time:1},  /* use index */
                          finalize: finalize_get_duration
                         });

