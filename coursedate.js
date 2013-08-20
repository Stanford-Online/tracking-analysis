
// settings
var session_collection_name = "session";
var coursedate_collection_name = "coursedate";

function map() {

    function date_only(datetime) {
        if (datetime && datetime !== null && datetime !== undefined) {
            return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
        } else {
            return null;
        }
    };
    
    emit({course_id: this._id.course_id, date: date_only(this.value.first_time)},
         {num_events: this.value.num_events,
          session_sec: this.value.session_sec, 
          sessions: 1});
};

function reduce(key, values) {
    num_events = 0;
    session_sec = 0;
    sessions = 0;

    values.forEach(function(v) {
        num_events += v.num_events;
        session_sec += v.session_sec;
        sessions += v.sessions;
    });
    return {num_events: num_events,
            session_sec: session_sec,
            sessions: sessions};
};

var session_collection = db.getCollection(session_collection_name);

"INFO: map/reduce on \"" + session_collection_name + "\", " +
                "results in \"" + coursedate_collection_name + "\"";
session_collection.mapReduce(map,
                             reduce,
                             {out: coursedate_collection_name});

