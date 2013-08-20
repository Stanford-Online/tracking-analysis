// sum time spent across all sessions, print result

var session_collection_name = "session";

"INFO: aggregating into \"" + session_collection_name + "\"" ;
var session_result = db.getCollection(session_collection_name);
session_result.aggregate([
               {$group: {_id: "total", 
                         session_secs: {$sum:"$value.session_sec"}}
               }
]);


