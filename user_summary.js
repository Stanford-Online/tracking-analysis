print("INFO: summing session stats by course_id and user");
var course_user_events = db.session.aggregate([
       {$group: {_id: {course_id: "$_id.course_id",
                       username: "$value.username"},
                 num_events: {$sum: "$value.num_events"},
                 session_sec: {$sum: "$value.session_sec"},
                 sessions: {$sum: 1} }},
]);

print("INFO: write results in the \"user_summary\" collection");
db.user_summary.remove();
db.user_summary.insert(course_user_events.result);

print("INFO: users per class (>100 users)");
var class_users = db.user_summary.aggregate([
        {$group: {_id: "$_id.course_id",
                  num_users: {$sum:1} }},
        {$match: {num_users: {$gt: 100}}},
        {$sort: {num_users: 1}}
]);
printjson(class_users.result);
