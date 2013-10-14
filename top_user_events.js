// top_users.js
//
// For a course pick the top users (by time spent), and show what actions
// those users did.  Assumes that the "tracking" collection contains edX
// tracking logs, and the "session" collection has been created from those
// logs, see session.js.
//
// Sef Kloninger
// (c) Stanford University 2013
//

var course_id = 'Stanford/2013/Three_Books';
var excludes = ['mtuttle', 'caesar2164', 'NicholasJenkins', 'kimhwrth','gbruhns'];
var topN = 20;

print("INFO: Course: " + course_id);
print("INFO: Find " + topN + " most active users, excluding staff: " + excludes.concat());
var top_users = db.session.aggregate([
        {$match: {'_id.course_id': course_id}}, 
        {$match: {'value.username': {$nin: excludes}}},
        {$group: {_id: "$value.username",
                  session_secs: {$sum:"$value.session_sec"}}},
        {$sort:  {session_secs: -1}},
        {$limit: topN}
]);
var top_users_result = top_users['result'];
var top_names = top_users_result.map(function (elem) {return elem._id;});
print("INFO: Top " + topN + " users: " + top_names.concat());

// Careful, building these indexes make take a looong time

print("INFO: Ensure index on \"tracking.course_id\"");
db.tracking.ensureIndex({course_id:1}, {background:true});

// not strictly required
// 
// print("INFO: Ensure index on \"tracking.username\"");
// db.tracking.ensureIndex({'username':1}, {background:true})

// add this to exclude page views
//      {$match: {_id: {$not: /^\/courses\/.*$/}}},

print("INFO: Events done by the top " + topN + " users.");
var activities = db.tracking.aggregate([
        {$match: {course_id: course_id}}, 
        {$match: {username: {$in: top_names}}},
        {$group: {_id: '$event_type',
                  count: {$sum:1}} },
        {$match: {count: {$gte: 100}}},
        {$sort:  {count: -1}}
]);

printjson(activities['result']);
