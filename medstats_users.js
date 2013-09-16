var result = db.session.aggregate([
       {$match: {"_id.course_id": "Education/EDUC115N/How_to_Learn_Math"}},
       {$group: {_id: "$value.username",
                 num_events: {$sum: "$value.num_events"},
                 session_sec: {$sum: "$value.session_sec"},
                 sessions: {$sum: 1} }},
]);

"INFO: results written to the \"medstats_users\" collection"
db.medstats_users.remove();
db.medstats_users.insert(result.result);

"INFO: first five users alphabetically"
db.medstats_users.find().sort({"_id":1}).limit(5).pretty();

