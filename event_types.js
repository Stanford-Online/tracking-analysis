
"INFO: creating index on event_type";
db.tracking.ensureIndex({event_type:1}, {background:true});

"INFO: aggregating on event_type";
var types = db.tracking.aggregate([
      {$group: {_id: '$event_type', 
                count: {$sum: 1}}},
      {$match: {_id: {$not: /^\/.*$/}}},   // ignore "path" events, start with "/"
      {$sort:  {count: 1}}
]);

"INFO: result stored in \"event_types\" collection";
db.event_types.remove();
db.event_types.insert(types.result);

// to get CSV output, use this command
// mongoexport --db edx --collection event_types --csv -f '_id,count' > event_types.csv
