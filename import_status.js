// I use like this:
//   watch -d -n 5 'mongo edx import_status.js'

db.tracking_imported.find({}, {good:1,error:1})
    .sort({date:-1})
    .forEach( function(f){print(tojson(f, '', true))} );
