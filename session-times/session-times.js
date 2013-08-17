
use tracking;

var map_valid_dates = function() {
    if (this.session && this.time) {
        emit(this.session, 
             {datelist:[new Date(this.time)], 
              first:0, last:0,
              events:0, session_sec:0, gap_sec:0})
    }
};

var reduce_sessions = function(session, datedur_list) {
    var combined = datedur_list[0].datelist;
    for (i=1; i < datedur_list.length; i++) {
        combined_dates = combined.concat(datedur_list[i].datelist);
    }
    var numeric_ascending = function(a,b) { return a-b };
    return {datelist:combined_dates.sort(numeric_ascending), duration:0};
}

var finalize_get_duration = function(session, datedur) {
    dates = datedur.datelist;
    session_sec = 0;
    gap_sec = 0;
    for (i=1; i < dates.length; i++) {
        extent = (dates[i] - dates[i-1])/1000;
        if (extent < 3600*4) {    /* >4 hours btw events doesnt count */
            session_sec += extent;
        } else {
            gap_sec += extent;
        }
    }
    return {datelist:[],
            first:dates[0], last:dates[dates.length-1],
            events:dates.length,
            session_sec:session_sec, gap_sec:gap_sec};
}

db.logs.mapReduce(map_valid_dates, 
                  reduce_sessions,
                  {out: "session_times",
                   sort: {session:1},
                   finalize: finalize_get_duration}
                 );

