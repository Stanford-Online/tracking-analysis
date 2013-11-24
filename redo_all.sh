#!/bin/bash

set -e 

for op in session top_user_events user_summary user_times course_user_activity event_types; do
    echo 
    echo "---------------- $op ----------------"
    date
    time mongo edx ${op}.js
done | tee -a redo_all.log
