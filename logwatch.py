#!/usr/bin/env python
#
# Example code for watching an S3 directory, meant to be the start of a data
# pipeline.

import sys
import os
import boto

LOCAL_LOG_STORE = "logwatch_logs/"
LOG_BUCKETNAME = "stanford-edx-logs"

myname = sys.argv[0]

try:
    conn = boto.connect_s3()
    log_bucket = conn.get_bucket(LOG_BUCKETNAME)
except boto.exception.NoAuthHandlerFound as e:
    # TODO: more error cases to watch here to be sure (bucket not found?)
    sys.stderr.write("%s: boto authentication error: %s\n" % (myname, str(e)))
    sys.stderr.write("suggestion: put your credentials in AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables, or a ~/.boto file\n")
    sys.exit(1)

logs = log_bucket.list()
for log in logs:
    logstr = str(log.name)
    if logstr[-1] == "/":
        continue

    dest = LOCAL_LOG_STORE+logstr
    if os.path.exists(dest):
        if os.stat(dest).st_size == log.size:
            print myname, "skipping:", logstr
            continue
        else:
            print myname, "removing partial:", logstr
            os.remove(dest)

    print myname, "downloading:", logstr
    dest_path = "/".join(dest.split("/")[0:-1])
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    log.get_contents_to_filename(dest)

