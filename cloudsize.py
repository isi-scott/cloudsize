#! /usr/bin/env python
# coding=UTF-8


"""
cloudsize.py is a tool to aid reporting of cloudpools archived file and directory
sizes. It maintains a local sqlite database, by default stored in the root of
/ifs. In order to execute in update mode, cloudsize needs to be run by the
root user, and be provided with either root creds or those for a user with
privileges for PAPI, Smartpools, and Cloudpools

Search mode outputs logical sizes.

Options:
  -h, --help            Show this help message and exit
  -d PATH, --database=PATH
                        Set path of sqlite db for this run. Default path is
                        /ifs/stub_file_list.db
  -s STRING, --search=STRING
                        Run cloudsize in search mode, matching STRING. No db
                        update takes place prior to searching.

Example usage:

cloudsize.py (no arguments - performs db update)
cloudsize.py -d /ifs/some_name.db
cloudsize.py -s /ifs/foo

Database format:
"""

use="%prog [options]"
exs="""
cloudsize.py is a tool to aid reporting of cloudpools archived file and directory
sizes. It maintains a local sqlite database, by default stored in the root of
/ifs. In order to execute in update mode, cloudsize needs to be run by the
root user, and be provided with either root creds or those for a user with
privileges for PAPI, Smartpools, and Cloudpools.

Search mode outputs logical sizes.

Options:
  -h, --help    Show this help message and exit
  -d PATH, --database=PATH
                Set path of sqlite db for this run. Default path is
                /ifs/stub_file_list.db
  -s STRING, --search=STRING
                Run cloudsize in search mode, matching STRING. No db
                update takes place prior to searching. 

Example usage: \n
cloudsize.py (no arguments - performs db update)
cloudsize.py -d /ifs/some_name.db
cloudsize.py -s /ifs/foo \n
"""

import base64
import httplib
import json
import os
import sqlite3
import ssl
import string
import sys
import time
import optparse
import getpass

def make_papi_call(method, uri, body="", cluster_ip="127.0.0.1"):
    """
    Function to handle generic calls to PAPI. Returns status, reason, and json
    payload.
    """
    username=uname
    pword=upass
    port = 8080
    headers = {}

    headers['Authorization'] = 'Basic ' + \
        string.strip(base64.encodestring(username + ":" + pword))
    headers['content-type'] = 'application/json'

    # Make a connection to the server, if ssl is setup to authenticate
    # certificates by default, pass in an unverified context
    connection = None
    if hasattr(ssl, "_create_unverified_context"):
        ssl_ctx = ssl._create_unverified_context()
        connection = httplib.HTTPSConnection(cluster_ip, port, context=ssl_ctx)
    else:
        connection = httplib.HTTPSConnection(cluster_ip, port)

    connection.request(method, uri, body, headers)
    connection.sock.settimeout(600)
    response = connection.getresponse()
    if response.status != 200:
        # Try up to 5 times waiting 2 seconds
        wcnt = 0;
        while response.status != 200 and wcnt != 5:
            time.sleep(2)
            response = connection.getresponse()
            wcnt += 1
    else:
        json_resp = response.read()

    # Close the connection
    connection.close()
    connection = None

    return response.status, response.reason, json_resp


def check_complete(sqlcur, jobid):
    """
    Takes active sql cursor and a job ID. Checks to see
    - if we've seen this job before
    - if we've processed all files
    Returns True if the job is fully processed, False if it needs work

    """
    # Check if the jobid exists in jobs table
    sqlcur.execute("select * from jobs where id='" + jobid + "'")
    try:
        result = sqlcur.fetchone()[0]
    except:
        # Table not found, needs to be processed
        return False
    # Check if the jobid table exists
    sqlcur.execute("select count(*) from sqlite_master where type='table' and name='" + jobid + "'")

    # Get the total number if files processed by this job
    status, reason, jobview = make_papi_call("GET", "/platform/3/cloud/jobs/" + jobid)
    resp_view = json.loads(jobview)
    for view in resp_view['jobs']:
        totalfiles = view['files']['total']

    # Get the row count of the table in the db
    sqlcur.execute("SELECT count(*) FROM files WHERE job_id='" + jobid + "'")
    tblrows = sqlcur.fetchone()[0]

    # See if the number of files processed equals rows in db
    if totalfiles <= tblrows:
        # Nothing to do, files are processed already
        return True
    else:
        # Still work to be done, process the jobid
        return False


def hsize(nbytes):
    """Convert bytes to human readable values"""
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


def searchmode(sdir):
    """Takes search string, queries sqlite db, prints output"""
    totalsize = 0
    # Connect to the sqlitedb
    conn = sqlite3.connect(dbpath)
    sqlcur = conn.cursor()

    # Process tables based on args
    sqlcur.execute("SELECT size from files WHERE name LIKE ? and size != 'NA'",
                   ("%" + sdir + "%",))
    for row in sqlcur:
        totalsize = totalsize + int(row[0])
    print "Cloud file size in paths matching %s: %s" % (sdir, hsize(totalsize))

    # Close the database
    conn.close()


def addjobs():
    # Get a list of all the CloudPools jobs
    status, reason, jobids = make_papi_call("GET", "/platform/3/cloud/jobs")

    # load into local JSON list
    resp_dict = json.loads(jobids)
    jobidlist = list()
    jobmap = {}

    # Connect to the sqlitedb
    conn = sqlite3.connect(dbpath)
    sqlcur = conn.cursor()
    sqlcur.execute("CREATE TABLE IF NOT EXISTS 'jobs' (id text, state text, UNIQUE(id))")
    # If the job is complete add it in jobidList (excludes long running jobs)
    for jobid in resp_dict['jobs']:
        if jobid['effective_state'] == "completed":
            # get the job into a string
            sjobid = str(jobid['id'])

            # Check to see if we need to process the job
            chkret = check_complete(sqlcur, sjobid)

            if chkret == False:
                jobidlist.append(sjobid)
                sqlcur.execute("INSERT OR IGNORE INTO jobs VALUES ('" + sjobid +
                               "','Processing')")

            # commit the entries
            jobmap[sjobid] = jobid['job_engine_job']['id']
            conn.commit()
    # Close the sqlite db
    conn.close()

    # return list of jobs to process and CPjob/JEjob map
    return jobidlist, jobmap


def addfiles(jobidlist, jobmap):
    # Connect to the sqlitedb
    conn = sqlite3.connect(dbpath)
    sqlcur = conn.cursor()
    sqlcur.execute("CREATE TABLE IF NOT EXISTS 'files' (id text, name text, state text, size text," +
                   " offset text, job_id text, je_job text, UNIQUE(id, offset))")

    # get the file list for each job
    for jobid in jobidlist:
        offset = 0
        resume = ""

        print "Processing files for Job ID %s" % jobid

        while (resume != None):
            status, reason, files = make_papi_call("GET",
                "/platform/3/cloud/jobs-files/" + jobid +
                    "?batch=true&limit=100000&offset=" + str(offset * 100000))
            resp_dict = json.loads(files, encoding = 'iso-8859-15')
            resume = resp_dict['resume']
            jobenginejob = jobmap[jobid]
            for fileinfo in resp_dict['files']:
                filesize = 0

                # Filename needs to be utf-8 for the sqlite insert
                # Filename needs to be iso-8859-15 for the stat to work
                sfilename = fileinfo['name']
                ufilename = sfilename.encode('utf-8')
                ifilename = sfilename.encode('iso-8859-15')

                # stat the file to get size info
                if fileinfo['name'] != '<missing>':
                   try:
                        statinfo = os.stat(ifilename)
                        filesize = statinfo.st_size
                   except:
                        # Error stat'ing the file
                        # The name may contain funky characters
                        # The file may not be there anymore
                        print sys.exc_info()[1]
                        filesize = "NA"

                # Make sure the name does not have single quote
                ufilename = ufilename.replace("'","''")

                # Insert the files into the job table
                sqlcur.execute("INSERT OR IGNORE INTO files VALUES ('" +
                            str(jobid) + str(fileinfo['id']) + "','" + ufilename +
                            "','" + fileinfo['state'] + "','" + str(filesize) +
                            "','" + str(offset) + "','" + str(jobid) +
                            "','" + str(jobenginejob) + "')")

            # Commit after the 100k of inserts.
            # 100k then commit takes about 7 seconds.
            # Commit after each insert takes about 400 seconds
            conn.commit()

            # bump the page number
            offset += 1
        sqlcur.execute("UPDATE jobs SET state='Complete' WHERE id=?", (str(jobid),))
        conn.commit()
    # Close the sqlite db
    conn.close()


# Handle command arguments with optparse(for pre-2.7 compatibility)
# optparse strips newlines with default formatting, so override that here
optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog

p = optparse.OptionParser(usage=use, epilog=exs)
p.add_option("--database", "-d", dest="dbpath", type="string",
             help="Set path of sqlite db for this run. Default path is \
                /ifs/stub_file_list.db")
p.add_option("--search", "-s", dest="searchdir", type="string",
             help="Run cloudsize in search mode, matching STRING. No db \
                update takes place prior to searching.")
options, args = p.parse_args()


# If database path passed, set global
if options.dbpath:
    dbpath = options.dbpath
else:
    dbpath='/ifs/stub_file_list.db'

# If a directory argument is passed, search and exit
if options.searchdir:
    searchmode(options.searchdir)
else:
    # Get user and password for API calls
    uname = raw_input("PAPI Authorized user:")
    upass = getpass.getpass()

    ids, map = addjobs()
    addfiles(ids, map)

sys.exit(0)
