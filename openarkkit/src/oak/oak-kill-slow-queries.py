#!/usr/bin/python

#
# Kill queries running for a long time
#
# Released under the BSD license
#
# Copyright (c) 2008, Shlomi Noach
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
#     * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import sys
import getpass
import MySQLdb
from optparse import OptionParser
import ConfigParser

def parse_options():
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL host (default: localhost)")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default="3306", help="TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("-l", "--slow-query-seconds", dest="slow_query_seconds", type="int", default="600", help="Number of seconds after which a query is considered slow")
    parser.add_option("-r", "--skip-root", action="store_true", dest="skip_root", default=False, help="Do not kill queries by 'root'")
    parser.add_option("-k", "--skip-user", dest="skip_user", default=None, help="Do not kill queries by by given user")
    parser.add_option("-f", "--filter-user", dest="filter_user", default=None, help="Only kill queries by by given user")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    return parser.parse_args()

def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    print "-- ERROR: %s" % message

def open_connection():
    if options.defaults_file:
        conn = MySQLdb.connect(read_default_file = options.defaults_file)
    else:
        if options.prompt_password:
            password=getpass.getpass()
        else:
            password=options.password
        conn = MySQLdb.connect(
            host = options.host,
            user = options.user,
            passwd = password,
            port = options.port,
            unix_socket = options.socket)
    return conn;

def act_final_query(query):        
    """
    Either print or execute the given query
    """
    if options.print_only:
        print query
    else:
        update_cursor = conn.cursor()
        try:
            try:
                update_cursor.execute(query)
                verbose("Successfuly killed query")
            except:
                print_error("error executing: %s" % query)
        finally:
            update_cursor.close()

def get_slow_processes_ids():
    """
    Return the list of process ids where queries are slow
    """
    slow_processes_ids = []
    cursor = None;
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SHOW PROCESSLIST")
        result_set = cursor.fetchall()
        for row in result_set:
            time = int(row["Time"])
            command = row["Command"]
            user = row["User"]
            # We will NOT kill:
            # * queries not running as long as 'slow_query_seconds'
            # * active queries
            # * replication slave
            kill_query = True
            if time < options.slow_query_seconds:
                kill_query = False
            if command == "Sleep":
                kill_query = False
            if user == "system user":
                kill_query = False
            if options.skip_root and user == "root":
                kill_query = False
            if options.skip_user and user == options.skip_user:
                kill_query = False
            if options.filter_user and user != options.filter_user:
                kill_query = False
            if kill_query:
                slow_processes_ids.append(row["Id"])
    finally:
        if cursor:
            cursor.close()
    return slow_processes_ids

def kill_slow_queries(conn):
    slow_processes_ids = get_slow_processes_ids()
    verbose("Found %s slow queries" % len(slow_processes_ids))
    for process_id in slow_processes_ids:
        cursor = conn.cursor()
        query = "KILL QUERY %d" % process_id
        
        act_final_query(query)

try:
    try:
        conn = None
        (options, args) = parse_options()

        conn = open_connection()
        kill_slow_queries(conn)
    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
