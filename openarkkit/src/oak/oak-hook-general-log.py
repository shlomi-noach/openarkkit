#!/usr/bin/python

#
# Hook into the MySQL general log, stream to standard output
#
# Released under the BSD license
#
# Copyright (c) 2008-2010, Shlomi Noach
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

import getpass
import MySQLdb
import sys
import time
import traceback
import warnings
from optparse import OptionParser

def parse_options():
    usage = "usage: oak-prepare-shutdown [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL host (default: localhost)")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default=3306, help="TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("-t", "--timeout-minutes", dest="timeout_minutes", type="int", default=1, help="Auto disconnect after given number of minutes (default: 1)")
    parser.add_option("-s", "--sleep-time", dest="sleep_time", type="int", default=1, help="Number of seconds between log polling (default: 1)")
    parser.add_option("", "--only-queries", dest="only_queries", action="store_true", default=False, help="Only print out logs of type Query")
    parser.add_option("", "--discard-existing", dest="discard_existing", action="store_true", default=False, help="Discard possibly pre-existing entries in the general log table")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("", "--debug", dest="debug", action="store_true", help="Print stack trace on error")
    return parser.parse_args()


def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    print >>sys.stderr, message

def open_connection():
    if options.defaults_file:
        conn = MySQLdb.connect(
            read_default_file = options.defaults_file)
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

def act_query(query):
    """
    Run the given query, commit changes
    """
    connection = conn
    cursor = connection.cursor()
    num_affected_rows = cursor.execute(query)
    cursor.close()
    connection.commit()
    return num_affected_rows


def get_row(query):
    connection = conn
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    row = cursor.fetchone()

    cursor.close()
    return row


def get_rows(query):
    connection = conn
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    return rows


def get_status_variable(variable_name):
    row = get_row("SHOW GLOBAL STATUS LIKE '%s'" % variable_name);
    value = row["Value"]
    return value


def get_log_output():
    log_output = get_row("SELECT UPPER(@@global.log_output) AS log_output")["log_output"]
    return log_output

def store_original_log_settings():
    act_query("SET @general_log_original_setting = @@global.general_log")
    act_query("SET @log_output_original_setting = @@global.log_output")
    verbose("Stored original settings")

def restore_original_log_settings():
    act_query("SET @@global.general_log = @general_log_original_setting")
    act_query("SET @@global.log_output = @log_output_original_setting")
    verbose("Restored original settings")

def enable_general_log_table_output():
    act_query("SET @@session.sql_log_off=1")
    act_query("SET @@global.general_log='ON'")
    log_output = get_log_output()
    if log_output.find("TABLE") >= 0:
        pass
    elif log_output.find("NONE") >= 0:
        log_output = "TABLE"
    else:
        log_output = "%s,TABLE" % log_output
    act_query("SET @@global.log_output = '%s'" % log_output)
    verbose("log_output is now %s" % get_log_output())


def get_inactive_shadow_table():    
    for table in shadow_tables:  
        if table != active_shadow_table:
            return table
    # Impossible to get here
    return None

    
def drop_shadow_tables():  
    for table in shadow_tables:  
        act_query("DROP TABLE IF EXISTS mysql.%s" % table)    


def create_shadow_table():
    act_query("CREATE TABLE mysql.%s LIKE mysql.general_log" % active_shadow_table)    
    verbose("%s table created" % active_shadow_table)


def cleanup_active_shadow_table():
    act_query("TRUNCATE TABLE mysql.%s" % active_shadow_table)    
    verbose("%s table truncated" % active_shadow_table)


def rotate_general_log_table():
    global active_shadow_table
    global num_rotates
    
    cleanup_active_shadow_table()
    act_query("RENAME TABLE mysql.general_log TO mysql.%s, mysql.%s TO mysql.general_log" % (get_inactive_shadow_table(), active_shadow_table))
    active_shadow_table = get_inactive_shadow_table()    
    if options.discard_existing and num_rotates == 0:
        cleanup_active_shadow_table()
    num_rotates = num_rotates + 1
    verbose("%s is now active" % active_shadow_table)


def dump_general_log_snapshot():
    rows = get_rows("SELECT * FROM mysql.%s" % active_shadow_table)
    for row in rows:
        event_time = row["event_time"]
        user_host = row["user_host"]
        thread_id = row["thread_id"]
        server_id = row["server_id"]
        command_type = row["command_type"]
        argument = row["argument"]
        should_print = True
        if options.only_queries and command_type != "Query":
            should_print = False
        if should_print:
            print "%s\t%s\t%s\t%s\t%s\t%s" % (event_time, user_host, thread_id, server_id, command_type, argument)
            sys.stdout.flush()
        


def hook_general_log():
    start_time = time.time()
    store_original_log_settings()
    enable_general_log_table_output()
    drop_shadow_tables()
    create_shadow_table()
    while time.time() - start_time < options.timeout_minutes*60:
        rotate_general_log_table()
        dump_general_log_snapshot()
        time.sleep(options.sleep_time)
        
    drop_shadow_tables()
    restore_original_log_settings()


def exit_with_error(error_message):
    """
    Notify and exit.
    """
    print_error(error_message)
    exit(1)


try:
    try:
        conn = None
        reuse_conn = True
        (options, args) = parse_options()

        warnings.simplefilter("ignore", MySQLdb.Warning) 
        conn = open_connection()
        
        shadow_tables = ["general_log_shadow_0", "general_log_shadow_1"]
        active_shadow_table = shadow_tables[0]
        num_rotates = 0
        
        hook_general_log()
    except Exception, err:
        if options.debug:
            traceback.print_exc()
        print err
finally:
    if conn:
        conn.close()
