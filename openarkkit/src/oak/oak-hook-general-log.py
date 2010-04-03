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
    parser.add_option("-f", "--filter", dest="filter", default=None, help="Only output logs of certain type. Valid values: [explain_fullscan|explain_temporary|explain_filesort|query|connection]")
    parser.add_option("", "--include-existing", dest="include_existing", action="store_true", default=False, help="Include possibly pre-existing entries in the general log table (default: disabled)")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("", "--debug", dest="debug", action="store_true", help="Print stack trace on error")
    return parser.parse_args()


def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    sys.stderr.write("-- ERROR: %s\n" % message)

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


def get_explain_plan(query, database):
    if database:
        act_query("USE %s" % database)
    explain_query = "EXPLAIN %s" % query
    rows = get_rows(explain_query)
    return rows


def explain_plan_contains(query, database, explain_column, search_value):
    if not query.lower().strip().startswith("select"):
        return False

    explain_plan = get_explain_plan(query, database)
    for explain_row in explain_plan:
        if explain_row[explain_column].find(search_value) >= 0:
            return True
    return False


def get_processlist():
    rows = get_rows("SHOW PROCESSLIST")
    return rows


def get_database_per_connection_map():
    database_per_connection_map = {}
    for row in get_processlist():
        connection_id = row["Id"]
        database = row["db"]
        database_per_connection_map[connection_id] = database
    return database_per_connection_map 
    

def get_global_variable(variable_name):
    row = get_row("SHOW GLOBAL VARIABLES LIKE '%s'" % variable_name);
    value = row["Value"]
    return value


def get_log_output():
    log_output = get_row("SELECT UPPER(@@global.log_output) AS log_output")["log_output"]
    return log_output

def store_original_log_settings():
    act_query("SET @general_log_original_setting = @@global.general_log")
    act_query("SET @log_output_original_setting = @@global.log_output")
    verbose("Stored original settings: general_log=%s, log_output=%s" % (get_global_variable("general_log"), get_global_variable("log_output")))

def restore_original_log_settings():
    act_query("SET @@global.general_log = @general_log_original_setting")
    act_query("SET @@global.log_output = @log_output_original_setting")
    verbose("Restored original settings")

def enable_general_log_table_output():
    global originally_used_log_tables
    
    act_query("SET @@session.sql_log_off=1")
    act_query("SET @@global.general_log='ON'")
    log_output = get_log_output()
    if log_output.find("TABLE") >= 0:
        originally_used_log_tables = True
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
    if num_rotates == 0 and not options.include_existing:
        cleanup_active_shadow_table()
    num_rotates = num_rotates + 1
    verbose("%s is now active" % active_shadow_table)


def truncate_slow_log_table():
    """
    When enabling table logs, both general log and slow log tables are enabled at once.
    When log_queries_not_using_indexes is used, we can be certain that this very tool will
    cause for bloating of the slow_log.
    Anyway, we want to make sure the slow_log table does not bloat. We therefore TRUNCATE it.
    """
    query = "TRUNCATE mysql.slow_log"
    act_query(query)
    

def dump_general_log_snapshot():
    database_per_connection_map = get_database_per_connection_map()
    rows = get_rows("SELECT * FROM mysql.%s" % active_shadow_table)
    for row in rows:
        event_time = row["event_time"]
        user_host = row["user_host"]
        thread_id = row["thread_id"]
        server_id = row["server_id"]
        command_type = row["command_type"]
        argument = row["argument"]
        
        database = None
        if database_per_connection_map.has_key(thread_id):
            database = database_per_connection_map[thread_id]

        should_print = False
        if options.filter is None:
            should_print = True
        elif options.filter == "explain_fullscan":
            if explain_plan_contains(argument, database, "type", "ALL"):
                should_print = True
        elif options.filter == "explain_temporary":
            if explain_plan_contains(argument, database, "Extra", "Using temporary"):
                should_print = True
        elif options.filter == "explain_filesort":
            if explain_plan_contains(argument, database, "Extra", "Using filesort"):
                should_print = True
        elif options.filter == "query":
            if command_type == "Query":
                should_print = True
        elif options.filter == "connection":
            if command_type in ["Connect", "Quit"]:
                should_print = True
        else:
            exit_with_error("Unknown filter value: %s" % options.filter)

        if should_print:
            print "%s\t%s\t%s\t%s\t%s\t%s" % (event_time, user_host, thread_id, server_id, command_type, argument)
            sys.stdout.flush()
        

def hook_general_log():
    start_time = time.time()
    store_original_log_settings()
    enable_general_log_table_output()
    drop_shadow_tables()
    create_shadow_table()
    try:
        while time.time() - start_time < options.timeout_minutes*60:
            rotate_general_log_table()
            if not originally_used_log_tables:
                truncate_slow_log_table()
            dump_general_log_snapshot()
            time.sleep(options.sleep_time)
    except KeyboardInterrupt:
        # Catch a Ctrl-C. We still want to restore defaults, most probably disabling general log.
        pass
        
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
        originally_used_log_tables = False
        
        hook_general_log()
    except Exception, err:
        if options.debug:
            traceback.print_exc()
        print err
finally:
    if conn:
        conn.close()
