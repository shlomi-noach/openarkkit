#!/usr/bin/python

#
# Prepare for MySQL shutdown: close down slave replication, flush innodb pages
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
from optparse import OptionParser

def parse_options():
    usage = "usage: oak-prepare-shutdown [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL host (default: localhost)")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default="3306", help="TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
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


def get_status_variable(variable_name):
    row = get_row("SHOW GLOBAL STATUS LIKE '%s'" % variable_name);
    value = row["Value"]
    return value


def stop_slave():
    act_query("STOP SLAVE")

def start_slave():
    act_query("START SLAVE")

def slave_is_running():
    slave_status = get_row("SHOW SLAVE STATUS")
    return slave_status is not None

def get_slave_open_temp_tables():
    return int(get_status_variable("Slave_open_temp_tables"))

def get_innodb_buffer_pool_pages_dirty():
    return int(get_status_variable("Innodb_buffer_pool_pages_dirty"))


def set_innodb_max_dirty_pages_pct(value):
    query = "SET GLOBAL innodb_max_dirty_pages_pct = %d" % value
    act_query(query)


def get_global_variable(variable_name):
    row = get_row("SHOW GLOBAL VARIABLES LIKE '%s'" % variable_name);
    value = row["Value"]
    return value


def prepare_shutdown():
    if get_slave_open_temp_tables() > 0 and not slave_is_running():
        exit_with_error("Slave_open_temp_tables detected though slave is not running; this tool will not start the slave, since it's not it's job.")
    if slave_is_running():
        while True:
            stop_slave()
            slave_open_temp_tables = get_slave_open_temp_tables()
            if slave_open_temp_tables == 0:
                verbose("Slave stopped. There are no slave_open_temp_tables")
                break
            start_slave()
            verbose("Slave stopped, but there were %d slave_open_temp_tables. Slave started; will try again" % slave_open_temp_tables)
            time.sleep(1)
            
    original_innodb_max_dirty_pages_pct = int(get_global_variable("innodb_max_dirty_pages_pct"))

    num_succesive_non_improvements = 0
    max_succesive_non_improvements = 10
    min_innodb_buffer_pool_pages_dirty = get_innodb_buffer_pool_pages_dirty()
    verbose("innodb_buffer_pool_pages_dirty: %d" % min_innodb_buffer_pool_pages_dirty)
    set_innodb_max_dirty_pages_pct(0)
    try:
        # Iterate until no improvement is made for max_succesive_non_improvements seconds, 
        # or until the number of dirty pages reaches 0, which is optimal.
        while (num_succesive_non_improvements < max_succesive_non_improvements) and (min_innodb_buffer_pool_pages_dirty > 0):
            time.sleep(1)
            innodb_buffer_pool_pages_dirty = get_innodb_buffer_pool_pages_dirty()
            if innodb_buffer_pool_pages_dirty < min_innodb_buffer_pool_pages_dirty:
                num_succesive_non_improvements = 0
                min_innodb_buffer_pool_pages_dirty = innodb_buffer_pool_pages_dirty
                verbose("Down to %d" % min_innodb_buffer_pool_pages_dirty)
            else:
                num_succesive_non_improvements += 1
                verbose("No improvement from %d" % min_innodb_buffer_pool_pages_dirty)
    except KeyboardInterrupt:
        # Catch a Ctrl-C. Restore original settings
        set_innodb_max_dirty_pages_pct(original_innodb_max_dirty_pages_pct)
        exit_with_error("Ctrl-C hit. Terminating")
    verbose("Found no improvement for %d successive attempts. Will now terminate" % max_succesive_non_improvements)


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

        conn = open_connection()
        prepare_shutdown()
    except Exception, err:
        if options.debug:
            traceback.print_exc()
        print err
finally:
    if conn:
        conn.close()
