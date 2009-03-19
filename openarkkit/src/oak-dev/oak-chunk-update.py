#!/usr/bin/python

#
# Chunk a given query using an auto_increment column
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

import getpass
import MySQLdb
import time
import re
from optparse import OptionParser

def parse_options():
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL host (default: localhost)")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default="3306", help="TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("-d", "--database", dest="database", help="Database name (required unless query uses fully qualified table names)")
    parser.add_option("-e", "--execute", dest="execute_query", help="Query (UPDATE or DELETE) to execute, which contains a chunk placeholder (required)")
    parser.add_option("-t", "--table", dest="table", help="Table with AUTO_INCREMENT column by which to chunk")
    parser.add_option("-c", "--chunk-size", dest="chunk_size", type="int", default=1000, help="Number of rows to act on in chunks (default: 1000). 0 means all rows updated in one operation")
    parser.add_option("--start-with", dest="start_with", type="int", default=None, help="AUTO_INCREMENT value to start with (default: minimal in table)")
    parser.add_option("--end-with", dest="end_with", type="int", default=None, help="AUTO_INCREMENT value to end with (default: maximal in table)")
    parser.add_option("--sleep", dest="sleep_millis", type="int", default=0, help="Number of milliseconds to sleep between chunks. Default: 0")
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
            db = database_name,
            unix_socket = options.socket)
    return conn;


def act_query(query):
    """
    Run the given query, commit changes
    """
    if reuse_conn:
        connection = conn
    else:
        connection = open_connection()
    cursor = connection.cursor()
    #print query
    cursor.execute(query)
    cursor.close()
    connection.commit()
    if not reuse_conn:
        connection.close()


def get_row(query):
    if reuse_conn:
        connection = conn
    else:
        connection = open_connection()
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    row = cursor.fetchone()

    cursor.close()
    if not reuse_conn:
        connection.close()
    return row


def get_rows(query):
    if reuse_conn:
        connection = conn
    else:
        connection = open_connection()
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    if not reuse_conn:
        connection.close()
    return rows


def get_auto_increment_column():
    """
    Return the column name (lower case) of the AUTO_INCREMENT column in the given table,
    or None if no such column is found.
    """
    auto_increment_column_name = None

    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='%s'
            AND TABLE_NAME='%s'
            AND LOCATE('auto_increment', EXTRA) > 0
        """ % (database_name, table_name)
    row = get_row(query)

    if row:
        auto_increment_column_name = row['COLUMN_NAME'].lower()
    verbose("%s.%s AUTO_INCREMENT column is %s" % (database_name, table_name, auto_increment_column_name))

    return auto_increment_column_name


def table_exists(check_table_name):
    """
    See if the a given table exists:
    """
    count = 0

    query = """
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='%s'
            AND TABLE_NAME='%s'
        """ % (database_name, check_table_name)

    row = get_row(query)
    count = int(row['count'])

    return count


def get_auto_increment_range():
    """
    Return the MIN and MAX values for the AUTO INCREMENT column in the original table
    """
    query = """
        SELECT
          IFNULL(MIN(%s),0) AS auto_increment_min_value,
          IFNULL(MAX(%s),0) AS auto_increment_max_value
        FROM %s.%s
        """ % (auto_increment_column_name, auto_increment_column_name,
               database_name, table_name)
    row = get_row(query)
    auto_increment_min_value = int(row['auto_increment_min_value'])
    auto_increment_max_value = int(row['auto_increment_max_value'])

    return auto_increment_min_value, auto_increment_max_value


def get_auto_increment_range_end(auto_increment_range_start):
    query = """
        SELECT MAX(%s) AS auto_increment_range_end
        FROM (SELECT %s FROM %s.%s
          WHERE %s >= %d
          AND %s <= %d
          ORDER BY %s LIMIT %d) SEL1
        """ % (auto_increment_column_name,
               auto_increment_column_name, database_name, table_name,
               auto_increment_column_name, auto_increment_range_start,
               auto_increment_column_name, auto_increment_max_value,
               auto_increment_column_name, options.chunk_size)

    row = get_row(query)
    auto_increment_range_end = int(row['auto_increment_range_end'])

    return auto_increment_range_end


def chunk_update():
    if auto_increment_min_value is None:
        return

    auto_increment_range_start = auto_increment_min_value
    while auto_increment_range_start < auto_increment_max_value:
        auto_increment_range_end = get_auto_increment_range_end(auto_increment_range_start)
        progress = int(100.0*(auto_increment_range_start-auto_increment_min_value)/(auto_increment_max_value-auto_increment_min_value))

        between_statement = "%s.%s.%s BETWEEN %d AND %d" % (database_name, table_name, auto_increment_column_name, auto_increment_range_start, auto_increment_range_end)
        query = "%s %s %s" % (options.execute_query[:match.start()], between_statement, options.execute_query[match.end():])

        verbose("Updating range (%d, %d), %d%% progress" % (auto_increment_range_start, auto_increment_range_end, progress))
        act_query(query)

        auto_increment_range_start = auto_increment_range_end+1

        if options.sleep_millis > 0:
            verbose("Will sleep for %f seconds" % (options.sleep_millis/1000.0))
            time.sleep(options.sleep_millis/1000.0)



try:
    try:
        conn = None
        reuse_conn = True
        (options, args) = parse_options()

        if options.chunk_size < 0:
            print_error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")
            exit(1)

        if not options.execute_query:
            print_error("Query to execute must be provided via -e or --execute")
            exit(1)

        match_regexp = "OAK_CHUNK[\\s]*\((.*?)\)"
        match = re.search(match_regexp, options.execute_query)
        if not match:
            print_error("Query must include the following token: 'OAK_CHUNK(table_name)', where table_name should be replaced with a table which consists of an AUTO_INCREMENT column by which chunks are made.")
            exit(1)

        table_name_match = match.group(1).strip()

        database_name = None
        table_name =  None

        if options.database:
            database_name=options.database

        table_tokens = table_name_match.split(".")
        table_name = table_tokens[-1]
        if len(table_tokens) == 2:
            database_name = table_tokens[0]

        if not database_name:
            print_error("No database specified. Specify with fully qualified table name insode OAK_CHUNK(...) or with -d or --database")
            exit(1)

        conn = open_connection()

        if not table_exists(table_name):
            print_error("Table %s.%s does not exist" % (database_name, table_name))
            exit(1)

        auto_increment_column_name = get_auto_increment_column()
        if not auto_increment_column_name:
            print_error("Table must have an AUTO_INCREMENT column")
            exit(1)

        auto_increment_min_value, auto_increment_max_value = get_auto_increment_range()
        if options.start_with is not None:
            auto_increment_min_value = options.start_with
        if options.end_with is not None:
            auto_increment_max_value = options.end_with
        verbose("Will update range: (%d, %d)" % (auto_increment_min_value, auto_increment_max_value))

        chunk_update()

        verbose("Chunk update completed")
    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
