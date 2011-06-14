#!/usr/bin/python

#
# Show the ratio between AUTO_INCREMENT columns limits and current values.
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
    parser.add_option("-d", "--database", dest="database_name", help="Database name (leave blank for all databases")
    parser.add_option("-t", "--table", dest="table_name", help="Table name (leave blank for all tables")
    parser.add_option("-c", "--column", dest="column_name", help="AUTO_INCREMENT column name (leave blank for all AUTO_INCREMENT columns")
    parser.add_option("-l", "--threshold", dest="ratio_threshold", type="float", default="0", help="Only show ratios larger than given threshold in range [0..1)")
    return parser.parse_args()

def verbose(message):
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

def show_limits(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA') AND LOCATE('auto_increment', EXTRA) > 0"
    if options.database_name:
        query += " AND TABLE_SCHEMA='%s'" % options.database_name
    if options.table_name:
        query += " AND TABLE_NAME='%s'" % options.table_name
    if options.column_name:
        query += " AND COLUMN_NAME='%s'" % options.column_name
    cursor.execute(query)
    for row in cursor.fetchall():
        schema_name = row['TABLE_SCHEMA']
        table_name = row['TABLE_NAME']
        column_name = row['COLUMN_NAME']
        data_type = row['DATA_TYPE']
        data_type_max_value = max_values[ data_type ]
        if not 'unsigned' in row['COLUMN_TYPE']:
            # This is a signed int value
            data_type_max_value = data_type_max_value // 2
        data_type_max_value = data_type_max_value - 1

        try:
            value_cursor = conn.cursor(MySQLdb.cursors.DictCursor)
            value_cursor.execute("SELECT MAX(`%s`) AS max_value FROM `%s`.`%s`" % (column_name, schema_name, table_name))
            max_value = value_cursor.fetchone()['max_value']
            value_cursor.close()
            if max_value:
                # It's None when there are no rows...
                ratio = float(max_value)/data_type_max_value
                if ratio >= options.ratio_threshold:
                    print "%s.%s.%s:\t%d/%d\t%f" % (schema_name, table_name, column_name, max_value, data_type_max_value , ratio)
        except Exception, err:
            print_error("Error reading %s.%s.%s" % (schema_name, table_name, column_name))
            print err

    cursor.close()

try:
    max_values = {}
    max_values['tinyint'] = 2**8
    max_values['smallint'] = 2**16
    max_values['mediumint'] = 2**24
    max_values['int'] = 2**32
    max_values['bigint'] = 2**64
    try:
        conn = None
        (options, args) = parse_options()

        conn = open_connection()
        show_limits(conn)

    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
