#!/usr/bin/python

#
# Show the grants on a given mysql server
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
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    parser.add_option("--verbose", action="store_true", dest="verbose", help="Print user firendly messages")
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

def build_query(row):
    column_type = row['COLUMN_TYPE']
    column_default = row['COLUMN_DEFAULT']
    is_nullable = (row['IS_NULLABLE'] == 'YES')
    query = "ALTER TABLE %s.%s MODIFY COLUMN %s %s" % (schema_name, table_name, column_name, column_type)
    query += " CHARSET %s" % new_charset
    if collation_supplied:
        query += " COLLATE %s" % new_collation
    if not is_nullable:
        query += " NOT NULL"
    if column_default:
        query += " DEFAULT '%s'" % column_default
    return query

def alter_column(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'" % (schema_name, table_name, column_name))
    row = cursor.fetchone()

    character_set_name = row['CHARACTER_SET_NAME']
    if not character_set_name:
        print_error("Column %s.%s.%s is not textual" % (schema_name, table_name, column_name))
        return
    query = build_query(row)
    if options.print_only:
        print query+";"
    else:
        alter_cursor = conn.cursor()
        try:
            try:
                alter_cursor.execute(query)
                verbose("Successfuly executed: %s" % query)
            except:
                print_error("Failed %s;" % query)
        finally:
            alter_cursor.close()
    cursor.close()

try:
    try:
        conn = None
        (options, args) = parse_options()
        if not 2 <= len(args) <= 3:
            print_error("Usage: oak-modify-charset schema_name.table_name.column_name new_charset_name [new_collate_name]")
            exit(1)
        column_tokens = args[0].split(".")
        if len(column_tokens) != 3:
            print_error("column must in the following format: schema_name.table_name.column_name")
            exit(1)
        schema_name, table_name, column_name = column_tokens
        new_charset = args[1]
        collation_supplied = (len(args) == 3)
        if collation_supplied:
            new_collation = args[2]

        conn = open_connection()
        alter_column(conn)
    except Exception, err:
        print err[-1]
finally:
    if conn:
        conn.close()
