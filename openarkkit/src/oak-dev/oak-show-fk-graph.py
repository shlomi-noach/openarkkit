#!/usr/bin/python

#
# Show a FOREIGN KEY graph.
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

def show_fk_graph(conn):
    
    all_tables = set([])
    edges = set([])
    table_references = {}

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT TABLE_NAME, REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    query += " WHERE TABLE_SCHEMA='%s'" % options.database_name
    
    cursor.execute(query)
    for row in cursor.fetchall():
        table_name = row['TABLE_NAME']
        referenced_table_name = row['REFERENCED_TABLE_NAME']
        
        all_tables.add(table_name)
        all_tables.add(referenced_table_name)
        edges.add((table_name, referenced_table_name,))
        
        if referenced_table_name:
            if table_name in table_references:
                table_references[table_name].add(referenced_table_name)
            else:
                table_references[table_name] = set([referenced_table_name])
            
        print "%20s\t%20s" % (table_name, referenced_table_name)
    cursor.close()
    
    handled_tables = [table_name for table_name in all_tables if table_name not in table_references.keys()]
    all_tables = all_tables.difference(handled_tables)
    print handled_tables
    tables_by_levels = {}
    tables_by_levels[0] = handled_tables

    level = 1
    while all_tables:
        next_tables = [table_name for table_name in all_tables if table_references[table_name].issubset(handled_tables)]
        if next_tables:
            handled_tables.extend(next_tables)
            all_tables = all_tables.difference(handled_tables)
            tables_by_levels[level] = next_tables
            level += 1
        else:
            break
        
    for level in tables_by_levels.keys():
        print ("%d: " % level)+tables_by_levels[level].__str__()
    print all_tables

try:
    try:
        conn = None
        (options, args) = parse_options()

        conn = open_connection()
        show_fk_graph(conn)

    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
