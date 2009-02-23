#!/usr/bin/python

#
# Apply referencial integrity based on a parent-child relation between two columns.
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
    parser.add_option("--parent", dest="parent_column", help="Fully qualified parent (referenced) column. Must be in the following format: schema_name.table_name.column_name")
    parser.add_option("--child", dest="child_column", help="Fully qualified child (referencing) column. Must be in the following format: schema_name.table_name.column_name")
    parser.add_option("--condition", dest="condition", help="Condition to validate referential integrity")
    parser.add_option("-l", "--safety-level", dest="safety_level", default="normal", help="Level of tests to make in order for action to take place: 'none', 'normal' (default), 'high'")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
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
            except:
                print_error("error executing: %s" % query)
        finally:
            update_cursor.close()


def get_column_property(full_column, property):
    """
    Given a fully qualified column name, get the given property from INFORMATION_SCHEMA.COLUMNS
    """
    column_qualification = full_column.split(".")

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT %s FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'" % tuple([property] + column_qualification))
    row = cursor.fetchone()

    property_value = row[property]
    cursor.close()

    return property_value

def generate_ri_trigger(conn):
    """
    Do the main work
    """
    parent_table = ".".join(options.parent_column.split(".")[:-1])
    child_table = ".".join(options.child_column.split(".")[:-1])

    if not options.condition:
        options.condition = "%s = OLD.%s" % (child_column_name, parent_column_name)
    query = """
        CREATE TRIGGER %s_AD AFTER DELETE ON %s
        FOR EACH ROW
            DELETE FROM %s WHERE %s;
    """ % (parent_table, parent_table, child_table, options.condition)

    act_final_query(query)

try:
    try:
        conn = None
        (options, args) = parse_options()
        if not options.parent_column or not options.child_column:
            print_error("Both --parent and --child must be specified")
            exit(1)

        parent_column_tokens = options.parent_column.split(".")
        if len(parent_column_tokens) != 3:
            print_error("parent column must in the following format: schema_name.table_name.column_name")
            exit(1)
        child_column_tokens = options.child_column.split(".")
        if len(child_column_tokens) != 3:
            print_error("child column must in the following format: schema_name.table_name.column_name")
            exit(1)

        safety_levels = {}
        safety_levels['none'] = 0
        safety_levels['normal'] = 1
        safety_levels['high'] = 2

        options.safety_level = options.safety_level.lower()
        if not options.safety_level in safety_levels.keys():
            print_error("safety-level must be one of 'none', 'normal', 'high'")
            exit(1)

        conn = open_connection()

        # Perform safety checks:
        safety_errors = []
        if safety_levels[options.safety_level] >= safety_levels['normal']:
            # We compare data types. Character types must be identical down to length level.
            # other type's precision is not validated (int(11) is the same as int(2))
            parent_data_type = get_column_property(options.parent_column, "DATA_TYPE")
            child_data_type = get_column_property(options.child_column, "DATA_TYPE")
            if parent_data_type != child_data_type:
                safety_errors.append("safety-level 'normal' error: parent and child column data types are not identical: %s, %s" % (parent_data_type, child_data_type))
            else:
                if parent_data_type in ['char', 'varchar']:
                    parent_column_type = get_column_property(options.parent_column, "COLUMN_TYPE")
                    child_column_type = get_column_property(options.child_column, "COLUMN_TYPE")
                    if parent_column_type != child_column_type:
                        safety_errors.append("safety-level 'normal' error: parent and child character column types are not identical: %s, %s. Specify a lower safety-level to override." % (parent_column_type, child_column_type))

        parent_column_name = parent_column_tokens[-1]
        child_column_name = child_column_tokens[-1]
        if safety_levels[options.safety_level] >= safety_levels['high']:
            # We compare column names: they must be identical
            if parent_column_name != child_column_name:
                safety_errors.append("safety-level 'high' error: parent and child column names are not identical: %s, %s. Specify a lower safety-level to override." % (parent_column_name, child_column_name))

        if safety_errors:
            for error in safety_errors:
                print_error(error)
            exit(1)

        generate_ri_trigger(conn)

    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
