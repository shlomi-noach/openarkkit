#!/usr/bin/python

#
# Show variables which have been changed by SET GLOBAL, and whose value differs from the defaults.
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

import os
import subprocess
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
    parser.add_option("", "--mysqld", dest="mysqld", default="mysqld", help="mysqld binary full path")
    parser.add_option("", "--generate-defaults", action="store_true", dest="generate_defaults", help="Generate SET GLOBAL statements to restore default values")
    parser.add_option("", "--generate-changes", action="store_true", dest="generate_changes", help="Generate SET GLOBAL statements to apply changed values")
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


def get_rows(query):
    connection = conn
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    return rows


def read_global_variables(conn):
    global_variables = {}
    query = "SHOW GLOBAL VARIABLES"
    for row in get_rows(query):
        variable_name = row["Variable_name"].lower().replace('-','_')
        variable_value = row["Value"]
        global_variables[variable_name] = variable_value
    return global_variables


def read_default_variables():
    """
    Spawn mysqld, get the list of default variables.
    Variables start after a "---------------" line and end with an empty line
    """

    default_variables = {}
    p = subprocess.Popen([options.mysqld, "--help", "--verbose"],  stdout=subprocess.PIPE, env={"PATH": os.environ['PATH']})
    lines = p.stdout.readlines()
    has_reached_variables_section = False
    for line in lines:
        line = line.strip()
        if line.startswith("--------"):
            has_reached_variables_section = True
            continue
        if not has_reached_variables_section:
            continue
        if not line:
            break
        tokens = line.split(None,1)
        variable_name = tokens[0].lower().replace('-','_')
        if len(tokens) == 1:
            variable_value = ""
        else:
            variable_value = tokens[1]
        default_variables[variable_name] = variable_value
    return default_variables


def get_normalized_variable_value(default_value, global_value):
    if default_value == "FALSE" and global_value == "OFF":
        return "0"
    if default_value == "TRUE" and global_value == "ON":
        return "1"
    if default_value == "1" and global_value == "ON":
        return "1"
    if default_value == "1" and global_value == "YES":
        return "1"
    return None


def show_changed_variable(variable_name, default_value, global_value):
    if default_value == global_value:
        return
    if default_value == "(No default value)":
        return
    # The following are not handles:
    unhandled_variables = ["datadir", "open_files_limit", "pid_file",]
    if variable_name in unhandled_variables:
        return
    # Handle those quirky cases:
    if default_value == "FALSE" and global_value == "OFF":
        return
    if default_value == "TRUE" and global_value == "ON":
        return
    if default_value == "1" and global_value == "ON":
        return
    if default_value == "1" and global_value == "YES":
        return

    verbose("[%s] ::: %s ::: %s" % (variable_name, global_value, default_value))
    
    if options.generate_defaults:
        print "SET GLOBAL %s = %s ;" % (variable_name, default_value)
    if options.generate_changes:
        print "SET GLOBAL %s = %s ;" % (variable_name, global_value)


def show_changed_variables(conn):
    default_variables = read_default_variables()
    global_variables = read_global_variables(conn)
#    for variable_name in sorted(default_variables.keys()):
#        print "[%s]" % variable_name, ":::", default_variables[variable_name]
    for variable_name in sorted(global_variables.keys()):
        if default_variables.has_key(variable_name):
            default_value = default_variables[variable_name]
            global_value = global_variables[variable_name]
            show_changed_variable(variable_name, default_value, global_value)

try:
    try:
        conn = None
        (options, args) = parse_options()

        conn = open_connection()
        show_changed_variables(conn)

    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
