#!/usr/bin/python

#
# Block or release an account
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
    parser.add_option("--account-user", dest="account_user", help="Mandatory: the user (login) name to block/release")
    parser.add_option("--account-host", dest="account_host", help="The account's host. Leave blank to apply for all hosts")
    parser.add_option("-b", "--block", action="store_true", dest="block", help="Block the specified account")
    parser.add_option("-r", "--release", action="store_true", dest="release", help="Release a blocked account")
    parser.add_option("-k", "--kill", action="store_true", dest="kill", help="With --block: kill current blocked accounts")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    return parser.parse_args()

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

def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    print "-- ERROR: %s" % message

def get_blocked_accounts_processes_ids():
    """
    Return the list of process ids which match the account details
    """
    blocked_accounts_processes_ids = []
    cursor = None;
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SHOW PROCESSLIST")
        result_set = cursor.fetchall()
        for row in result_set:
            user = row["User"]
            host = row["Host"].split(":")[0]

            if user == options.account_user:
                if host == options.account_host or not options.account_host:
                    blocked_accounts_processes_ids.append(row["Id"])
    finally:
        if cursor:
            cursor.close()
    return blocked_accounts_processes_ids

def kill_blocked_accounts_processes(conn):
    """
    Kill the connections for the blocked account(s)
    """
    blocked_accounts_processes_ids = blocked_accounts_processes_ids()
    verbose("Found %s connections to kill" % len(slow_processes_ids))
    for process_id in blocked_accounts_processes_ids:
        cursor = conn.cursor()
        query = "KILL %d" % process_id
        act_final_query(query)

def is_new_password(password):
    """
    MySQL's new passwords are indicated by a 40 characters long text, prefixed by '*',
    for total 41 characters. Old style passwords are 16 characters long.
    """
    return len(password) == 41 and not '~' in password

def blocked_password(password):
    if is_new_password(password):
        if password.startswith("*"):
            return password[::-1]
    else:
        if not password.startswith("~"):
            return "~"*25+password
    return None

def released_password(password):
    if is_new_password(password):
        if password.endswith("*"):
            return password[::-1]
    else:
        if password.startswith("~"):
            return password[25:]
    return None

def act_final_query(query, message):
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
                verbose(message)
            except:
                print_error("error executing: %s" % query)
        finally:
            update_cursor.close()

def block_account(conn):
    if not options.account_host:
        verbose("Will act on all hosts for user %s" % options.account_user)

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT user,host,password FROM mysql.user WHERE user='%s'" % options.account_user
    if options.account_host:
        query += " AND host='%s'" % options.account_host
    query += " ORDER BY user,host"

    cursor.execute(query)
    for row in cursor.fetchall():
        try:
            user = row['user']
            host = row['host']
            password = row['password']
            new_password = None

            if is_new_password(password):
                password_format = "new"
            else:
                password_format = "old"
            verbose("password for '%s'@'%s' is in %s format" % (user, host, password_format))

            if options.block:
                new_password = blocked_password(password)
                if not new_password:
                    print_error("Account is already blocked")
            if options.release:
                new_password = released_password(password)
                if not new_password:
                    print_error("Account is already released")

            if new_password:
                update_query = "SET PASSWORD FOR '%s'@'%s' = '%s'" % (user, host, new_password)
                act_final_query(update_query, "Successfuly updated password")
        except Exception, err:
            print "-- Cannot change password for %s: %s" % (user, err)
    cursor.close()

try:
    try:
        conn = None
        (options, args) = parse_options()
        if not options.block and not options.release:
            print_error("either --block or --release must be specified")
            exit(1)
        if options.block and options.release:
            print_error("--block and --release may not be specified together")
            exit(1)
        if options.kill and not options.block:
            print_error("--kill may only be specified with --block")
            exit(1)
        if not options.account_user:
            print_error("--account-user must be specifeid")
            exit(1)

        conn = open_connection()
        block_account(conn)
        if options.kill:
            kill_blocked_accounts_processes(conn)
    except Exception, err:
        print err[-1]
finally:
    if conn:
        conn.close()
