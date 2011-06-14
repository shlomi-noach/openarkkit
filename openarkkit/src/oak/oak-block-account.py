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
import re
import sys
import traceback
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
    parser.add_option("-l", "--list", action="store_true", dest="list", help="List accounts blocked/released status")
    parser.add_option("-k", "--kill", action="store_true", dest="kill", help="With --block: kill current blocked accounts")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("", "--debug", dest="debug", action="store_true", help="Print stack trace on error")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    return parser.parse_args()



def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    sys.stderr.write("-- ERROR: %s\n" % message)

def open_connection():
    if options.defaults_file:
        conn = MySQLdb.connect(
            read_default_file=options.defaults_file)
    else:
        if options.prompt_password:
            password = getpass.getpass()
        else:
            password = options.password
        conn = MySQLdb.connect(
            host=options.host,
            user=options.user,
            passwd=password,
            port=options.port,
            unix_socket=options.socket)
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


def verify_single_param_is_set(*params):
    num_set_params = 0
    for param in params:
        if param:
            num_set_params += 1
    return (num_set_params == 1)


def get_blocked_accounts_processes_ids():
    """
    Return the list of process ids which match the account details
    """
    blocked_accounts_processes_ids = []
    query = "SHOW PROCESSLIST"
    for row in get_rows(query):
        user = row["User"]
        host = row["Host"].split(":")[0]

        if user == options.account_user:
            if host == options.account_host or not options.account_host:
                blocked_accounts_processes_ids.append(row["Id"])
    return blocked_accounts_processes_ids


def kill_blocked_accounts_processes():
    """
    Kill the connections for the blocked account(s)
    """
    blocked_accounts_processes_ids = get_blocked_accounts_processes_ids()
    verbose("Found %s connections to kill" % len(blocked_accounts_processes_ids))
    for process_id in blocked_accounts_processes_ids:
        query = "KILL %d" % process_id
        act_query(query)


def is_empty_password(password):
    """
    Just check for length
    """
    return len(password) == 0 or password == blocked_empty_password


def is_blocked_password(password):
    """
    Is the given password a blocked one?
    """
    if password == blocked_empty_password:
        return True
    if password.startswith(blocked_old_password_prefix):
        return True;
    if blocked_new_passoword_regexp.match(password):
        return True
    return False


def is_new_password(password):
    """
    MySQL's new passwords are indicated by a 40 characters long text, prefixed by '*',
    for total 41 characters. Old style passwords are 16 characters long.
    """
    return len(password) == 41 and not '~' in password


def blocked_password(password):
    if is_empty_password(password):
        if len(password) == 0:
            return blocked_empty_password
    elif is_new_password(password):
        if password.startswith("*"):
            return password[::-1]
    else:
        if not password.startswith("~"):
            return blocked_old_password_prefix + password
    return None


def released_password(password):
    if is_empty_password(password):
        if password.startswith("?"):
            return ""
    elif is_new_password(password):
        if password.endswith("*"):
            return password[::-1]
    else:
        if password.startswith("~"):
            return password[len(blocked_old_password_prefix):]
    return None




def get_listing_query():
    query = "SELECT user, host, password FROM mysql.user"
    if options.account_user:
        query += " WHERE user='%s'" % options.account_user
        if options.account_host:
            query += " AND host='%s'" % options.account_host
    query += " ORDER BY user,host"
    return query


def block_account():
    if not options.account_host:
        verbose("Will act on all hosts for user %s" % options.account_user)

    for row in get_rows(get_listing_query()):
        try:
            user = row['user']
            host = row['host']
            password = row['password']
            new_password = None
            account = "'%s'@'%s'" % (user, host,)

            if is_empty_password(password):
                verbose("password for '%s'@'%s' is empty" % (user, host))
            else:
                if is_new_password(password):
                    password_format = "new"
                else:
                    password_format = "old"
                verbose("password for '%s'@'%s' is in %s format" % (user, host, password_format))

            if options.block:
                new_password = blocked_password(password)
                if new_password is None:
                    print_error("Account %s is already blocked" % account)
            if options.release:
                new_password = released_password(password)
                if new_password is None:
                    print_error("Account %s is already released" % account)

            if new_password is not None:
                update_query = "SET PASSWORD FOR '%s'@'%s' = '%s'" % (user, host, new_password)
                act_query(update_query)
                verbose("Successfully updated password for account %s" % account)
        except Exception, err:
            if options.debug:
                traceback.print_exc()
            print_error("Cannot change password for %s: %s" % (account, err))



def list_accounts():
    verbose("Listing accounts blocked status")

    for row in get_rows(get_listing_query()):
        user = row['user']
        host = row['host']
        password = row['password']
        account = "'%s'@'%s'" % (user, host,)

         
        if is_blocked_password(password):
            blocked_status = "blocked"
        else:
            blocked_status = "released"
        print("%s\t%s" % (account, blocked_status))


try:
    try:
        conn = None
        (options, args) = parse_options()
        if not verify_single_param_is_set(options.block, options.release, options.list):
            print_error("either --block, --release or --list must be specified, and only one of them")
            exit(1)
        if options.kill and not options.block:
            print_error("--kill may only be specified with --block")
            exit(1)
        if not options.account_user and (options.block or options.release):
            print_error("--account-user must be specified for blocking/releasing")
            exit(1)

        blocked_empty_password = "?" * 41
        blocked_old_password_prefix = "~" * 25
        blocked_new_passoword_regexp = re.compile(r'^([0-9a-fA-F]{40})[*]$')

        conn = open_connection()
        if options.list:
            list_accounts()
        else:
            block_account()
        if options.kill:
            kill_blocked_accounts_processes()
    except Exception, err:
        if options.debug:
            traceback.print_exc()
        print err[-1]
finally:
    if conn:
        conn.close()
