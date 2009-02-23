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
    parser.add_option("--account-user", dest="account_user", help="A specific user for whom to show grants")
    parser.add_option("--account-host", dest="account_host", help="A specific host for which to show grants")
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

def show_grants(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user ORDER BY user,host")
    found_grants = set([])
    for row in cursor.fetchall():
        try:
            user, host = row["user"], row["host"]
            if options.account_user and options.account_user != user:
                continue
            if options.account_host and options.account_host != host:
                continue
            if not (user, host) in found_grants:
                print "-- grants for '%s'@'%s'" % (user, host,)
            found_grants.add((user,host,))

            query = "SHOW GRANTS FOR '%s'@'%s'" % (user, host,)
            grant_cursor = conn.cursor()
            grant_cursor.execute(query)
            for grant in grant_cursor.fetchall():
                print grant[0]
            grant_cursor.close()
        except:
            print "-- Cannot %s" % query
    cursor.close()

try:
    try:
        conn = None
        (options, args) = parse_options()
        conn = open_connection()
        show_grants(conn)
    except Exception, err:
        print err[-1]
finally:
    if conn:
        conn.close()
