#!/usr/bin/python

#
# manage roles and users
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
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    return parser.parse_args()


def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    print "-- ERROR: %s" % message

def open_connection():
    verbose("Connecting to MySQL")
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
    if options.print_only:
        print query
    else:
        connection = conn
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.commit()


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


def role_exists(role_name):
    """
    See if the given role already exists
    """
    query = """
        SELECT NULL
        FROM mysql.user 
        WHERE user = '%s' AND password = '%s'
        """ % (role_name, pad_to_password_length(account_role_indicator))
    return len(get_rows(query))


def create_role(role_name, role_password):
    """
    """
    if role_exists(role_name):
        print_error("Role '%s' already exists")
    query = """
        INSERT INTO mysql.user (user, host, password) 
        VALUES ('%s', '%s', '%s')
        """ % (role_name, role_password, pad_to_password_length(account_role_indicator))
    act_query(query)

        
def drop_role(role_name):
    """
    """
    if not role_exists(role_name):
        print_error("Role '%s' does not exist")
    query = """
        DELETE FROM mysql.user 
        WHERE user = '%s' AND password = '%s'
        """ % (role_name, pad_to_password_length(account_role_indicator))
    act_query(query)


def get_existing_association_seeds(account_user, account_host):
    """
    """
    qeury = """
        SELECT host FROM mysql.user 
        WHERE 
        account_user = '%s' AND account_host='%s' AND password like '%s:%%'
        """ % (account_user, account_host, account_association_indicator)
        
    association_seeds = [row["host"].split(":")[-1] for row in get_rows(query)]
    return association_seeds


def generate_unique_association_seed(account_user, account_host):
    association_seeds = get_existing_association_seeds(account_user, account_host)
    for i in range (1, 2**16):
        seed = hex(i).split('x')[-1]
        if not random_seed in association_seeds:
            return random_seed
    

def account_role_association_exists(account_user, account_host, role_name):
    """
    See if the given account is associated with the given role
    """
    query = """
        SELECT NULL
        FROM mysql.user 
        WHERE user = '%s' AND host LIKE '%s:%%' 
        AND password LIKE '%s:%s:%%'
        """ % (role_name, account_host, 
               account_association_indicator, role_name)
    return len(get_rows(query))


def associate_account(account_user, account_host, role_name):
    if account_role_association_exists(account_user, account_host, role_name):
        print_error("Account '%s'@'%s' already associated with role %s" % (account_user, account_host, role_name))
        
    host = "%s:%s" % (account_host, generate_unique_association_seed(account_user, account_host))
    query = """
        INSERT INTO mysql.user (user, host, password)
        VALUES ('%s', '%s', '%s:%s:')
        """ % (account_user, host, account_association_indicator, role_name)



def unassociate_account(account_user, account_host, role_name):
    if not account_role_association_exists(account_user, account_host, role_name):
        print_error("Account '%s'@'%s' is not associated with role %s" % (account_user, account_host, role_name))
        
    host = "%s:%s" % (account_host, generate_unique_association_seed(account_user, account_host))
    query = """
        DELETE FROM mysql.user
        WHERE user = '%s' AND host LIKE '%s:%%' AND password LIKE '%s:%s:'
        """ % (account_user, host, account_association_indicator, role_name)


def pad_to_password_length(s):
    return s + "#"*(41-len(s))

                
try:
    try:
        account_role_indicator = "oak:role:account:"
        account_association_indicator = "oak:role:association:"
        
        conn = None
        (options, args) = parse_options()
        conn = open_connection()
    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
