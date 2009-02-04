#!/usr/bin/python

#
# Audit a server's accounts and privileges
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
    parser.add_option("-r", "--assume-root", dest="assume_root", default=None, help="Comma seperated list of users which are to be treated as 'root'")
    parser.add_option("-l", "--audit-level", dest="audit_level", default="strict", help="Level of auditing tests: 'normal' or 'strict' (default)")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    return parser.parse_args()


def verbose(message):
    print "-- %s" % message


def verbose_topic(message):
    verbose("")
    verbose(message)
    verbose("-"*len(message))


def recommend(message):
    verbose(message+". Recommended actions:")


def verbose_passed():
    verbose("Passed")


def print_error(message):
    print "-- ERROR: %s" % message


def get_in_query(list):
    return "(" + ", ".join([ "'%s'" % item for item in list ]) + ")"

def get_root_users_in_query():
    return get_in_query(root_users)

def grantee_is_root(grantee):
    grantee_user = grantee.split("@")[0]
    if grantee_user.startswith("'") and grantee_user.endswith("'"):
        grantee_user = grantee_user[1:-1]
    return grantee_user in root_users

def is_strict():
    return options.audit_level == "strict"

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


def audit_root_user(conn):
    verbose_topic("Looking for non local 'root' accounts")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user WHERE user IN %s AND host NOT IN ('localhost', '127.0.0.1')" % get_root_users_in_query())
    rows = cursor.fetchall()
    if rows:
        recommend("Found %d non local 'root' accounts" % len(rows))
        for row in rows:
            try:
                user, host = row["user"], row["host"]
                query = "RENAME USER '%s'@'%s' TO '%s'@'localhost';" % (user, host, user,)
                #query = "DROP USER '%s'@'%s';" % (user, host,)
                print query
            except:
                print_error("-- Cannot %s" % query)
    else:
        verbose_passed()
    cursor.close()

def audit_anonymous_user(conn):
    verbose_topic("Looking for anonymous user accounts")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user WHERE user=''")
    rows = cursor.fetchall()
    if rows:
        recommend("Found %d non anonymous accounts" % len(rows))
        for row in rows:
            try:
                user, host = row["user"], row["host"]
                query = "DROP USER '%s'@'%s';" % (user, host,)
                print query
            except:
                print_error("-- Cannot %s" % query)
    else:
        verbose_passed()
    cursor.close()

def audit_any_host(conn):
    verbose_topic("Looking for accounts accessible from any host")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user WHERE host='%'")
    rows = cursor.fetchall()
    if rows:
        recommend("Found %d accounts accessible from any host" % len(rows))
        for row in rows:
            try:
                user, host = row["user"], row["host"]
                query = "RENAME USER '%s'@'%s' TO '%s'@'<specific host>';" % (user, host, user,)
                print query
            except:
                print_error("-- Cannot %s" % query)
    else:
        verbose_passed()
    cursor.close()

def audit_empty_passwords_accounts(conn):
    verbose_topic("Looking for accounts with empty passwords")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user WHERE password=''")
    rows = cursor.fetchall()
    if rows:
        recommend("Found %d accounts with empty passwords" % len(rows))
        for row in rows:
            try:
                user, host = row["user"], row["host"]
                new_password = '<some password>'

                query = "SET PASSWORD FOR '%s'@'%s' = PASSWORD('%s');" % (user, host, new_password)
                print query
            except:
                print_error("-- Cannot %s" % query)
    else:
        verbose_passed()
    cursor.close()

def audit_identical_passwords_accounts(conn):
    verbose_topic("Looking for accounts with identical (non empty) passwords")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT CONCAT('''', user, '''@''', host, '''' ) AS account, pass FROM (SELECT user1.user, user1.host, user2.user AS u2, user2.host AS h2, left(user1.password,5) as pass FROM mysql.user AS user1 INNER JOIN mysql.user AS user2 ON (user1.password = user2.password) WHERE user1.user != user2.user AND user1.password != '') users GROUP BY (CONCAT(user,'@',host)) ORDER BY pass")
    rows = cursor.fetchall()
    if rows:
        passwords = set([row["pass"] for row in rows])
        verbose("There are %d groups of accounts sharing the same passwords" % len(passwords))
        for password in passwords:
            accounts = [row["account"] for row in rows if row['pass'] == password]
            recommend("The following accounts have different users yet share the same password: %s" % ", ".join(accounts))
            for account in accounts:
                new_password = '<some passowrd>'
                query = "SET PASSWORD FOR %s = PASSWORD('%s');" % (account, new_password)
                print query
    else:
        verbose_passed()
    cursor.close()


def audit_all_privileges(conn):
    verbose_topic("Looking for (non root) accounts with all privileges")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT user,host FROM mysql.user ORDER BY user,host")

    permissive_privileges = []
    for row in cursor.fetchall():
        try:
            user, host = row["user"], row["host"]

            query = "SHOW GRANTS FOR '%s'@'%s'" % (user, host,)
            grant_cursor = conn.cursor()
            grant_cursor.execute(query)
            grants = grant_cursor.fetchall()

            for grant in [grantrow[0] for grantrow in grants]:
                if grant.startswith("GRANT ALL PRIVILEGES ON *.* TO") and not user in get_root_users_in_query():
                    query = "GRANT <specific privileges> ON *.* TO '%s'@'%s';" % (user, host,)
                    permissive_privileges.append((user,host,query,))
            grant_cursor.close()

        except:
            print "-- Cannot %s" % query
    if permissive_privileges:
        verbose("There are %d non root accounts with all privileges" % len(permissive_privileges))
        for (user,host,query) in permissive_privileges:
            print query
    else:
        verbose_passed()

    cursor.close()


def audit_admin_privileges(conn):
    verbose_topic("Looking for (non-root) accounts with admin privileges")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT GRANTEE, GROUP_CONCAT(PRIVILEGE_TYPE) AS privileges FROM information_schema.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN %s GROUP BY GRANTEE" % get_in_query(privileges_admin)
    cursor.execute(query)

    grantees = [row["GRANTEE"] for row in cursor.fetchall()]
    suspicious_grantees = [grantee for grantee in grantees if not grantee_is_root(grantee)]

    if suspicious_grantees:
        verbose("There are %d non-root accounts with admin privileges" % len(suspicious_grantees))
        recommend("admin privileges are: %s" % ", ".join(privileges_admin))
        for grantee in suspicious_grantees:
            query = "GRANT <non-admin-privileges> ON *.* TO %s;" % grantee
            print query
    else:
        verbose_passed()

    cursor.close()


def audit_global_ddl_privileges(conn):
    verbose_topic("Looking for (non-root) accounts with data definition privileges")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT GRANTEE, GROUP_CONCAT(PRIVILEGE_TYPE) AS privileges FROM information_schema.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN %s GROUP BY GRANTEE" % get_in_query(privileges_ddl)
    cursor.execute(query)

    grantees = [row["GRANTEE"] for row in cursor.fetchall()]
    suspicious_grantees = [grantee for grantee in grantees if not grantee_is_root(grantee)]

    if suspicious_grantees:
        verbose("There are %d non-root accounts with global data definition privileges." % len(suspicious_grantees))
        verbose("These accounts can drop or alter tables in all schemata, including the mysql database itself")
        recommend("data definition privileges are: %s" % ", ".join(privileges_ddl))
        for grantee in suspicious_grantees:
            query = "GRANT <non-data-definition-privileges> ON *.* TO %s;" % grantee
            print query
        verbose("It is further recommended to only grant privileges on specific databases")
    else:
        verbose_passed()

    cursor.close()


def audit_db_ddl_privileges(conn):
    verbose_topic("Looking for (non-root) accounts with schema data definition privileges")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT GRANTEE, GROUP_CONCAT(PRIVILEGE_TYPE) AS privileges, TABLE_SCHEMA FROM information_schema.SCHEMA_PRIVILEGES WHERE PRIVILEGE_TYPE IN %s GROUP BY GRANTEE" % get_in_query(privileges_ddl)
    cursor.execute(query)

    suspicious_grantees = []
    for row in cursor.fetchall():
        grantee, schema = (row["GRANTEE"], row["TABLE_SCHEMA"],)
        if not grantee_is_root(grantee):
            suspicious_grantees.append((grantee, schema,))

    if suspicious_grantees:
        verbose("There are %d non-root accounts with schema data definition privileges" % len(suspicious_grantees))
        verbose("These accounts can drop or alter tables in those schemas, or drop the schema itself.")
        recommend("data definition privileges are: %s" % ", ".join(privileges_ddl))
        for grantee, schema in suspicious_grantees:
            query = 'GRANT <non-data-definition-privileges> ON "%s".* TO %s;' % (schema, grantee,)
            print query
    else:
        verbose_passed()

    cursor.close()


def audit_global_dml_privileges(conn):
    verbose_topic("Looking for (non-root) accounts with global data manipulation privileges")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT GRANTEE, GROUP_CONCAT(PRIVILEGE_TYPE) AS privileges FROM information_schema.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN %s GROUP BY GRANTEE" % get_in_query(privileges_dml)
    cursor.execute(query)

    grantees = [row["GRANTEE"] for row in cursor.fetchall()]
    suspicious_grantees = [grantee for grantee in grantees if not grantee_is_root(grantee)]

    if suspicious_grantees:
        verbose("There are %d non-root accounts with global data manipulation privileges." % len(suspicious_grantees))
        verbose("These accounts can read and change data in all schemata, including the mysql database itself")
        recommend("data definition privileges are: %s" % ", ".join(privileges_dml))
        verbose("Only grant privileges on specific schemata")
        for grantee in suspicious_grantees:
            query = "GRANT <the-privileges> ON <specific_schema>.* TO %s;" % grantee
            print query
    else:
        verbose_passed()

    cursor.close()


def audit_mysql_privileges(conn):
    verbose_topic("Looking for (non-root) accounts with write privileges on the mysql schema")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = "SELECT GRANTEE, GROUP_CONCAT(PRIVILEGE_TYPE) AS privileges, TABLE_SCHEMA FROM information_schema.SCHEMA_PRIVILEGES WHERE TABLE_SCHEMA='mysql' AND PRIVILEGE_TYPE IN %s GROUP BY GRANTEE" % get_in_query(privileges_ddl+privileges_dml)
    cursor.execute(query)

    suspicious_grantees = []
    for row in cursor.fetchall():
        grantee, privileges = (row["GRANTEE"], row["privileges"],)
        write_privileges = [privilege for privilege in privileges.split(",") if privilege in privileges_ddl+privileges_dml]
        if not grantee_is_root(grantee):
            suspicious_grantees.append((grantee, write_privileges,))

    if suspicious_grantees:
        verbose("There are %d non-root accounts with write privileges on the mysql schema" % len(suspicious_grantees))
        verbose("These accounts can drop or alter tables in those schemas, or drop the schema itself.")
        recommend("data definition privileges are: %s" % ", ".join(privileges_ddl))
        for grantee, write_privileges in suspicious_grantees:
            query = 'REVOKE %s ON "mysql".* FROM %s;' % (",".join(write_privileges), grantee,)
            print query
    else:
        verbose_passed()

    cursor.close()


def audit_sql_mode(conn):
    verbose_topic("Checking global sql_mode")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT @@sql_mode AS sql_mode")
    sql_mode = cursor.fetchone()["sql_mode"]

    NO_AUTO_CREATE_USER = "NO_AUTO_CREATE_USER"
    if NO_AUTO_CREATE_USER in sql_mode.split(","):
        verbose_passed()
    else:
        recommend("sql_mode does not contain %s" % NO_AUTO_CREATE_USER)
        desired_sql_mode = NO_AUTO_CREATE_USER
        if sql_mode:
            desired_sql_mode += ","+sql_mode
        query = "SET GLOBAL sql_mode = '%s';" % desired_sql_mode
        print query

def audit_old_passwords(conn):
    verbose_topic("Checking old_passwords setting")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT @@old_passwords AS old_passwords")
    old_passwords = int(cursor.fetchone()["old_passwords"])

    if old_passwords:
        recommend("Old passwords are being used")
        verbose("Consider removing old-passwords from configuration. Make sure you read the manual first")
    else:
        verbose_passed()
    cursor.close()

def audit_skip_networking(conn):
    verbose_topic("Checking networking")
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SHOW GLOBAL VARIABLES LIKE 'skip_networking'")
    value = cursor.fetchone()['Value']
    if value == 'OFF':
        recommend("Networking is enabled")
        verbose("This is usually fine. If you're only accessing MySQL from the localhost,")
        verbose("consider setting --skip-networking and using UNIX socket or named pipes.")
    else:
        verbose("Networking is disabled")

    cursor.close()

def audit_test_database(conn):
    verbose_topic("Checking for `test` database existance")
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    rows = cursor.fetchall()
    if 'test' in [row[0] for row in rows]:
        recommend("`test` database found")
        query = "DROP DATABASE test;"
        print query
    else:
        verbose_passed()

    cursor.close()


try:
    try:
        privileges_admin = ["SUPER", "SHUTDOWN", "RELOAD", "PROCESS", "CREATE USER", "REPLICATION CLIENT", "REPLICATION SLAVE", ]
        privileges_ddl = ["CREATE", "DROP", "EVENT", "ALTER", "INDEX", "TRIGGER", "CREATE VIEW", "ALTER ROUTINE", "CREATE ROUTINE", ]
        privileges_dml = ["DELETE", "INSERT", "UPDATE", "CREATE TEMPORARY TABLES", ]
        conn = None
        (options, args) = parse_options()
        conn = open_connection()

        options.audit_level = options.audit_level.lower()
        if not options.audit_level in ["normal", "strict"]:
            print_error("audit-level must be one of 'normal', 'strict'")
            exit(1)
        verbose("Auditing in %s level" % options.audit_level)

        root_users = set([])
        root_users.add("root")
        if options.assume_root:
            for user in options.assume_root.split(","):
                root_users.add(user.strip())

        verbose("The following users are assumed as root: %s" % ", ".join(root_users))

        audit_root_user(conn)
        audit_anonymous_user(conn)
        audit_any_host(conn)

        audit_empty_passwords_accounts(conn)
        audit_identical_passwords_accounts(conn)

        audit_all_privileges(conn)
        audit_admin_privileges(conn)
        audit_mysql_privileges(conn)
        audit_global_ddl_privileges(conn)
        if is_strict():
            audit_db_ddl_privileges(conn)
            audit_global_dml_privileges(conn)

        audit_sql_mode(conn)
        if is_strict():
            audit_old_passwords(conn)
            audit_skip_networking(conn)
        audit_test_database(conn)

    except Exception, err:
        print err[-1]
finally:
    if conn:
        conn.close()
