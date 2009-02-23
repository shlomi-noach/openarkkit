#!/usr/bin/python

#
# Analyze account grants, find accounts with identical grants, deduce grants order.
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


def get_representative(collection):
    for element in collection:
        return element
    return None

def grantee_has_db_grants(grantee):
    return accounts_db_grants[grantee]

def is_global_equal(grantee1, grantee2):
    if (grantee1, grantee2,) in accounts_global_equal:
        return True
    return False

def is_global_greater(grantee1, grantee2):
    if (grantee1, grantee2,) in accounts_global_greater:
        return True
    return False

def is_global_lower(grantee1, grantee2):
    if (grantee2, grantee1,) in accounts_global_greater:
        return True
    return False

def is_equal_on_all_db(grantee1, grantee2):
    for table_schema in accounts_db_equal:
        if not (grantee1, grantee2,) in accounts_db_equal[table_schema]:
            return False
    return True

def is_greater_on_some_db(grantee1, grantee2):
    for table_schema in accounts_db_equal:
        if (grantee1, grantee2,) in accounts_db_greater[table_schema]:
            return True
    return False

def is_lower_on_some_db(grantee1, grantee2):
    for table_schema in accounts_db_equal:
        if (grantee2, grantee1,) in accounts_db_greater[table_schema]:
            return True
    return False

def is_total_equal(grantee1, grantee2):
    if not is_global_equal(grantee1, grantee2):
        return False
    if grantee_has_db_grants(grantee1) or grantee_has_db_grants(grantee2):
        return is_equal_on_all_db(grantee1, grantee2)
    else:
        return True

def is_db_greater(grantee1, grantee2):
    return is_greater_on_some_db(grantee1, grantee2) and not is_lower_on_some_db(grantee1, grantee2)


def is_total_greater(grantee1, grantee2):
    if is_global_greater(grantee1, grantee2) and not grantee_has_db_grants(grantee1) and not grantee_has_db_grants(grantee2):
        return True
    if is_db_greater(grantee1, grantee2) and is_global_equal(grantee1, grantee2):
        return True
    if is_db_greater(grantee1, grantee2) and is_global_greater(grantee1, grantee2):
        return True
    if is_equal_on_all_db(grantee1, grantee2) and is_global_greater(grantee1, grantee2):
        return True
    return False

def one_way_compare_grants(grants1, is_grantable1, grants2, is_grantable2,):
    if grants1 is None:
        grants1 = {}
    if grants2 is None:
        grants2 = {}
    if is_grantable1 is None:
        is_grantable1 = 0
    if is_grantable2 is None:
        is_grantable2 = 0
    if grants1.issuperset(grants2):
        if len(grants1) > len(grants2) and is_grantable1 >= is_grantable2:
            return 1
        if len(grants1) >= len(grants2) and is_grantable1 > is_grantable2:
            return 1
        if len(grants1) == len(grants2) and is_grantable1 == is_grantable2:
            return 0
    return None


def read_global_grants(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """SELECT GRANTEE, PRIVILEGE_TYPE,
        CASE IS_GRANTABLE
            WHEN 'NO' THEN 0
            WHEN 'YES' THEN 1
        END
        AS IS_GRANTABLE
        FROM information_schema.USER_PRIVILEGES
        """
    cursor.execute(query)

    for row in cursor.fetchall():
        try:
            grantee = row["GRANTEE"]
            privilege_type = row["PRIVILEGE_TYPE"]
            is_grantable = int(row["IS_GRANTABLE"])

            grantees.add(grantee)
            if not accounts_global_grants.has_key(grantee):
                accounts_global_grants[grantee] = set([])
                accounts_global_grants[grantee].add("USAGE")
            accounts_global_grants[grantee].add(privilege_type)
            accounts_global_grant_options[grantee] = is_grantable
        except Exception, err:
            print err[-1]
    cursor.close()


def analyze_global_grants(conn):
    for grantee1 in grantees:
        for grantee2 in grantees:
            if grantee1 == grantee2:
                continue
            grants1 = accounts_global_grants[grantee1]
            grants2 = accounts_global_grants[grantee2]
            is_grantable1 = accounts_global_grant_options[grantee1]
            is_grantable2 = accounts_global_grant_options[grantee2]

            comparison = one_way_compare_grants(grants1, is_grantable1, grants2, is_grantable2)
            if comparison == 0:
                accounts_global_equal.add((grantee1, grantee2,))
            if comparison > 0:
                accounts_global_greater.add((grantee1, grantee2,))


def read_db_grants(conn):
    for grantee in grantees:
        accounts_db_grants[grantee] = {}
        accounts_db_grant_options[grantee] = {}

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """SELECT GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE,
        CASE IS_GRANTABLE
            WHEN 'NO' THEN 0
            WHEN 'YES' THEN 1
        END
        AS IS_GRANTABLE
        FROM information_schema.SCHEMA_PRIVILEGES
        """
    cursor.execute(query)

    for row in cursor.fetchall():
        try:
            grantee = row["GRANTEE"]
            table_schema = row["TABLE_SCHEMA"]
            privilege_type = row["PRIVILEGE_TYPE"]
            is_grantable = int(row["IS_GRANTABLE"])

            if not accounts_db_grants[grantee].has_key(table_schema):
                accounts_db_grants[grantee][table_schema] = set([])
                
            accounts_db_grants[grantee][table_schema].add(privilege_type)
            accounts_db_grant_options[grantee][table_schema] = is_grantable
            
        except Exception, err:
            print err[-1]
    cursor.close()


def analyze_db_grants(conn):
    for grantee1 in grantees:
        for table_schema in accounts_db_grants[grantee1]:
            for grantee2 in grantees:
                if grantee1 == grantee2:
                    continue

                if accounts_db_grants[grantee1].has_key(table_schema):
                    grants1 = accounts_db_grants[grantee1][table_schema]
                else:
                    grants1 = set([])
                for grant in accounts_global_grants[grantee1]:
                    grants1.add(grant)
                if accounts_db_grant_options[grantee1].has_key(table_schema):
                    is_grantable1 = accounts_db_grant_options[grantee1][table_schema]
                else:
                    is_grantable1 = 0
                if accounts_global_grant_options[grantee1] > 0:
                    is_grantable1 = 1

                if accounts_db_grants[grantee2].has_key(table_schema):
                    grants2 = accounts_db_grants[grantee2][table_schema]
                else:
                    grants2 = set([])
                for grant in accounts_global_grants[grantee2]:
                    grants2.add(grant)
                if accounts_db_grant_options[grantee2].has_key(table_schema):
                    is_grantable2 = accounts_db_grant_options[grantee2][table_schema]
                else:
                    is_grantable2 = 0
                if accounts_global_grant_options[grantee2] > 0:
                    is_grantable2 = 1
                
                if not accounts_db_equal.has_key(table_schema):
                    accounts_db_equal[table_schema] = set([])
                if not accounts_db_greater.has_key(table_schema):
                    accounts_db_greater[table_schema] = set([])
                    
                comparison = one_way_compare_grants(grants1, is_grantable1, grants2, is_grantable2)
                if comparison == 0:
                    accounts_db_equal[table_schema].add((grantee1, grantee2,))
                elif comparison > 0:
                    accounts_db_greater[table_schema].add((grantee1, grantee2,))
                else:
                    comparison = one_way_compare_grants(grants2, is_grantable2, grants1, is_grantable1)
                    if comparison > 0:
                        accounts_db_greater[table_schema].add((grantee2, grantee1,))
                        

def analyze_equality_groups():
    equality_groups = [set([grantee]) for grantee in grantees]
    should_continue = True
    while should_continue:
        should_continue = False
        for group1 in equality_groups:
            for group2 in equality_groups:
                if group1 == group2:
                    continue
                grantee1 = get_representative(group1)
                grantee2 = get_representative(group2)
                if is_total_equal(grantee1, grantee2):
                    for grantee in group2:
                        group1.add(grantee)
                    equality_groups.remove(group2)
                    should_continue = True
    for group in equality_groups:
        if len(group) == 1:
            continue
        print "The following accounts are globally equal:"
        for grantee in group:
            print "- "+grantee
        print ""
    greater_chains = []
    for group1 in equality_groups:
        for group2 in equality_groups:
            if group1 == group2:
                continue
            grantee1 = get_representative(group1)
            grantee2 = get_representative(group2)
            if is_total_greater(grantee1, grantee2):
                greater_chains.append([group1, group2])
                print "%s > %s" % (group1, group2,)
    should_continue = True
    while should_continue:
        should_continue = False
        for chain1 in greater_chains:
            for chain2 in greater_chains:
                if chain1 == chain2:
                    continue
                if chain1[-1] == chain2[0]:
                    for group in chain2:
                        chain1.append(group)
                    greater_chains.remove(chain2)
                    should_continue = True
                    
    for chain in greater_chains:
        chain_text = ""
        for group in chain:
            grantee = get_representative(group)
            if chain_text:
                chain_text = chain_text + " > "
            chain_text = chain_text + grantee
            print chain_text


def print_results():
    for grantee1 in grantees:
        for grantee2 in grantees:
            if grantee1 == grantee2:
                continue
    
            if is_total_equal(grantee1, grantee2):
                print "%s = %s" % (grantee1, grantee2,)
            if is_total_greater(grantee1, grantee2):
                print "%s > %s" % (grantee1, grantee2,)
                
try:
        conn = None
        (options, args) = parse_options()
        conn = open_connection()

        grantees = set([])
        accounts_global_grants = {}
        accounts_global_grant_options = {}
        accounts_global_equal = set([])
        accounts_global_greater = set([])

        accounts_db_grants = {}
        accounts_db_equal = {}
        accounts_db_greater = {}
        accounts_db_grant_options = {};

        read_global_grants(conn)
        analyze_global_grants(conn)
        read_db_grants(conn)
        analyze_db_grants(conn)
        print_results()
        analyze_equality_groups()
finally:
    if conn:
        conn.close()
