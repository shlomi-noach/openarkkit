#!/usr/bin/python

#
# Perform an online ALTER TABLE
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
import re
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
    parser.add_option("-d", "--database", dest="database", help="Database name (required unless table is fully qualified)")
    parser.add_option("-t", "--table", dest="table", help="Table with AUTO_INCREMENT column to alter (optionally fully qualified)")
    parser.add_option("-a", "--alter", dest="alter_statement", help="Comma delimited ALTER statement details, excluding the 'ALTER TABLE t' itself")
    parser.add_option("-c", "--chunk-size", dest="chunk_size", type="int", default=1000, help="Number of rows to act on in chunks. Default: 1000")
    parser.add_option("-l", "--lock-chunks", action="store_true", dest="lock_chunks", default=False, help="User LOCK TABLES for each chunk")
    parser.add_option("--sleep", dest="sleep_millis", type="int", default=0, help="Number of milliseconds to sleep between chunks. Default: 0")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
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


def act_query(conn, query):
    """
    Run the given query, commit changes
    """
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.commit()

    
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


def get_auto_increment_column(conn, read_table_name):
    auto_increment_column_name = None
    
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA='%s' 
            AND TABLE_NAME='%s' 
            AND LOCATE('auto_increment', EXTRA) > 0
        """ % (database_name, read_table_name)
    
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        auto_increment_column_name = row['COLUMN_NAME'].lower()

    cursor.close()
    return auto_increment_column_name


def get_table_engine(conn):
    engine = None
    
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT ENGINE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='%s' 
            AND TABLE_NAME='%s' 
        """ % (database_name, table_name)
    
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        engine = row['ENGINE'].lower()
        verbose("Table %s.%s is of engine %s" % (database_name, table_name, engine))

    cursor.close()
    return engine


def validate_no_triggers_exist(conn):
    """
    No 'AFTER' triggers allowed on table, since this utility creates all three AFTER 
    triggers (INSERT, UPDATE, DELETE)
    """
   
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT COUNT(*) AS count 
        FROM INFORMATION_SCHEMA.TRIGGERS 
        WHERE TRIGGER_SCHEMA='%s' 
            AND EVENT_OBJECT_TABLE='%s' 
            AND ACTION_TIMING='AFTER'
        """ % (database_name, table_name)
    
    cursor.execute(query)
    row = cursor.fetchone()
    count = int(row['count'])

    cursor.close()
    return count == 0

def table_exists(conn, check_table_name):
    """
    See if the a given table exists:
    """
    count = 0
    
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT COUNT(*) AS count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA='%s' 
            AND TABLE_NAME='%s'
        """ % (database_name, check_table_name)
    
    cursor.execute(query)
    row = cursor.fetchone()
    count = int(row['count'])

    cursor.close()
    return count

def drop_table(conn, drop_table_name):
    """
    """
    if table_exists(conn, drop_table_name):
        query = "DROP TABLE IF EXISTS %s.%s" % (database_name, drop_table_name)
        act_query(conn, query)
        verbose("Table %s.%s was found and dropped" % (database_name, drop_table_name))
    

def create_ghost_table(conn):
    """
    """
 
    drop_table(conn, ghost_table_name)

    query = "CREATE TABLE %s.%s LIKE %s.%s" % (database_name, ghost_table_name, database_name, table_name)
    act_query(conn, query)
    verbose("Table %s.%s has been created" % (database_name, ghost_table_name))
    

def alter_ghost_table(conn):
    """
    """
    
    if not options.alter_statement:
        verbose("No ALTER statement provided")
        return
    query = "ALTER TABLE %s.%s %s" % (database_name, ghost_table_name, options.alter_statement)
    act_query(conn, query)
    verbose("Table %s.%s has been altered" % (database_name, ghost_table_name))


def truncate_ghost_table(conn):
    """
    TRUNCATE is not allowed within LOCK TABLES scope, do we got for hard DELETE.
    We expect this to be quick, though, as it is performed instantly after adding the trigegrs,
    so not many rows are expected.
    """
    
    query = "DELETE FROM %s.%s" % (database_name, ghost_table_name)
    act_query(conn, query)
    verbose("Table %s.%s has been truncated" % (database_name, ghost_table_name))


def get_table_columns(conn, read_table_name):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA='%s' 
            AND TABLE_NAME='%s' 
        """ % (database_name, read_table_name)
    cursor.execute(query)

    column_names = set([row["COLUMN_NAME"].lower() for row in cursor.fetchall()])

    cursor.close()
    return column_names


def get_shared_columns(conn):
    original_columns = get_table_columns(conn, table_name)
    ghost_columns = get_table_columns(conn, ghost_table_name)
    shared_columns  = original_columns.intersection(ghost_columns)
    
    return shared_columns


def lock_tables_write(conn):
    query = """
        LOCK TABLES %s.%s WRITE, %s.%s WRITE
        """ % (database_name, table_name, database_name, ghost_table_name)
    act_query(conn, query)
    verbose("Tables locked WRITE")


def lock_tables_read(conn):
    query = """
       LOCK TABLES %s.%s READ, %s.%s WRITE
         """ % (database_name, table_name, database_name, ghost_table_name)
    act_query(conn, query)
    verbose("Tables locked READ, WRITE")


def unlock_tables(conn):
    query = """
        UNLOCK TABLES
        """
    act_query(conn, query)
    verbose("Tables unlocked")


def create_custom_triggers(conn):
    query = """
        CREATE TRIGGER %s.%s_AD_oak AFTER DELETE ON %s.%s
        FOR EACH ROW
            DELETE FROM %s.%s WHERE %s = OLD.%s;
        """ % (database_name, table_name, database_name, table_name, database_name, ghost_table_name, auto_increment_column_name, auto_increment_column_name)
    act_query(conn, query)
    verbose("Created AD trigger")

    shared_columns_listing = ", ".join(shared_columns)
    shared_columns_new_listing = ", ".join(["NEW.%s" % column_name for column_name in shared_columns])

    query = """
        CREATE TRIGGER %s.%s_AU_oak AFTER UPDATE ON %s.%s
        FOR EACH ROW
            REPLACE INTO %s.%s (%s) VALUES (%s);
        """ % (database_name, table_name, database_name, table_name, database_name, ghost_table_name, shared_columns_listing, shared_columns_new_listing)
    act_query(conn, query)
    verbose("Created AU trigger")

    query = """
        CREATE TRIGGER %s.%s_AI_oak AFTER INSERT ON %s.%s
        FOR EACH ROW
            REPLACE INTO %s.%s (%s) VALUES (%s);
        """ % (database_name, table_name, database_name, table_name, 
               database_name, ghost_table_name, shared_columns_listing, shared_columns_new_listing)
    act_query(conn, query)
    verbose("Created AI trigger")


def drop_custom_triggers(conn):
    cursor = conn.cursor()
    query = """
        DROP TRIGGER IF EXISTS %s.%s_AD_oak
        """ % (database_name, table_name)
    cursor.execute(query)
    verbose("Dropped custom AD trigger")

    query = """
        DROP TRIGGER IF EXISTS %s.%s_AI_oak
        """ % (database_name, table_name)
    cursor.execute(query)
    verbose("Dropped custom AI trigger")

    query = """
        DROP TRIGGER IF EXISTS %s.%s_AU_oak
        """ % (database_name, table_name)
    cursor.execute(query)
    verbose("Dropped custom AU trigger")

    cursor.close()
    

def get_auto_increment_range(conn):
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT 
          IFNULL(MIN(%s),0) AS auto_increment_min_value, 
          IFNULL(MAX(%s),0) AS auto_increment_max_value 
        FROM %s.%s
        """ % (auto_increment_column_name, auto_increment_column_name, 
               database_name, table_name)
    cursor.execute(query)
    row = cursor.fetchone()
    auto_increment_min_value = int(row['auto_increment_min_value'])
    auto_increment_max_value = int(row['auto_increment_max_value'])
    verbose("%s (min, max) values: (%d, %d)" % (auto_increment_column_name, auto_increment_min_value, auto_increment_max_value))

    cursor.close()
    return auto_increment_min_value, auto_increment_max_value
    
    
def get_auto_increment_range_end(auto_increment_range_start):    
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT MAX(%s) AS auto_increment_range_end
        FROM (SELECT %s FROM %s.%s 
          WHERE %s >= %d 
          AND %s <= %d  
          ORDER BY %s LIMIT %d) SEL1
        """ % (auto_increment_column_name, 
               auto_increment_column_name, database_name, table_name, 
               auto_increment_column_name, auto_increment_range_start, 
               auto_increment_column_name, auto_increment_max_value, 
               auto_increment_column_name, options.chunk_size)
        
    cursor.execute(query)
    row = cursor.fetchone()
    auto_increment_range_end = int(row['auto_increment_range_end'])

    cursor.close()
    return auto_increment_range_end

    
def copy_data_pass(conn):
    if auto_increment_min_value is None:
        return
    

    shared_columns_listing = ", ".join(shared_columns)
    auto_increment_range_start = auto_increment_min_value
    while auto_increment_range_start < auto_increment_max_value:
        auto_increment_range_end = get_auto_increment_range_end(auto_increment_range_start)
        progress = int(100.0*(auto_increment_range_start-auto_increment_min_value)/(auto_increment_max_value-auto_increment_min_value))
        engine_flags = ""
        if table_engine == "innodb":
            engine_flags = "LOCK IN SHARE MODE"
            
        query = """
            INSERT IGNORE INTO %s.%s (%s) 
                (SELECT %s FROM %s.%s WHERE %s BETWEEN %d AND %d
                %s)
            """ % (database_name, ghost_table_name, shared_columns_listing, 
                shared_columns_listing, database_name, table_name, auto_increment_column_name, auto_increment_range_start, auto_increment_range_end,
                engine_flags)
        if options.lock_chunks:
            lock_tables_read(conn)
            
        verbose("Copying range (%d, %d), %d%% progress" % (auto_increment_range_start, auto_increment_range_end, progress))
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()

        if options.lock_chunks:
            unlock_tables(conn)
        auto_increment_range_start = auto_increment_range_end+1
        
        if options.sleep_millis > 0:
            verbose("Will sleep for %f seconds" % (options.sleep_millis/1000.0))
            time.sleep(options.sleep_millis/1000.0)

    
def delete_data_pass(conn):
    if auto_increment_min_value is None:
        return
    

    shared_columns_listing = ", ".join(shared_columns)
    auto_increment_range_start = auto_increment_min_value
    while auto_increment_range_start < auto_increment_max_value:
        auto_increment_range_end = get_auto_increment_range_end(auto_increment_range_start)
        progress = int(100.0*(auto_increment_range_start-auto_increment_min_value)/(auto_increment_max_value-auto_increment_min_value))
        verbose("Deleting range (%d, %d), %d%% progress" % (auto_increment_range_start, auto_increment_range_end, progress))
        query = """
            DELETE FROM %s.%s
            WHERE %s BETWEEN %d AND %d
            AND %s NOT IN
                (SELECT %s FROM %s.%s WHERE %s BETWEEN %d AND %d)
            """ % (database_name, ghost_table_name, 
                auto_increment_column_name, auto_increment_range_start, auto_increment_range_end,
                auto_increment_column_name,
                auto_increment_column_name, database_name, table_name, auto_increment_column_name, auto_increment_range_start, auto_increment_range_end)
            
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()

        auto_increment_range_start = auto_increment_range_end+1

        if options.sleep_millis > 0:
            verbose("Will sleep for %f seconds" % (options.sleep_millis/1000.0))
            time.sleep(options.sleep_millis/1000.0)
    
 

def rename_tables(conn):
    """
    """
    
    drop_table(conn, archive_table_name)
    query = """
        RENAME TABLE 
            %s.%s TO %s.%s, 
            %s.%s TO %s.%s
        """ % (database_name, table_name, database_name, archive_table_name,
               database_name, ghost_table_name, database_name, table_name, )
    act_query(conn, query)
    verbose("Table %s.%s has been renamed to %s.%s" % (database_name, table_name, database_name, archive_table_name))
    verbose("Table %s.%s has been renamed to %s.%s" % (database_name, ghost_table_name, database_name, table_name))

    
try:
    try:
        conn = None
        (options, args) = parse_options()

        if not options.table:
            print_error("No table specified. Specify with -t or --table")
            exit(1)
            
        if options.chunk_size <= 0:
            print_error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")
            exit(1)

        database_name = None
        table_name =  None
        
        if options.database:
            database_name=options.database
            
        table_tokens = options.table.split(".")
        table_name = table_tokens[-1]
        if len(table_tokens) == 2:
            database_name = table_tokens[0]
            
        if not database_name:
            print_error("No database specified. Specify with fully qualified table name or with -d or --database")
            exit(1)
        
        conn = open_connection()
        
        table_engine = get_table_engine(conn)
        if not table_engine:
            print_error("Table %s.%s does not exist" % (database_name, table_name))
            exit(1)

        auto_increment_column_name = get_auto_increment_column(conn, table_name)
        if not auto_increment_column_name:
            print_error("Table must have an AUTO_INCREMENT column")
            exit(1)
            
        ghost_table_name  = "__oak_"+table_name
        archive_table_name = "__arc_"+table_name
        
        drop_custom_triggers(conn)
        if not validate_no_triggers_exist(conn):
            print_error("Table must not have any 'AFTER' triggers defined.")
            exit(1)
            
        create_ghost_table(conn)
        alter_ghost_table(conn)
        
        ghost_auto_increment_column_name = get_auto_increment_column(conn, ghost_table_name)
        if not ghost_auto_increment_column_name:
            drop_table(conn, ghost_table_name)
            print_error("Altered table must have an AUTO_INCREMENT column")
            exit(1)
        if ghost_auto_increment_column_name != auto_increment_column_name:
            drop_table(conn, ghost_table_name)
            print_error("Altered table must not change the AUTO_INCREMENT column name")
            exit(1)

        shared_columns = get_shared_columns(conn)
            
        create_custom_triggers(conn)
        lock_tables_write(conn)
        #truncate_ghost_table(conn)
        auto_increment_min_value, auto_increment_max_value = get_auto_increment_range(conn)
        unlock_tables(conn)
        
        copy_data_pass(conn)
        delete_data_pass(conn)
        
        rename_tables(conn)

        verbose("ALTER TABLE completed")
    except Exception, err:
        print err
finally:
    if conn:
        conn.close()
