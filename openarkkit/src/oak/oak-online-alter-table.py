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
import sys
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
    parser.add_option("-t", "--table", dest="table", help="Table to alter (optionally fully qualified)")
    parser.add_option("-g", "--ghost", dest="ghost", help="Table name to serve as ghost. This table will be created and synchronized with the original table")
    parser.add_option("-a", "--alter", dest="alter_statement", help="Comma delimited ALTER statement details, excluding the 'ALTER TABLE t' itself")
    parser.add_option("-c", "--chunk-size", dest="chunk_size", type="int", default=1000, help="Number of rows to act on in chunks. Default: 1000")
    parser.add_option("-l", "--lock-chunks", action="store_true", dest="lock_chunks", default=False, help="Use LOCK TABLES for each chunk")
    parser.add_option("-N", "--skip-binlog", dest="skip_binlog", action="store_true", default=False, help="Disable binary logging")
    parser.add_option("-r", "--max-lock-retries", type="int", dest="max_lock_retries", default="10", help="Maximum times to retry on deadlock or lock_wait_timeout. (default: 10; 0 is unlimited)")
    parser.add_option("--skip-delete-pass", dest="skip_delete_pass", action="store_true", default=False, help="Do not execute the DELETE data pass")
    parser.add_option("--sleep", dest="sleep_millis", type="int", default=0, help="Number of milliseconds to sleep between chunks. Default: 0")
    parser.add_option("", "--sleep-ratio", dest="sleep_ratio", type="float", default=0, help="Ratio of sleep time to execution time. Default: 0")
    parser.add_option("--cleanup", dest="cleanup", action="store_true", default=False, help="Remove custom triggers, ghost table from possible previous runs")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=True, help="Print user friendly messages")
    parser.add_option("-q", "--quiet", dest="verbose", action="store_false", help="Quiet mode, do not verbose")
    return parser.parse_args()

def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    sys.stderr.write("-- ERROR: %s\n" % message)

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


def get_session_variable_value(session_variable_name):
    query = """
        SELECT @%s AS %s
        """ % (session_variable_name, session_variable_name)
    row = get_row(query)
    session_variable_value = row[session_variable_name]

    return session_variable_value


def get_possible_unique_key_columns(read_table_name):
    """
    Return the columns with unique keys which are acceptable by this utility
    """
    verbose("Checking for UNIQUE columns on %s.%s, by which to chunk" % (database_name, read_table_name))
    query = """
        SELECT
          COLUMNS.TABLE_SCHEMA,
          COLUMNS.TABLE_NAME,
          COLUMNS.COLUMN_NAME,
          UNIQUES.INDEX_NAME,
          UNIQUES.COLUMN_NAMES,
          UNIQUES.COUNT_COLUMN_IN_INDEX,
          COLUMNS.DATA_TYPE,
          COLUMNS.CHARACTER_SET_NAME
        FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
          SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            INDEX_NAME,
            COUNT(*) AS COUNT_COLUMN_IN_INDEX,
            GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
            SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME
          FROM INFORMATION_SCHEMA.STATISTICS
          WHERE NON_UNIQUE=0
          GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
        ) AS UNIQUES
        ON (
          COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
          COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
          COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
        )
        WHERE
          COLUMNS.TABLE_SCHEMA = '%s'
          AND COLUMNS.TABLE_NAME = '%s'
        ORDER BY
          COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
          CASE UNIQUES.INDEX_NAME
            WHEN 'PRIMARY' THEN 0
            ELSE 1
          END,
          CASE IFNULL(CHARACTER_SET_NAME, '')
              WHEN '' THEN 0
              ELSE 1
          END,
          CASE DATA_TYPE
            WHEN 'tinyint' THEN 0
            WHEN 'smallint' THEN 1
            WHEN 'int' THEN 2
            WHEN 'bigint' THEN 3
            ELSE 100
          END,
          COUNT_COLUMN_IN_INDEX
        """ % (database_name, read_table_name)
    rows = get_rows(query)
    return rows


def get_possible_unique_key_column_names_set(read_table_name):
    """
    Return the names of columns with acceptable unique keys
    """
    rows = get_possible_unique_key_columns(read_table_name)
    possible_unique_key_column_names_set = [row["COLUMN_NAMES"].lower() for row in rows]

    verbose("Possible UNIQUE KEY column names in %s.%s:" % (database_name, read_table_name))
    for possible_unique_key_column_names in possible_unique_key_column_names_set:
        verbose("- %s" % possible_unique_key_column_names)

    return set(possible_unique_key_column_names_set)


def get_shared_unique_key_columns(shared_unique_key_column_names_set):
    """
    Choose a unique key, listed in the shared_unique_key_column_names_set (thus
    it is shared between the original and ghost table), and return the list of
    columns covered by the key.
    """

    rows = get_possible_unique_key_columns(original_table_name)

    original_table_unique_key_name = None
    unique_key_column_names = None
    unique_key_type = None
    count_columns_in_unique_key = None
    if rows:
        verbose("- Found following possible unique keys:")
        for row in rows:
            column_names = row["COLUMN_NAMES"].lower()
            if column_names in shared_unique_key_column_names_set:
                column_data_type = row["DATA_TYPE"].lower()
                character_set_name = row["CHARACTER_SET_NAME"]
                verbose("- %s (%s)" % (column_names, column_data_type))
                if unique_key_column_names is None:
                    unique_key_column_names = column_names
                    original_table_unique_key_name = row["INDEX_NAME"]
                    count_columns_in_unique_key = int(row["COUNT_COLUMN_IN_INDEX"])
                    if character_set_name is not None:
                        unique_key_type = "text"
                    elif column_data_type in ["tinyint", "smallint", "mediumint", "int", "bigint"]:
                        unique_key_type = "integer"
                    elif column_data_type in ["time", "date", "timestamp", "datetime"]:
                        unique_key_type = "temporal"

        verbose("Chosen unique key is '%s'" % unique_key_column_names)

    return unique_key_column_names, original_table_unique_key_name, count_columns_in_unique_key, unique_key_type


def get_table_engine():
    """
    Return the storage engine (lowercase) the given table belongs to.
    """
    engine = None

    query = """
        SELECT ENGINE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='%s'
            AND TABLE_NAME='%s'
        """ % (database_name, original_table_name)

    row = get_row(query)
    if row:
        engine = row['ENGINE'].lower()
        verbose("Table %s.%s is of engine %s" % (database_name, original_table_name, engine))

    return engine


def validate_no_after_triggers_exist():
    """
    No 'AFTER' triggers allowed on table, since this utility creates all three AFTER
    triggers (INSERT, UPDATE, DELETE)
    """

    query = """
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TRIGGERS
        WHERE TRIGGER_SCHEMA='%s'
            AND EVENT_OBJECT_TABLE='%s'
            AND ACTION_TIMING='AFTER'
        """ % (database_name, original_table_name)

    row = get_row(query)
    count = int(row['count'])

    return count == 0


def validate_no_foreign_keys_exist():
    """
    At the moment, no foreign keys are allowed
    """

    query = """
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
            REFERENCED_TABLE_NAME IS NOT NULL
            AND ((TABLE_SCHEMA='%s' AND TABLE_NAME='%s')
              OR (REFERENCED_TABLE_SCHEMA='%s' AND REFERENCED_TABLE_NAME='%s')
            )
        """ % (database_name, original_table_name,
               database_name, original_table_name)

    row = get_row(query)
    count = int(row['count'])

    return count == 0


def table_exists(check_table_name):
    """
    See if the a given table exists:
    """
    if not check_table_name:
        return 0

    count = 0

    query = """
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='%s'
            AND TABLE_NAME='%s'
        """ % (database_name, check_table_name)

    row = get_row(query)
    count = int(row['count'])

    return count


def drop_table(drop_table_name):
    """
    Drop the given table
    """
    if not drop_table_name:
        return
    if table_exists(drop_table_name):
        query = "DROP TABLE IF EXISTS %s.%s" % (database_name, drop_table_name)
        act_query(query)
        verbose("Table %s.%s was found and dropped" % (database_name, drop_table_name))


def create_ghost_table():
    """
    Create the ghost table in the likes of the original table.
    Later on, it will be altered.
    """

    drop_table(ghost_table_name)

    query = "CREATE TABLE %s.%s LIKE %s.%s" % (database_name, ghost_table_name, database_name, original_table_name)
    act_query(query)
    verbose("Table %s.%s has been created" % (database_name, ghost_table_name))


def alter_ghost_table():
    """
    Perform the ALTER TABLE on the ghost table
    """

    if not options.alter_statement:
        verbose("No ALTER statement provided")
        return
    query = "ALTER TABLE %s.%s %s" % (database_name, ghost_table_name, options.alter_statement)
    act_query(query)
    verbose("Table %s.%s has been altered" % (database_name, ghost_table_name))


def get_table_columns(read_table_name):
    """
    Return the list of column names (lowercase) for the given table
    """
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='%s'
            AND TABLE_NAME='%s'
        ORDER BY ORDINAL_POSITION
        """ % (database_name, read_table_name)
    column_names = set([row["COLUMN_NAME"].lower() for row in get_rows(query)])

    return column_names


def get_shared_columns():
    """
    Return the set of columns which are shared between the original table
    and the ghost (altered) table.
    """
    original_columns = get_table_columns(original_table_name)
    ghost_columns = get_table_columns(ghost_table_name)
    shared_columns  = original_columns.intersection(ghost_columns)
    verbose("Shared columns: %s" % ", ".join(shared_columns))

    return shared_columns


def lock_tables_write():
    """
    Lock the original and ghost tables in WRITE mode.
    This can fail due to InnoDB deadlocks, so we keep trying endlessly until it succeeds.
    """
    query = """
        LOCK TABLES %s.%s WRITE, %s.%s WRITE
        """ % (database_name, original_table_name, database_name, ghost_table_name)
    verbose("Attempting to lock tables")
    lock_succeeded = False
    while not lock_succeeded:
        try:
            act_query(query)
            lock_succeeded = True
        except:
            verbose("...")
            time.sleep(0.1)
    print
    verbose("Tables locked WRITE")


def lock_tables_read():
    query = """
       LOCK TABLES %s.%s READ, %s.%s WRITE
         """ % (database_name, original_table_name, database_name, ghost_table_name)
    act_query(query)
    verbose("Tables locked READ, WRITE")


def unlock_tables():
    query = """
        UNLOCK TABLES
        """
    act_query(query)
    verbose("Tables unlocked")


def create_custom_triggers():
    """
    Create the three 'AFTER' triggers on the original table
    """
    unique_key_column_names_old = ",".join(["OLD.%s" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list])
    query = """
        CREATE TRIGGER %s.%s AFTER DELETE ON %s.%s
        FOR EACH ROW
            DELETE FROM %s.%s WHERE (%s) = (%s);
        """ % (database_name, after_delete_trigger_name, database_name, original_table_name,
               database_name, ghost_table_name, unique_key_column_names, unique_key_column_names_old)
    act_query(query)
    verbose("Created AD trigger")

    shared_columns_listing = ", ".join(["`%s`" % shared_column for shared_column in shared_columns])
    shared_columns_new_listing = ", ".join(["NEW.`%s`" % column_name for column_name in shared_columns])

    # Reason for the DELETE in the AFTER UPDATE trigger is that the UPDATE query may 
    # modify the columns used by the chunking index itself, in which case the REPLACE does not 
    # remove the row from the ghost table.
    query = """
        CREATE TRIGGER %s.%s AFTER UPDATE ON %s.%s
        FOR EACH ROW
        BEGIN
            DELETE FROM %s.%s WHERE (%s) = (%s);
            REPLACE INTO %s.%s (%s) VALUES (%s);
        END
        """ % (database_name, after_update_trigger_name, database_name, original_table_name,
               database_name, ghost_table_name, unique_key_column_names, unique_key_column_names_old,
               database_name, ghost_table_name, shared_columns_listing, shared_columns_new_listing)
    act_query(query)
    verbose("Created AU trigger")

    query = """
        CREATE TRIGGER %s.%s AFTER INSERT ON %s.%s
        FOR EACH ROW
            REPLACE INTO %s.%s (%s) VALUES (%s);
        """ % (database_name, after_insert_trigger_name, database_name, original_table_name,
               database_name, ghost_table_name, shared_columns_listing, shared_columns_new_listing)
    act_query(query)
    verbose("Created AI trigger")


def trigger_exists(trigger_name):
    """
    See if the given trigger exists on the original table
    """

    query = """
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TRIGGERS
        WHERE TRIGGER_SCHEMA='%s'
            AND EVENT_OBJECT_TABLE='%s'
            AND TRIGGER_NAME='%s'
        """ % (database_name, original_table_name, trigger_name)

    row = get_row(query)
    count = int(row['count'])

    return count


def drop_custom_trigger(trigger_name):
    if not trigger_name:
        return
    if trigger_exists(trigger_name):
        query = """
            DROP TRIGGER IF EXISTS %s.%s
            """ % (database_name, trigger_name)
        act_query(query)
        verbose("Dropped custom trigger %s" % trigger_name)


def drop_custom_triggers():
    """
    Cleanup
    """
    drop_custom_trigger(after_delete_trigger_name)
    drop_custom_trigger(after_update_trigger_name)
    drop_custom_trigger(after_insert_trigger_name)


def get_unique_key_min_values_variables():
    return ",".join(["@unique_key_min_value_%d" % i for i in range(0,count_columns_in_unique_key)])

def get_unique_key_max_values_variables():
    return ",".join(["@unique_key_max_value_%d" % i for i in range(0,count_columns_in_unique_key)])

def get_unique_key_range_start_variables():
    return ",".join(["@unique_key_range_start_%d" % i for i in range(0,count_columns_in_unique_key)])

def get_unique_key_range_end_variables():
    return ",".join(["@unique_key_range_end_%d" % i for i in range(0,count_columns_in_unique_key)])

def get_unique_key_range():
    """
    Return the first and last unique key values in the original table
    """
    query = """
        SELECT
          %s
        FROM %s.%s
        ORDER BY %s LIMIT 1
        INTO %s
        """ % (unique_key_column_names,
               database_name, original_table_name,
               ",".join(["%s ASC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]),
               get_unique_key_min_values_variables())
    act_query(query)

    query = """
        SELECT
          %s
        FROM %s.%s
        ORDER BY %s LIMIT 1
        INTO %s
        """ % (unique_key_column_names,
               database_name, original_table_name,
               ",".join(["%s DESC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]),
               get_unique_key_max_values_variables())
    act_query(query)

    query = """
        SELECT
          COUNT(*) FROM (
            SELECT NULL
            FROM %s.%s LIMIT 1
          ) SEL1 INTO @range_exists
        """ % (database_name, original_table_name)
    act_query(query)

    unique_key_min_values = [get_session_variable_value("unique_key_min_value_%d" % i) for i in range(0,count_columns_in_unique_key)]
    unique_key_max_values = [get_session_variable_value("unique_key_max_value_%d" % i) for i in range(0,count_columns_in_unique_key)]
    range_exists = int(get_session_variable_value("range_exists"))
    verbose("%s (min, max) values: (%s, %s)" % (unique_key_column_names, unique_key_min_values, unique_key_max_values))

    return unique_key_min_values, unique_key_max_values, range_exists


def get_value_comparison(column, value, comparison_sign):
    """
    Given a column, value and comparison sign, return the SQL comparison of the two.
    e.g. 'id', 7, '<'
    results with (id < 7)
    """
    return "(%s %s %s)" % (column, comparison_sign, value)


def get_multiple_columns_equality(columns, values):
    """
    Given a list of columns and a list of values (of same length), produce an
    SQL equality of the form:
    ((col1 = val1) AND (col2 = val2) AND...)
    """
    if not columns:
        return ""
    equalities = ["(%s = %s)" % (column, value) for (column, value) in zip(columns, values)]
    return "(%s)" % " AND ".join(equalities)


def get_multiple_columns_non_equality_comparison(columns, values, comparison_sign, include_equality=False):
    """
    Given a list of columns and a list of values (of same length), produce a
    'less than' or 'greater than' (optionally 'or equal') SQL equasion, by splitting into multiple conditions.
    An example result may look like:
    (col1 < val1) OR
    ((col1 = val1) AND (col2 < val2)) OR
    ((col1 = val1) AND (col2 = val2) AND (col3 < val3)) OR
    ((col1 = val1) AND (col2 = val2) AND (col3 = val3)))
    Which stands for (col1, col2, col3) <= (val1, val2, val3).
    The latter being simple in representation, however MySQL does not utilize keys
    properly with this form of condition, hence the splitting into multiple conditions.
    """
    comparisons = []
    for i in range(0,len(columns)):
        equalities_comparison = get_multiple_columns_equality(columns[0:i], values[0:i])
        range_comparison = get_value_comparison(columns[i], values[i], comparison_sign)
        if equalities_comparison:
            comparison = "(%s AND %s)" % (equalities_comparison, range_comparison)
        else:
            comparison = range_comparison
        comparisons.append(comparison)
    if include_equality:
        comparisons.append(get_multiple_columns_equality(columns, values))
    return "(%s)" % " OR ".join(comparisons)


def get_multiple_columns_non_equality_comparison_by_names(delimited_columns_names, delimited_values, comparison_sign, include_equality=False):
    """
    Assumes 'delimited_columns_names' is comma delimited column names, 'delimited_values' is comma delimited values.
    @see: get_multiple_columns_non_equality_comparison()
    """
    columns = delimited_columns_names.split(",")
    values = delimited_values.split(",")
    return get_multiple_columns_non_equality_comparison(columns, values, comparison_sign, include_equality)


def set_unique_key_range_end(first_round):
    """
    Get the range end: calculate the highest value in the next chunk of rows.
    """

    limit_count = options.chunk_size
    if not first_round:
        limit_count += 1

    query = """
        SELECT %s
        FROM (SELECT %s FROM %s.%s
          WHERE
                %s
            AND
                %s
          ORDER BY %s LIMIT %d) SEL1
        ORDER BY %s LIMIT 1
        INTO %s
        """ % (unique_key_column_names,
               unique_key_column_names, database_name, original_table_name,
               get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_start_variables(), ">", True),
               get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_max_values_variables(), "<", True),
               ",".join(["%s ASC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]), limit_count,
               ",".join(["%s DESC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]),
               get_unique_key_range_end_variables())
    act_query(query)


def set_unique_key_next_range_start():
    """
    Calculate the starting point of the next range
    """
    query = "SELECT %s INTO %s" % (get_unique_key_range_end_variables(), get_unique_key_range_start_variables())
    act_query(query)


def is_range_overflow(first_round):
    if first_round:
        return False

    query = """
        SELECT (%s) >= (%s) AS range_overflow
        """ % (get_unique_key_range_start_variables(), get_unique_key_max_values_variables())
    row = get_row(query)
    range_overflow = int(row["range_overflow"])
    return range_overflow


def is_range_degenerated():
    """
    Contributed, see Issue 29: oak-online-alter-table stuck in an endless loop
    """
    query = """
        SELECT (%s) >= (%s) AS range_degenerated
        """ % (get_unique_key_range_start_variables(), get_unique_key_range_end_variables())
    row = get_row(query)
    range_degenerated = int(row["range_degenerated"])
    return range_degenerated


def get_eta_seconds(elapsed_times, ratio_complete):
    if not elapsed_times:
        return 0

    if ratio_complete == 0:
        return 0

    e0, e1 = elapsed_times[0][0], elapsed_times[-1][0]
    r0, r1 = elapsed_times[0][1], elapsed_times[-1][1]
    elapsed_time = e1

    if r1 == r0:
        return 0

    estimated_total_time = e0 + (1.0 - r0)*(e1 - e0)/(r1 - r0)
    eta_seconds = estimated_total_time - elapsed_time
    return eta_seconds


def get_eta_presentation(eta_seconds, data_valid):
    if not data_valid:
        return "N/A"
    eta_seconds = round(eta_seconds+0.5)
    hours = eta_seconds / (60*60)
    minutes = (eta_seconds / 60) % 60
    seconds = eta_seconds % 60
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


def get_progress_and_eta_presentation(elapsed_times, elapsed_time, ratio_complete):
    elapsed_times.append((elapsed_time, ratio_complete,))
    elapsed_times = elapsed_times[-5:]
    progress = int(100.0 * ratio_complete)
    #eta_seconds = get_eta_seconds(elapsed_times, ratio_complete)
    #return "progress: %d%%, ETA: %s" % (progress, get_eta_presentation(eta_seconds, len(elapsed_times) >= 5))
    return "progress: %d%%" % progress


def to_string_list(list):
    return ["%s" % val for val in list]


def sleep_after_chunk(query_execution_time):
    sleep_seconds = None
    if options.sleep_millis > 0:
        sleep_seconds = options.sleep_millis/1000.0
    elif options.sleep_ratio > 0:
        sleep_seconds = options.sleep_ratio * query_execution_time
    if sleep_seconds:
        verbose("+ Will sleep for %s seconds" % round(sleep_seconds, 2))
        time.sleep(sleep_seconds)


def act_data_pass(first_data_pass_query, rest_data_pass_query, description):
    # Is there any range to work with, at all?
    if not range_exists:
        return

    query = """
        SELECT %s INTO %s
        """ % (get_unique_key_min_values_variables(), get_unique_key_range_start_variables())
    act_query(query)

    start_time = time.time()
    elapsed_times = []

    total_num_affected_rows = 0
    first_round = True
    total_num_attempts = 0
    while not is_range_overflow(first_round):
        if first_round:
            execute_data_pass_query = first_data_pass_query
        else:
            execute_data_pass_query = rest_data_pass_query
        elapsed_time = time.time() - start_time

        set_unique_key_range_end(first_round)
        first_round = False

        unique_key_range_start_values = [get_session_variable_value("unique_key_range_start_%d" % i) for i in range(0,count_columns_in_unique_key)]
        unique_key_range_end_values = [get_session_variable_value("unique_key_range_end_%d" % i) for i in range(0,count_columns_in_unique_key)]

        if total_num_attempts % 20 == 0:
            verbose("- Reminder: altering %s.%s: %s..." % (database_name, original_table_name, options.alter_statement[0:30])) 
        if unique_key_type == "integer":
            ratio_complete_query = """
                SELECT
                    IFNULL((@unique_key_range_start_0-@unique_key_min_value_0)/
                    (@unique_key_max_value_0-@unique_key_min_value_0), 1)
                    AS ratio_complete
                """
            ratio_complete = float(get_row(ratio_complete_query)["ratio_complete"])
            verbose("%s range (%s), (%s), %s" % (description, ",".join(to_string_list(unique_key_range_start_values)), ",".join(to_string_list(unique_key_range_end_values)), get_progress_and_eta_presentation(elapsed_times, elapsed_time, ratio_complete)))
        elif unique_key_type == "temporal":
            ratio_complete_query = """
                SELECT
                    IFNULL(TIMESTAMPDIFF(SECOND, @unique_key_min_value_0, @unique_key_range_start_0)/
                    TIMESTAMPDIFF(SECOND, @unique_key_min_value_0, @unique_key_max_value_0), 1)
                    AS ratio_complete
                """
            ratio_complete = float(get_row(ratio_complete_query)["ratio_complete"])
            verbose("%s range ('%s', '%s'), %s" % (description, ",".join(unique_key_range_start_values), ",".join(unique_key_range_end_values), get_progress_and_eta_presentation(elapsed_times, elapsed_time, ratio_complete)))
        else:
            verbose("%s range (%s), (%s), progress: N/A" % (description, ",".join(unique_key_range_start_values), ",".join(unique_key_range_end_values)))

        if options.lock_chunks:
            lock_tables_read()
            
        retry_data_pass = True
        num_attempts = 0
        query_execution_time = 0
        while retry_data_pass:
            try:
                query_start_time = time.time()
                total_num_attempts += 1
                num_affected_rows = act_query(execute_data_pass_query)
                total_num_affected_rows += num_affected_rows
                query_execution_time = (time.time() - query_start_time)
                retry_data_pass = False
            except Exception, err:
                print_error("Failed chunk: %s" % err)
                sleep_after_chunk(1)
                num_attempts += 1
            if (num_attempts >= options.max_lock_retries) and (options.max_lock_retries > 0):
                retry_data_pass = False
            if retry_data_pass:
                verbose("Retrying same chunk %s/%s" % (num_attempts, options.max_lock_retries))

        if options.lock_chunks:
            unlock_tables()

        if is_range_degenerated():
            break
        
        set_unique_key_next_range_start()

        sleep_after_chunk(query_execution_time)
    verbose("%s range 100%% complete. Number of rows: %s" % (description, total_num_affected_rows))


def copy_data_pass():
    shared_columns_listing = ", ".join(["`%s`" % shared_column for shared_column in shared_columns])
    
    # We generate two queries: 
    # one for first round (includes range start value, or >=),
    # oen for all the rest (skips range start, or >)

    engine_flags = ""
    if table_engine == "innodb":
        engine_flags = "LOCK IN SHARE MODE"
        
    data_pass_queries = ["""
        INSERT IGNORE INTO %s.%s (%s)
            (SELECT %s FROM %s.%s FORCE INDEX (%s)
            WHERE 
                (%s
                AND
                %s)
            %s)
        """ % (database_name, ghost_table_name, shared_columns_listing,
            shared_columns_listing, database_name, original_table_name, original_table_unique_key_name,
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_start_variables(), ">", first_round),
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_end_variables(), "<", True),
            engine_flags
            ) for first_round in [True, False]]
    first_data_pass_query = data_pass_queries[0]
    rest_data_pass_query = data_pass_queries[1]

    act_data_pass(first_data_pass_query, rest_data_pass_query, "Copying")


def delete_data_pass():
    shared_columns_listing = ", ".join(shared_columns)
    data_pass_queries = ["""
        DELETE FROM %s.%s
        WHERE
            (%s
            AND
            %s)
        AND (%s) NOT IN
            (SELECT %s FROM %s.%s
            WHERE
                (%s
                AND
                %s)
            )
        """ % (database_name, ghost_table_name,
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_start_variables(), ">", first_round),
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_end_variables(), "<", True),
            unique_key_column_names,
            unique_key_column_names, database_name, original_table_name,
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_start_variables(), ">", first_round),
            get_multiple_columns_non_equality_comparison_by_names(unique_key_column_names, get_unique_key_range_end_variables(), "<", True)
            ) for first_round in [True, False]]
    first_data_pass_query = data_pass_queries[0]
    rest_data_pass_query = data_pass_queries[1]

    act_data_pass(first_data_pass_query, rest_data_pass_query, "Deleting")


def rename_tables():
    """
    """

    drop_table(archive_table_name)
    query = """
        RENAME TABLE
            %s.%s TO %s.%s,
            %s.%s TO %s.%s
        """ % (database_name, original_table_name, database_name, archive_table_name,
               database_name, ghost_table_name, database_name, original_table_name, )
    act_query(query)
    verbose("Table %s.%s has been renamed to %s.%s," % (database_name, original_table_name, database_name, archive_table_name))
    verbose("and table %s.%s has been renamed to %s.%s" % (database_name, ghost_table_name, database_name, original_table_name))


def cleanup():
    """
    Remove any data this utility may have created during this runtime or previous runtime.
    """
    if conn:
        unlock_tables()
        drop_custom_triggers()
        if not options.ghost:
            drop_table(ghost_table_name)
        drop_table(archive_table_name)


def exit_with_error(error_message):
    """
    Notify, cleanup and exit.
    """
    print_error("Errors found. Initiating cleanup")
    cleanup()
    print_error(error_message)
    sys.exit(1)


try:
    try:
        conn = None
        (options, args) = parse_options()

        if not options.table:
            exit_with_error("No table specified. Specify with -t or --table")

        if options.chunk_size <= 0:
            exit_with_error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")

        database_name = None
        original_table_name =  None
        archive_table_name = None
        after_delete_trigger_name = None
        after_update_trigger_name = None
        after_insert_trigger_name = None

        if options.database:
            database_name=options.database

        table_tokens = options.table.split(".")
        original_table_name = table_tokens[-1]
        if len(table_tokens) == 2:
            database_name = table_tokens[0]

        if not database_name:
            exit_with_error("No database specified. Specify with fully qualified table name or with -d or --database")

        conn = open_connection()
        if options.skip_binlog:
            query = "SET SESSION SQL_LOG_BIN=0"
            act_query(query)
            verbose("Binary logging for session disabled")

        ghost_table_name = None
        if options.ghost:
            if table_exists(options.ghost):
                exit_with_error("Ghost table: %s.%s already exists." % (database_name, options.ghost))

        if options.ghost:
            ghost_table_name = options.ghost
        else:
            ghost_table_name = "__oak_"+original_table_name
        archive_table_name = "__arc_"+original_table_name

        after_delete_trigger_name = "%s_AD_oak" % original_table_name
        after_update_trigger_name = "%s_AU_oak" % original_table_name
        after_insert_trigger_name = "%s_AI_oak" % original_table_name

        if options.cleanup:
            # All we do now is clean up
            cleanup()
        else:
            table_engine = get_table_engine()
            if not table_engine:
                exit_with_error("Table %s.%s does not exist" % (database_name, original_table_name))

            drop_custom_triggers()
            if not validate_no_after_triggers_exist():
                exit_with_error("Table must not have any 'AFTER' triggers defined.")

            if not validate_no_foreign_keys_exist():
                exit_with_error("Table must not have any foreign keys defined (neither as parent nor child).")

            original_table_unique_key_names_set = get_possible_unique_key_column_names_set(original_table_name)
            if not original_table_unique_key_names_set:
                exit_with_error("Table must have a UNIQUE KEY on a single column")

            create_ghost_table()
            alter_ghost_table()

            ghost_table_unique_key_names_set = get_possible_unique_key_column_names_set(ghost_table_name)
            if not original_table_unique_key_names_set:
                exit_with_error("Altered table must have a UNIQUE KEY on a single column")

            shared_unique_key_column_names_set = original_table_unique_key_names_set.intersection(ghost_table_unique_key_names_set)

            if not shared_unique_key_column_names_set:
                exit_with_error("Altered table must retain at least one unique key")

            unique_key_column_names, original_table_unique_key_name, count_columns_in_unique_key, unique_key_type = get_shared_unique_key_columns(shared_unique_key_column_names_set)
            unique_key_column_names_list = unique_key_column_names.split(",")

            shared_columns = get_shared_columns()

            create_custom_triggers()
            lock_tables_write()
            unique_key_min_values, unique_key_max_values, range_exists = get_unique_key_range()
            unlock_tables()

            copy_data_pass()
            if not options.skip_delete_pass:
                delete_data_pass()

            if options.ghost:
                verbose("Ghost table creation completed. Note that triggers on %s.%s were not removed" % (database_name, original_table_name))
            else:
                rename_tables()
                drop_table(archive_table_name)
                verbose("ALTER TABLE completed")
    except Exception, err:
        print Exception, err
        exit_with_error(err)
finally:
    if conn:
        conn.close()
