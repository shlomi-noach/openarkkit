#!/usr/bin/python

#
# Chunk a given UPDATE/DELETE query, possibly over mutiple tables.
#
# Released under the BSD license
#
# Copyright (c) 2008-2010, Shlomi Noach
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
    parser.add_option("-d", "--database", dest="database", help="Database name (required unless query uses fully qualified table names)")
    parser.add_option("-e", "--execute", dest="execute_query", help="Query to execute, which contains a chunk placeholder in the form of OAK_CHUNK(table_name) (required)")
    parser.add_option("-c", "--chunk-size", dest="chunk_size", type="int", default=1000, help="Number of rows to act on in chunks (default: 1000). 0 means all rows updated in one operation")
    parser.add_option("", "--start-with", dest="start_with", default=None, help="Assuming chunking on numeric field (e.g. AUTO_INCREMENT), start chunking from this value and onward. Either provide a constant or a query returning a single integer value.")
    parser.add_option("", "--end-with", dest="end_with", default=None, help="Assuming chunking on numeric field (e.g. AUTO_INCREMENT), end chunking with this value. Either provide a constant or a query returning a single integer value.")
    parser.add_option("", "--terminate-on-not-found", dest="terminate_on_not_found", action="store_true", default=False, help="Terminate on first occurrence where chunking did not affect any rows (default: False)")
    parser.add_option("", "--force-chunking-column", dest="forced_chunking_column", default=None, help="Columns to chunk by; avoids querying in INFORMATION_SCHEMA. Format: either column_name:type, where type is integer/text/temporal - for single column keys, or column1_name,column2_name,... for one or more column keys, with no type.")
    parser.add_option("", "--skip-lock-tables", dest="skip_lock_tables", action="store_true", default=False, help="Do not issue a LOCK TABLES READ. May be required when using queries within --start-with or --end-with")
    parser.add_option("", "--skip-retry-chunk", dest="skip_retry_chunk", action="store_true", default=False, help="Avoid retrying a chunk operation on error. Default: false")
    parser.add_option("", "--no-log-bin", dest="no_log_bin", action="store_true", help="Do not log to binary log (actions will not replicate)")
    parser.add_option("", "--sleep", dest="sleep_millis", type="int", default=0, help="Number of milliseconds to sleep between chunks. Default: 0")
    parser.add_option("", "--sleep-ratio", dest="sleep_ratio", type="float", default=0, help="Ratio of sleep time to execution time. Default: 0")
    parser.add_option("", "--debug", dest="debug", action="store_true", help="Print stack trace on error")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    parser.add_option("", "--print-progress", dest="print_progress", action="store_true", help="Redundant. Use --verbose instead")
    return parser.parse_args()


def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    sys.stderr.write("-- ERROR: %s\n" % message)

def open_connection():
    if options.defaults_file:
        conn = MySQLdb.connect(
            read_default_file = options.defaults_file,
            db = database_name)
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


def get_row_nondict(query):
    connection = conn
    cursor = connection.cursor()
    cursor.execute(query)
    row = cursor.fetchone()

    cursor.close()
    return row


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


def table_exists(check_table_name):
    """
    See if the a given table exists:
    """
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


def get_selected_unique_key_column_names(read_table_name):
    """
    Return the names of columns with acceptable unique keys
    """
    if options.forced_chunking_column:
        forced_chunking_column_tokens = options.forced_chunking_column.split(",")
        if len(forced_chunking_column_tokens) > 1:
            # Multicolumn key; we do not expect column types, so it's just names
            verbose("Forced columns: %s" % options.forced_chunking_column)
            return options.forced_chunking_column, len(forced_chunking_column_tokens), None
        else:
            # Single column; let's see if it contains a type
            if len(options.forced_chunking_column.split(":")) > 1:
                column_name, unique_key_type = options.forced_chunking_column.split(":")
                verbose("Forced column %s of type %s" % (column_name, unique_key_type))
                return column_name, 1, unique_key_type
            else:
                verbose("Forced column %s of ungiven type" % options.forced_chunking_column)
                return options.forced_chunking_column, 1, None
    
    # No forcing; check for column in INFORMATION_SCHEMA
    rows = get_possible_unique_key_columns(read_table_name)
    if not rows:
        return None
    # Choose first result (result ordered by preferrable keys)
    row = rows[0]
    column_names = row["COLUMN_NAMES"].lower()
    count_columns_in_unique_key = int(row["COUNT_COLUMN_IN_INDEX"])
    column_data_type = row["DATA_TYPE"].lower()
    character_set_name = row["CHARACTER_SET_NAME"]

    unique_key_type = None
    if character_set_name is not None:
        unique_key_type = "text"
    elif column_data_type in ["tinyint", "smallint", "int", "bigint"]:
        unique_key_type = "integer"
    elif column_data_type in ["time", "date", "timestamp", "datetime"]:
        unique_key_type = "temporal"

    return column_names, count_columns_in_unique_key, unique_key_type


def lock_table_read():
    query = """
       LOCK TABLES %s.%s READ
         """ % (database_name, table_name)
    act_query(query)
    verbose("Table locked READ")


def unlock_table():
    query = """
        UNLOCK TABLES
        """
    act_query(query)
    verbose("Table unlocked")


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
    Return the first and last unique key values in the table
    """
    # First (lowest) unique key values:
    if options.start_with is not None:
        # Sanity:
        if unique_key_type == "integer" and count_columns_in_unique_key == 1:
            if options.start_with.isdigit():
                # Constant provided
                start_with = int(options.start_with)
            else:
                row = get_row_nondict(options.start_with)
                start_with = row[0]
            query = "SELECT %d INTO %s" % (start_with, get_unique_key_min_values_variables())
            verbose("Starting with: %d" % start_with)
        else:
            exit_with_error("--start-with only applies to single column integer chunking keys")
    else:
        query = """
            SELECT
              %s
            FROM %s.%s
            ORDER BY %s LIMIT 1
            INTO %s
            """ % (unique_key_column_names,
                   database_name, table_name,
                   ",".join(["%s ASC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]),
                   get_unique_key_min_values_variables())
    act_query(query)

    # Last (highest) unique key values:
    if options.end_with is not None:
        # Sanity:
        if unique_key_type == "integer" and count_columns_in_unique_key == 1:
            if options.end_with.isdigit():
                # Constant provided
                end_with = int(options.end_with)
            else:
                row = get_row_nondict(options.end_with)
                end_with = row[0]
            query = "SELECT %d INTO %s" % (end_with, get_unique_key_max_values_variables())
            verbose("Ending with: %d" % end_with)
        else:
            exit_with_error("--end-with only applies to single column integer chunking keys")
    else:
        query = """
            SELECT
              %s
            FROM %s.%s
            ORDER BY %s LIMIT 1
            INTO %s
            """ % (unique_key_column_names,
                   database_name, table_name,
                   ",".join(["%s DESC" % unique_key_column_name for unique_key_column_name in unique_key_column_names_list]),
                   get_unique_key_max_values_variables())
    act_query(query)

    # Number of rows
    query = """
        SELECT
          COUNT(*) FROM (
            SELECT NULL
            FROM %s.%s LIMIT 1
          ) SEL1 INTO @range_exists
        """ % (database_name, table_name)
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
    equalities = []
    for i in range(0,len(columns)):
        equalities.append(get_value_comparison(columns[i], values[i], "="))
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
               unique_key_column_names, database_name, table_name,
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


def get_progress_and_eta_presentation(ratio_complete):
    progress = int(100.0 * ratio_complete)
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
    """
    Do the chunk update loop. Main business goes here.
    """
    # Is there any range to work with, at all?
    if not range_exists:
        return
    
    start_time = time.time()

    query = """
        SELECT %s INTO %s
        """ % (get_unique_key_min_values_variables(), get_unique_key_range_start_variables())
    act_query(query)

    first_round = True
    total_num_affected_rows = 0
    accumulated_work_time = 0;
    while not is_range_overflow(first_round):
        try:
            # Different queries for first round and next rounds
            if first_round:
                execute_data_pass_query = first_data_pass_query
            else:
                execute_data_pass_query = rest_data_pass_query
            if not execute_data_pass_query:
                # Can happen when chunk-size=0, thus doing everything in one chunk
                break
    
            set_unique_key_range_end(first_round)
            first_round = False
    
            unique_key_range_start_values = [get_session_variable_value("unique_key_range_start_%d" % i) for i in range(0,count_columns_in_unique_key)]
            unique_key_range_end_values = [get_session_variable_value("unique_key_range_end_%d" % i) for i in range(0,count_columns_in_unique_key)]
                
            if unique_key_type == "integer":
                ratio_complete_query = """
                    SELECT
                        (@unique_key_range_start_0-@unique_key_min_value_0)/
                        (@unique_key_max_value_0-@unique_key_min_value_0)
                        AS ratio_complete
                    """
                ratio_complete = float(get_row(ratio_complete_query)["ratio_complete"])
                verbose("%s range (%s), (%s), %s" % (description, ",".join(to_string_list(unique_key_range_start_values)), ",".join(to_string_list(unique_key_range_end_values)), get_progress_and_eta_presentation(ratio_complete)))
            elif unique_key_type == "temporal":
                ratio_complete_query = """
                    SELECT
                        TIMESTAMPDIFF(SECOND, @unique_key_min_value_0, @unique_key_range_start_0)/
                        TIMESTAMPDIFF(SECOND, @unique_key_min_value_0, @unique_key_max_value_0)
                        AS ratio_complete
                    """
                ratio_complete = float(get_row(ratio_complete_query)["ratio_complete"])
                verbose("%s range ('%s', '%s'), %s" % (description, ",".join(to_string_list(unique_key_range_start_values)), ",".join(to_string_list(unique_key_range_end_values)), get_progress_and_eta_presentation(ratio_complete)))
            else:
                verbose("%s range (%s), (%s), progress: N/A" % (description, ",".join(to_string_list(unique_key_range_start_values)), ",".join(to_string_list(unique_key_range_end_values))))
    
            num_affected_rows = 0
            query_execution_time = 0
            retry_data_pass = True
            should_sleep_after_chunk = False
            while retry_data_pass:
                try:
                    query_start_time = time.time()
                    num_affected_rows = act_query(execute_data_pass_query)
                    query_execution_time = (time.time() - query_start_time)
                    accumulated_work_time += query_execution_time
                    total_num_affected_rows += num_affected_rows
                    should_sleep_after_chunk = True
                    retry_data_pass = False
                except Exception, err:
                    print_error("Failed chunk: %s" % err)
                    sleep_after_chunk(1)
                    if options.skip_retry_chunk:                 
                        retry_data_pass = False
                        verbose("Will not retry same chunk again")
                    else:
                        should_sleep_after_chunk = True
                        verbose("Retrying same chunk (may lead to infinite loop if problem is inherent to query). Use --skip-retry-chunk to avoid retrying")
            time_now = time.time()
            elapsed_seconds = round(time_now - start_time, 1)
    
            if (query_comment):
                verbose("+ Query comment: %s" % query_comment)
            verbose("+ Rows: %d affected, %d accumulating; seconds: %s elapsed; %s executed" % (num_affected_rows, total_num_affected_rows, elapsed_seconds, round(accumulated_work_time, 2)))
            if num_affected_rows == 0 and options.terminate_on_not_found:
                verbose("+ Will now terminate due to unfound rows")
                break;
    
            set_unique_key_next_range_start()
    
            if should_sleep_after_chunk:
                sleep_after_chunk(query_execution_time)
        except KeyboardInterrupt:
            # Catch a Ctrl-C. We still want to cleanly close connections
            verbose("User interrupt")
            break
    verbose("%s range complete. Affected rows: %s" % (description, total_num_affected_rows))


def chunk_update():
    """
    Define the chunking queries, work out the chunks
    """
    if not range_exists:
        return

    if options.no_log_bin:
        query = "SET SESSION SQL_LOG_BIN=0"
        act_query(query)

    # We generate two queries:
    # one for first round (includes range start value, or >=),
    # oen for all the rest (skips range start, or >)
    between_statements = ["""
            (%s
        AND
            %s)
        """ % (
            get_multiple_columns_non_equality_comparison_by_names(fully_qualified_unique_key_column_names, get_unique_key_range_start_variables(), ">", first_round),
            get_multiple_columns_non_equality_comparison_by_names(fully_qualified_unique_key_column_names, get_unique_key_range_end_variables(), "<", True)
        ) for first_round in [True, False]]
    if options.chunk_size > 0:
        first_data_pass_query = "%s %s %s" % (options.execute_query[:match.start()], between_statements[0], options.execute_query[match.end():])
        rest_data_pass_query = "%s %s %s" % (options.execute_query[:match.start()], between_statements[1], options.execute_query[match.end():])
    else:
        first_data_pass_query = "%s %s %s" % (options.execute_query[:match.start()], "1", options.execute_query[match.end():])
        rest_data_pass_query = None
        verbose("chunk size is zero; Will only execute: %s" % first_data_pass_query)
    act_data_pass(first_data_pass_query, rest_data_pass_query, "Performing chunks")


def exit_with_error(error_message):
    """
    Notify and exit.
    """
    print_error(error_message)
    sys.exit(1)


try:
    try:
        conn = None
        reuse_conn = True
        (options, args) = parse_options()

        if options.chunk_size < 0:
            exit_with_error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")

        if not options.execute_query:
            exit_with_error("Query to execute must be provided via -e or --execute")

        match_regexp = "OAK_CHUNK[\\s]*\((.*?)\)"
        match = re.search(match_regexp, options.execute_query)
        if not match:
            exit_with_error("Query must include the following token: 'OAK_CHUNK(table_name)', where table_name should be replaced with a table which consists of an AUTO_INCREMENT column by which chunks are made.")

        comment_regexp = "/\*(.*?)\*/"
        query_comment_match = re.search(comment_regexp, options.execute_query)
        query_comment = None
        if query_comment_match:
            query_comment = query_comment_match.group(1).strip()
            
        table_name_match = match.group(1).strip()

        database_name = None
        table_name =  None

        if options.database:
            database_name=options.database

        table_tokens = table_name_match.split(".")
        table_name = table_tokens[-1]
        if len(table_tokens) == 2:
            database_name = table_tokens[0]

        if not database_name:
            exit_with_error("No database specified. Specify with fully qualified table name insode OAK_CHUNK(...) or with -d or --database")

        conn = open_connection()

        if not table_exists(table_name):
            exit_with_error("Table %s.%s does not exist" % (database_name, table_name))

        unique_key_column_names, count_columns_in_unique_key, unique_key_type = get_selected_unique_key_column_names(table_name)
        if not unique_key_column_names:
            exit_with_error("Table must have a UNIQUE KEY on a single column")
        unique_key_column_names_list = unique_key_column_names.split(",")
        fully_qualified_unique_key_column_names = ",".join(["%s.%s" % (table_name, column_name) for column_name in unique_key_column_names_list])

        if not options.skip_lock_tables:
            lock_table_read()
        unique_key_min_values, unique_key_max_values, range_exists = get_unique_key_range()
        unlock_table()

        chunk_update()

        verbose("Chunk update completed")
    except Exception, err:
        if options.debug:
            traceback.print_exc()
        print err
finally:
    if conn:
        conn.close()
