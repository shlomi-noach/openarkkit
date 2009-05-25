#!/usr/bin/python

#
# Purge master logs using replication status and hints.
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

import sys
import getpass
import MySQLdb
from optparse import OptionParser
import ConfigParser
import os.path

def parse_options():
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user. Assumed to be the same for master and slaves.")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL master host")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password. Assumed to be the same for master and slaves.")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default=3306, help="MySQL master TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when master host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("--pro-master", action="store_true", dest="pro_master", help="Pro-master")
    parser.add_option("--pro-slaves", action="store_true", dest="pro_slaves", help="Pro-slaves")
    parser.add_option("-r", "--retain-logs", dest="retain_logs", type="int", default=5, help="Number of logs to retain on master (default: 5)")
    parser.add_option("-n", "--expect-num-slaves", dest="expect_num_slaves", type="int", default="-1", help="Number of slaves to expect (default: -1 = No expectation)")
    parser.add_option("", "--skip-show-slave-hosts", action="store_true", dest="skip_show_slave_hosts", help="Do not use SHOW SLAVE HOSTS to find slaves")
    parser.add_option("-f", "--flush-logs", action="store_true", dest="flush_logs", help="Perform FLUSH LOGS on startup")
    parser.add_option("--sentinel", dest="sentinel", default="/tmp/oak-purge-master-logs.sentinel", help="Sentinel file: exit if the file exists")
    parser.add_option("--print-only", action="store_true", dest="print_only", help="Do not execute. Only print statement")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Print user friendly messages")
    return parser.parse_args()

def verbose(message):
    if options.verbose:
        print "-- %s" % message

def print_error(message):
    print "-- ERROR: %s" % message


def act_final_query(query, verbose_message):
    """
    Either print or execute the given query
    """
    if options.print_only:
        print query
    else:
        update_cursor = master_connection.cursor()
        try:
            try:
                update_cursor.execute(query)
                verbose(verbose_message)
            except:
                print_error("error executing: %s" % query)
        finally:
            update_cursor.close()


def open_master_connection():
    """
    Open a connection on the master
    """
    if options.defaults_file:
        conn = MySQLdb.connect(read_default_file = options.defaults_file)
        config = ConfigParser.ConfigParser()
        try:
            config.read(options.defaults_file)
        except:
            pass
        username = config.get('client','user')
        password = config.get('client','password')
        port_number = int(config.get('client','port'))
    else:
        username = options.user
        port_number = options.port
        if options.prompt_password:
            password=getpass.getpass()
        else:
            password=options.password
        conn = MySQLdb.connect(
            host = options.host,
            user = username,
            passwd = password,
            port = options.port,
            unix_socket = options.socket)
    return conn, username, password, port_number

def get_master_logs():
    """
    Get the list of available binary logs on the master
    """
    cursor = None;
    try:
        cursor = master_connection.cursor()
        cursor.execute("SHOW MASTER LOGS")
        master_logs = []
        result_set = cursor.fetchall()
        master_logs = [row[0] for row in result_set]
    finally:
        if cursor:
            cursor.close()
    return master_logs


def get_server_id():
    """
    Returns the master's server id (to be later compared with listings from SHOW SLAVE HOSTS)
    """
    server_id = None
    cursor = None;
    try:
        cursor = master_connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SHOW GLOBAL VARIABLES LIKE 'server_id'")
        row = cursor.fetchone()
        server_id = int(row["Value"])
    finally:
        if cursor:
            cursor.close()
    return server_id


def get_slave_hosts_and_ports():
    """
    Return the list of slave hosts reported by SHOW SLAVE HOSTS
    """
    found_slave_hosts_and_ports = []
    if options.expect_num_slaves:
        cursor = None;
        if not options.skip_show_slave_hosts:
            try:
                server_id = get_server_id()
                cursor = master_connection.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute("SHOW SLAVE HOSTS")
                result_set = cursor.fetchall()
                # Get host,port for those slaves replicating this master
                found_slave_hosts_and_ports = [(row["Host"], int(row["Port"]),) for row in result_set if int(row["Master_id"]) == server_id]
            finally:
                if cursor:
                    cursor.close()
        if not found_slave_hosts_and_ports:
            # Couldn't get explicit hosts. Then we'll try to figure them out by SHOW PROCESSLIST.
            # This is less preferable, since the SUPER privilege will be required.
            try:
                cursor = master_connection.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute("SHOW PROCESSLIST")
                result_set = cursor.fetchall()
                # We assume slave port to be same as options.port_number
                found_slave_hosts_and_ports = [(row["Host"].split(":")[0], port_number,) for row in result_set if row["Command"].strip().lower() in ("binlog dump", "table dump")]
            finally:
                if cursor:
                    cursor.close()
    return found_slave_hosts_and_ports


def get_slaves_master_log_files():
    """
    Get the list of master logs reported by all slaves (one master logs per found slave)
    """
    slaves_master_log_files = []
    for (slave_host, slave_port,) in slave_hosts_and_ports:
        slave_connection = None
        try:
            try:
                slave_connection = MySQLdb.connect(host = slave_host, user = username, passwd = password, port = slave_port)
                verbose("-+ Connected to slave: %s:%d" % (slave_host, slave_port,))
                slave_cursor = slave_connection.cursor(MySQLdb.cursors.DictCursor)
                slave_cursor.execute("SHOW SLAVE STATUS")
                slave_status = slave_cursor.fetchone()
                slave_master_log_file = slave_status["Master_Log_File"]
                slaves_master_log_files.append(slave_master_log_file)
                slave_cursor.close()
                verbose(" + Slave: %s:%d, replicates master file: %s" % (slave_host, slave_port, slave_master_log_file))
            except Exception, err:
                print_error("Cannot SHOW SLAVE STATUS on %s" % slave_host)
        finally:
            if slave_connection:
                slave_connection.close()
    slaves_master_log_files.sort()
    return slaves_master_log_files

def purge_master_logs_to(master_log_file):
    """
    Execute the PARGE MASTER LOGS TO statement, given a log name
    """
    query = "PURGE MASTER LOGS TO '%s'" % master_log_file
    if(options.print_only):
        print query
        return

    verbose("Will purge master logs to %s" % desired_master_logs[0])
    purge_cursor = None
    try:
        try:
            purge_cursor = master_connection.cursor()
            purge_cursor.execute(query)
            verbose("Successfuly purged master logs to %s" % master_log_file)
        except:
            print_error("Failed purging master logs to %s" % master_log_file)
    finally:
        if purge_cursor:
            purge_cursor.close()

def purge_master_logs_as_desired():
    """
    Purge master logs up to options.retain_logs. This is the desired behavior.
    """
    purge_master_logs_to(desired_master_logs[0])

def purge_master_logs_on_delaying_slaves():
    """
    Purge master logs when all slaves are somewhere within the master logs,
    but not all slaves are in the desired master logs.
    """
    if options.pro_master and options.pro_slaves:
        master_log_to_purge_to = min(desired_master_logs[0], max_slave_master_log_file)
        purge_master_logs_to(master_log_to_purge_to)
    elif options.pro_master and not options.pro_slaves:
        purge_master_logs_to(desired_master_logs[0])
    else:
        # Not pro-master
        # Play safe: the only safe thing to do is to purge to the min_slave_master_log_file.
        purge_master_logs_to(min_slave_master_log_file)
        if options.pro_slaves:
            pass
        else:
            # No specific instruction given. So this should be reported.
            print_error("Not all slaves are in sync with last %d master logs.\nHave only purged up to %s. Specify --pro-master, --pro-slaves or both and rerun." % (options.retain_logs, min_slave_master_log_file))

def purge_master_logs_on_missing_slaves():
    """
    Purge master logs when some slaves are missing from the list (their whereabouts unknown)
    """
    if options.pro_master:
        if options.pro_slaves:
            master_log_to_purge_to = min(desired_master_logs[0], max_slave_master_log_file)
            purge_master_logs_to(master_log_to_purge_to)
        else:
            purge_master_logs_to(desired_master_logs[0])
    else:
        # Not pro-master
        print_error("Some slaves are missing. Have not purged anything. Specify --pro-master (possibly with --pro-slaves) to force purging.")

def handle_purging_logic():
    # handle crisis:
    if slaves_are_missing and not slaves_master_log_files:
        # All unknown: none of the expected slaves can be found!
        print_error("No slaves can be detected")
        if options.pro_master and not options.pro_slaves:
            purge_master_logs_as_desired()
        else:
            print_error("No action taken. Force purging with --pro-master, and without --pro-slaves")
        return

    if slaves_are_missing:
        verbose("Expected %s slaves. Found %s." % (options.expect_num_slaves, len(slaves_master_log_files)))
        purge_master_logs_on_missing_slaves();
        return

    if not slaves_master_log_files:
        verbose("No slaves can be detected, yet not considered as missing")
        purge_master_logs_as_desired()
        return

    verbose("All slaves are currently in sync with master")
    if min_slave_master_log_file in desired_master_logs:
        # Excellent
        verbose("All slaves are currently in sync with recent logs")
        purge_master_logs_as_desired()
    else:
        # Slight problem here: all slaves are in the master's logs list,
        # but not all slaves appear in the desired master logs list.
        purge_master_logs_on_delaying_slaves()


try:
    try:
        master_connection = None
        (options, args) = parse_options()
        if os.path.exists(options.sentinel):
            verbose("Sentinel file: %s found. Quitting" % options.sentinel)
        else:
            master_connection, username, password, port_number = open_master_connection()

            if options.flush_logs:
                act_final_query("FLUSH LOGS", "Logs have been flushed")
            master_logs = get_master_logs()

            verbose("Current master logs: %s" % master_logs)
            verbose("Current master log file: %s" % master_logs[-1])

            if len(master_logs) <= options.retain_logs:
                verbose("There are %s log files on the master host, no more than the %s configured retain-logs. Will do nothing" % (len(master_logs), options.retain_logs))
            else:
                desired_master_logs = master_logs[-options.retain_logs:]

                slave_hosts_and_ports = get_slave_hosts_and_ports()
                verbose("Slave hosts: %s" % ["%s:%d" % (slave_host, slave_port,) for (slave_host, slave_port) in slave_hosts_and_ports])
                # SHOW SLAVE HOSTS shows slaves from the entire topology. We wish to exclude slaves
                # which replicate master logs not in the current master.
                slaves_master_log_files = get_slaves_master_log_files()
                slaves_master_log_files = [ slave_master_log_file for slave_master_log_file in slaves_master_log_files if slave_master_log_file in master_logs]

                if slaves_master_log_files:
                    min_slave_master_log_file = slaves_master_log_files[0]
                    max_slave_master_log_file = slaves_master_log_files[-1]
                    verbose("Slaves' master log files: %s" % slaves_master_log_files)
                slaves_are_missing = (options.expect_num_slaves >= 0) and (len(slaves_master_log_files) < options.expect_num_slaves)

                handle_purging_logic()

    except Exception, err:
        print err
finally:
    if master_connection:
        master_connection.close()
