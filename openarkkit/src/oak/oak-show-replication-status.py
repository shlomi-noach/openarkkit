#!/usr/bin/python

#
# Show master status and slaves status, when run on master.
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

def parse_options():
    parser = OptionParser()
    parser.add_option("-u", "--user", dest="user", default="", help="MySQL user. Assumed to be the same for master and slaves.")
    parser.add_option("-H", "--host", dest="host", default="localhost", help="MySQL master host")
    parser.add_option("-p", "--password", dest="password", default="", help="MySQL password. Assumed to be the same for master and slaves.")
    parser.add_option("--ask-pass", action="store_true", dest="prompt_password", help="Prompt for password")
    parser.add_option("-P", "--port", dest="port", type="int", default="3306", help="MySQL master TCP/IP port (default: 3306)")
    parser.add_option("-S", "--socket", dest="socket", default="/var/run/mysqld/mysql.sock", help="MySQL socket file. Only applies when master host is localhost")
    parser.add_option("", "--defaults-file", dest="defaults_file", default="", help="Read from MySQL configuration file. Overrides all other options")
    parser.add_option("-n", "--expect-num-slaves", dest="expect_num_slaves", type="int", default="0", help="Number of slaves to expect (default: 0)")
    parser.add_option("-d", "--normal-delay", dest="normal_delay", type="int", default=0, help="Acceptable seconds behind master for slaves")
    parser.add_option("", "--skip-show-slave-hosts", action="store_true", dest="skip_show_slave_hosts", help="Do not use SHOW SLAVE HOSTS to find slaves")
    return parser.parse_args()

def verbose(message):
    print "-- %s" % message

def print_error(message):
    print "-- ERROR: %s" % message

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
            found_slave_hosts_and_ports = [(row["Host"].split(":")[0], port_number,) for row in result_set if row["Command"].strip().lower() in ("binlog dump", "table dump")]
        finally:
            if cursor:
                cursor.close()
    return found_slave_hosts_and_ports


def show_slaves_master_log_files():
    """
    Get the list of master logs reported by all slaves (one master logs per found slave)
    """
    verbose("Slave host\tSlave port\tMaster_Log_File\tSeconds_Behind_Master\tStatus")
    for (slave_host, slave_port,) in slave_hosts_and_ports:
        slave_connection = None
        try:
            try:
                slave_connection = MySQLdb.connect(host = slave_host, user = username, passwd = password, port = slave_port)
                slave_cursor = slave_connection.cursor(MySQLdb.cursors.DictCursor)
                slave_cursor.execute("SHOW SLAVE STATUS")
                slave_status = slave_cursor.fetchone()
                slave_master_log_file = slave_status["Master_Log_File"]
                seconds_behind_master = int(slave_status["Seconds_Behind_Master"])
                slave_cursor.close()
                if seconds_behind_master <= options.normal_delay:
                    status = "Good"
                elif slave_master_log_file == current_master_log:
                    status = "Lag"
                else:
                    status = "Far behind"
                verbose("%s\t%d\t%s\t%s\t%s" % (slave_host, slave_port, slave_master_log_file, seconds_behind_master, status))
            except:
                print_error("Cannot SHOW SLAVE STATUS on %s:%d" % (slave_host, slave_port,))
        finally:
            if slave_connection:
                slave_connection.close()

try:
    try:
        master_connection = None
        (options, args) = parse_options()
        master_connection, username, password, port_number = open_master_connection()

        master_logs = get_master_logs()
        current_master_log = master_logs[-1]

        verbose("master log: %s" % current_master_log)

        slave_hosts_and_ports = get_slave_hosts_and_ports()
        if len(slave_hosts_and_ports) < options.expect_num_slaves:
            print_error("Expected: %d slaves. Found: %d" % (options.expect_num_slaves, len(slave_hosts_and_ports)))
        show_slaves_master_log_files()

    except Exception, err:
        print err
finally:
    if master_connection:
        master_connection.close()
