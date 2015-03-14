<font color='#404040'>
<h1></h1>
<h1><font color='#F26522'>openark kit</font></h1>

The openark kit provides common utilities to administer, diagnose and audit MySQL databases.<br>
<br>
Please refer to the <a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/introduction.html'><font color='#F26522'><u>openark kit documentation</u></font></a> for details.<br>
<br>
<br>
The available tools are:<br>
<ul><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-apply-ri.html'><font color='#F26522'><u>oak-apply-ri</u></font></a>: apply referential integrity on two columns with parent-child relationship.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-block-account.html'><font color='#F26522'><u>oak-block-account</u></font></a>: block or release MySQL users accounts, disabling them or enabling them to login.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-chunk-update.html'><font color='#F26522'><u>oak-chunk-update</u></font></a>: perform long, non-blocking UPDATE/DELETE operation in auto managed small chunks.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-get-slave-lag.html'><font color='#F26522'><u>oak-get-slave-lag</u></font></a>: print slave replication lag and terminate with respective exit code.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-hook-general-log.html'><font color='#F26522'><u>oak-hook-general-log</u></font></a>: hook up and filter general log entries based on entry type or execution plan criteria.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-kill-slow-queries.html'><font color='#F26522'><u>oak-kill-slow-queries</u></font></a>: terminate long running queries.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-modify-charset.html'><font color='#F26522'><u>oak-modify-charset</u></font></a>: change the character set (and collation) of a textual column.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-online-alter-table.html'><font color='#F26522'><u>oak-online-alter-table</u></font></a>: perform a non-blocking ALTER TABLE operation.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-prepare-shutdown.html'><font color='#F26522'><u>oak-prepare-shutdown</u></font></a>: make for a fast and safe MySQL shutdown.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-purge-master-logs.html'><font color='#F26522'><u>oak-purge-master-logs</u></font></a>: purge master logs, depending on the state of replicating slaves.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-repeat-query.html'><font color='#F26522'><u>oak-repeat-query</u></font></a>: repeat query execution until some condition holds.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-security-audit.html'><font color='#F26522'><u>oak-security-audit</u></font></a>: audit accounts, passwords, privileges and other security settings.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-show-limits.html'><font color='#F26522'><u>oak-show-limits</u></font></a>: show AUTO_INCREMENT “free space”.<br>
</li><li><a href='http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-show-replication-status.html'><font color='#F26522'><u>oak-show-replication-status</u></font></a>: show how far behind are replicating slaves on a given master.</li></ul>

All tools are coded in Python, require Python 2.3 or newer, and the python-mysqldb driver. Some tools require MySQL 5.0 or higher; see the docs for each tool.<br>
<br>
</font>