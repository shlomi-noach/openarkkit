# Introduction #

This page describes the steps taken in order to implement an online ALTER TABLE.

# Details #
oak-online-alter-table requires the following:
  * The table must have at least one UNIQUE KEY (may expand one or more columns)
  * The altered table must share a UNIQUE KEY with the original one
  * No 'AFTER' triggers may be defined on the table
  * Foreign keys currently not supported
  * Table name must be at most 57 characters long

oak-online-alter-table works by creating a 'ghost' table, on which the ALTER is performed, and which is synchronized online with the original table.

Assuming original table name is 'T', and 'ghost' table name is 'G(T)',

The steps are as follows:
  1. Verify table T exists
  1. Remove possible 'AFTER' triggers created by this utility (in case of crash or cancel during previous run). The utility creates the triggers with known names, highly unlikely to be used by anyone.
  1. Verify T has no 'AFTER' triggers
  1. Verify T has UNIQUE KEY(s)
  1. Look for G(T), in case it was left there by prior execution of this utility and was not cleaned up (crash, cancel)
  1. Create the ghost table G(T). This is a canvas table, on which changes are made. It will later replace the original table T. The real name chosen for this table is an unlikely one.
  1. Execute an ALTER TABLE on G(T) as specified. Any errors in the ALTER itself are detected here.
  1. Verify G(T) has UNIQUE KEY(s)
  1. Verify T and G(T) share at least one UNIQUE KEY
  1. Choose a UNIQUE KEY by which to chunk
  1. Create an AFTER DELETE trigger on T, which deletes corresponding rows from G(T). Since no rows exist on G(T) at this time, nothing is really deleted on G(T).
  1. Create an AFTER UPDATE trigger on T, which uses REPLACE INTO G(T) with row values. Since no rows exist on G(T) at this time, nothing is really changed on G(T).
  1. Create an AFTER INSERT trigger on T, which uses REPLACE INTO G(T) with row values.
  1. LOCK TABLES T and G(T) with WRITE lock
  1. Get a snapshot on the chosen UNIQUE KEY's MIN and MAX values. The MIN & MAX values are named the "pass range"
  1. UNLOCK the two tables.
  1. Iterate through the pass range in chunks. For each chunk, copy rows from T to G(T) using INSERT IGNORE. Optionally sleep after each chunk is copied
  1. Iterate through the pass range in chunks. For each chunk, delete rows from G(T) in the current chunk range, which do not appear (according to chosen UNIQUE KEY values) in T, for that same chunk range.
  1. Rename T to OLD(T), G(T) to T
  1. Drop OLD(T)

