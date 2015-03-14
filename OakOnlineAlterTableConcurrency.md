# Introduction #

This page describes the concurrency issues with oak-online-alter-table.
  * See [this page](http://code.google.com/p/openarkkit/wiki/OakOnlineAlterTableSteps) for the general steps taken in oak-online-alter-table.
  * See [this page](http://code.google.com/p/openarkkit/wiki/OakOnlineAlterTableConstraints) for issues with constraints (UNIQUE KEYs) and how they are handled.


# Details #

To alter a table online, the utility creates a 'ghost' table, then synchronizes it with the original table, and lastly makes a swap.
Listed below are concurrency questions:

  * How do we avoid long table locks in MyISAM?
  * How do we avoid long row locks in InnoDB?
  * How do we ensure ghost table is indeed in sync with the original table?
  * How do we handle InnoDB in REPEATABLE\_READ isolation level?

The steps are described here, and will be discussed below.

Let's assume the original table is called T. We call our ghost table G(T).

The making of G(T) is divided in concept to two parts:
  1. Add triggers on T which 'replicate' into G(T)
  1. deal with rows on T which existed prior to declaring the triggers.

Here are the taken steps. We will discuss the concurrency issues for each step.

  1. Verify table T exists
    * No issues here. If T doesn't exist, we quit.
  1. Remove possible 'AFTER' triggers create by this utility (in case of crash or cancel during previous run). The utility creates the triggers with known names, highly unlikely to be used by anyone.
    * No issues here. It is assumed no one will define triggers named the same way this
> > > utility names them.
  1. Verify T has single-column UNIQUE KEY(s)
    * No issues here. If no single-column UNIQUE KEYs exist - we quit.
  1. Verify T has no 'AFTER' triggers
    * If this test passes, we move on. It is possible that someone adds triggers just
> > > after this test - but the utility will not check for that. It should be generally
> > > accepted that when runnign this utility, no one changes the table's schema
> > > (otherwise why run this utility?) or add triggers to the table.s
  1. Look for G(T), in case it was left there by prior execution of this utility and was not cleaned up (crash, cancel)
    * No issues, it is assumed no one will name a table like this utility does.
  1. Create the ghost table G(T). This is a canvas table, on which changes are made. It will later replace the original table T. The real name chosen for this table is an unlikely one.
    * Using "CREATE TABLE ... LIKE ...". There is a lock for just an instant. No issues here.
  1. Execute an ALTER TABLE on G(T) as specified. Any errors in the ALTER itself are detected here.
    * Since no one is using G(T), there's no concurrency issues here.
  1. Verify T has single-column UNIQUE KEY(s)
    * No issues here. If no single-column UNIQUE KEYs exist - we quit.
  1. Verify T and G(T) share at least one single-column UNIQUE KEY
    * Still no issues. We quit if no shared UNIQUE KEY columns exist
  1. Choose a UNIQUE KEY by which to chunk
  1. Create an AFTER DELETE trigger on T, which deletes corresponding rows from G(T). Since no rows exist on G(T) at this time, nothing is really deleted on G(T).
    * The CREATE TRIGEGR statement puts a very short LOCK on T.
> > > At any case, nothing will happen on G(T) since there are no rows to delete on G(T).
  1. Create an AFTER UPDATE trigger on T, which uses REPLACE INTO G(T) with row values. Since no rows exist on G(T) at this time, nothing is really changed on G(T).
    * The CREATE TRIGEGR statement puts a very short LOCK on T.
> > > At any case, nothing will happen on G(T) since there are no rows to update on G(T).
  1. Create an AFTER INSERT trigger on T, which uses REPLACE INTO G(T) with row values.
    * The CREATE TRIGEGR statement puts a very short LOCK on T.
    * Rows will now start to add on G(T).
    * An interesting issue is: how will the trigger affect INSERT statements which were pending during its creation?
> > > On MyISAM - we get back to the "table level lock" answer: since everything happens
> > > with table lock, there is no concurrency between the CREATE TRIGGER and INSERT commands.
    * On InnoDB, an INSERT which started just before the CREATE TRIGGER, will NOT be handled by the trigger.
    * So we're not too sure about rows which have been inserted during the CREATE statement.
    * In addition, immediate consequent DELETE statements may already remove such rows.
> > > Have those DELETE statements been invoked before the CREATE TRIGGER...AFTER DELETE?
    * So, at this point in time - we really don't know anything!

  * INSERTs, UPDATEs, DELETEs are now running against T, and hence on G(T).
  * Any INSERT on T is converted to REPLACE INTO on G(T).
  * Any UPDATE on T is converted to REPLACE INTO on G(T).
  * It is possible that rows in T are being UPDATEd, which do not exist on G(T):

> > any row in the **pass range** meets this scenario.
  * Any DELETE on T is passed on to G(T).
  * It is possible that rows from T are being DELETEd, which do not exist on G(T):
> > any row in the **pass range** meets this scenario.
  1. LOCK TABLES T and G(T) with WRITE lock
    * On both MyISAM & InnoDB - we wait for all writes to conclude.
    * This statement may take time.
    * When the statement completes, we are now the exclusive owners of the two tables.
    * Any INSERT/UPDATE/DELETE invoked just **after** this stage will suspend and till we unlock the tables.
    * We now have a frozen time.
  1. Get a snapshot on the chosen UNIQUE KEY's MIN and MAX values. The MIN & MAX values are named the "pass range"
    * The "pass range" denotes the chosen UNIQUE KEY values range, present in the table at 12:00.
    * Some of the rows in the range may be later DELETEd.
    * Some of the rows in the range may be later UPDATEd.
    * By virtue of our requirements, no new rows will be INSERTed in this range.
> > > If our requirements are not met - the behaviour of this tool is undefined.
  1. UNLOCK the two tables.
    * INSERTs, UPDATEs, DELETEs will now run again. See discussion for "Create trigger AFTER INSERT".
  1. Iterate through the pass range in chunks. For each chunk, copy rows from T to G(T) using INSERT IGNORE. Optionally sleep after each chunk is copied
    * Assuming chunk size of 1000, we read first 1000 chosen UNIQUE KEY values from T in the **pass range**,
    * We copy rows from T to G(T) using
> > > INSERT IGNORE INTO G(T) ... SELECT ... FROM T ... WHERE chosen\_unique\_key\_column IN (..., read values,...),
> > > for rows with chosen UNIQUE KEY values from the above reading.
    * What happens if rows are DELETEd from the chunk's range after we've read their chosen UNIQUE KEY values?
> > > Nothing bad happens, since in the "copy" stage these rows simply do not get copied.
    * How about DELETEs which happen **during** the copy?
      * In MyISAM, this is impossible, since the SELECT adds a READ LOCK on T.
      * For InnoDB tables, we add LOCK IN SHARE MODE to the SELECT. This means the chunk's rows are locked during that time.
> > > > DELETEs which were already pending when the INSERT IGNORE ... SELECT started will
> > > > cause the INSERT to block, since InnoDB adds locks on DELETEd rows. Our
> > > > LOCK IN SHARE mode will make sure we block.
    * What happens if rows are UPDATEd from the chunk's range after we've read their chosen UNIQUE KEY values?

> > > Nothing bad happens, since in the "copy" stage these updates simply get to be copied.
    * How about UPDATEs which happen **during** the copy?
> > > Same as in DELETE. See above.
    * It is possible, then, that just after the chunk is copied, it is already of date as rows from that chunk have been DELETEd.
    * Any UPDATE which occurs on this chunk just after the chunk is copied, get, due to the AFTER UPDATE trigger, to be applied on G(T).
    * We repeat the step for more chunks, until we reach the MAX value of the **pass range**.
    * When iteration is complete and all chunks have been copied, we know that:
      * Any row in T which is in the **pass range**, also appears in G(T) (as results of copying the chunks)
      * Any row in T which is **later** than the **pass range**, also appears in G(T) (as result of the AFTER INSERT trigger)
      * A row which appears in T appears in the exact same way in G(T) (UPDATEs are propagarted correctly)
      * There may be rows in G(T) which have been deleted from T.
  1. Iterate through the pass range in chunks. For each chunk, delete rows from G(T) in the current chunk range, which do not appear (according to chosen UNIQUE KEY values) in T, for that same chunk range.
    * This part fixes the DELETE issue from above. It removes, per chunk, rows from G(T) which are no longer in T.
  1. Rename T to OLD(T), G(T) to T
    * This puts a WRITE LOCK on both tables.
    * Still need to be checked:
      * can it be the the LOCK is gained after a row is inserted to T but befroe it is
> > > INSERTed to G(T) by virtue of the trigger? If so - this can break consistency.
> > > Otherwise, the two tables are consistent and are identical.
  1. Drop OLD(T)
    * Since no one will use OLD(T), we can safely drop it. No concurrency issues here.