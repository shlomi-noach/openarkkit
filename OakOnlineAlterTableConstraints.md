# Introduction #

This page describes the constraints issues with oak-online-alter-table.


# Details #

To alter a table online, the utility creates a 'ghost' table, then synchronizes it with the original table, and lastly makes a swap.
We will answer the following question:
How are the constraints for UNIQUE KEYs maintained?

The steps are described here, and will be discussed below.

Let's assume the original table is called T. We call our ghost table G(T).
To discuss the UNIQUE issues, let's assume T is defiend as follows:
```
CREATE TABLE T (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ...
  u <some_data_type>,
  ...
  UNIQUE KEY(u),
  ...
);
```
So the 'u' column will have unique values. The above example presents a single-column key, though compound keys are also supported.
Our discussing will apply when there are still more unique keys on T, including compound keys.

The making of G(T) is divided in concept to two parts:
  1. Add triggers on T which 'replicate' into G(T)
  1. deal with rows on T which existed prior to declaring the triggers.

Here are the taken steps. We will discuss the constraint issues for each step.

  1. Verify table T exists
  1. Remove possible 'AFTER' triggers create by this utility (in case of crash or cancel during previous run). The utility creates the triggers with known names, highly unlikely to be used by anyone.
  1. Verify T has UNIQUE KEY(s)
  1. Verify T has no 'AFTER' triggers
  1. Look for G(T), in case it was left there by prior execution of this utility and was not cleaned up (crash, cancel)
  1. Create the ghost table G(T). This is a canvas table, on which changes are made. It will later replace the original table T. The real name chosen for this table is an unlikely one.
  1. Execute an ALTER TABLE on G(T) as specified. Any errors in the ALTER itself are detected here.
  1. Verify G(T) has UNIQUE KEY(s)
    * No issues, assuming T's schema was not altered during this time.s
  1. Verify T and G(T) share at least one UNIQUE KEY
  1. Choose a UNIQUE KEY by which to chunk
  1. Create an AFTER DELETE trigger on T, which deletes corresponding rows from G(T). Since no rows exist on G(T) at this time, nothing is really deleted on G(T).
> > Nothing will happen on G(T) since there are no rows to delete on G(T).
  1. Create an AFTER UPDATE trigger on T, which uses REPLACE INTO G(T) with row values. Since no rows exist on G(T) at this time, nothing is really changed on G(T).
> > Nothing will happen on G(T) since there are no rows to update on G(T).
  1. Create an AFTER INSERT trigger on T, which uses REPLACE INTO G(T) with row values.
    * Rows will now start to add on G(T).
    * The UNIQUE KEY values, including AUTO\_INCREMENT values are copied from T as they were.
    * Since the trigger acts AFTER INSERT, only rows which were admitted in T get to be inserted to G(T).
> > > By virtue of constraints on T, there should be no issues on G(T).
    * INSERTs, UPDATEs, DELETEs are now running against T, and hence on G(T).
    * Any INSERT on T is converted to REPLACE INTO on G(T).
> > > Assume the following scenario:
      * Row 1000 is INSERTED, with u=5
      * Row 1000 is DELETEd
      * Row 1002 is INSERTED, with u=5
> > > These commands are propagated to G(T) as they were on T.
> > > Since less rows appear on G(T) than on T at this stage, we cannot fail a constraint on G(T).
  1. LOCK TABLES T and G(T) with WRITE lock
  1. Get a snapshot on the chosen UNIQUE KEY's MIN and MAX values. The MIN & MAX values are named the "pass range"
    * Since we remove rows, we pose no UNIQUE contradictions.
  1. UNLOCK the two tables.
    * Any UPDATE on T is converted to REPLACE INTO on G(T).
> > > In part, this will ensure no constraints are broken later on.
> > > Any UPDATE which succeeds of T, **must** succeed on G(T). REPLACE INTO will always succeed.
    * Any DELETE on T is passed on to G(T).
    * It is possible that rows from T are being DELETEd, which do not exist on G(T):
> > > any row in the **pass range** meets this scenario.
    * Since we use REPLACE INTO for UPDATEs, it is possible that a row in the **pass range**
> > > has been updated - and so created on G(T), but has then been deleted. So some rows
> > > from the **pass range** could already exist on G(T).
  1. Iterate through the pass range in chunks. For each chunk, copy rows from T to G(T) using INSERT IGNORE. Optionally sleep after each chunk is copied

> > Assume the following scenario:
      * Row 1000 is INSERTED, with u=5
      * We start running the utility. AFTER DELETE, AFTER UPDATE and AFTER INSERT triggers are created.
> > > > Tables are LOCKED, tables are UNLOCKED.
      * Row 1000 is DELETEd
      * Since no suck row exists on G(T), nothing is DELETEd from G(T).
      * The "copy pass" starts. It copies row 1000 to G(T) using INSERT IGNORE.
      * Row 1002 is INSERTED, with u=5
      * On T there is no constraint violation, since row 1000 was previously removed.
      * On G(T) there will be no constraint violation, since a REPLACE INTO is issued,
> > > > removing row 1000 from G(T).
  1. Iterate through the pass range in chunks. For each chunk, delete rows from G(T) in the current chunk range, which do not appear (according to chosen UNIQUE KEY values) in T, for that same chunk range.
    * Since we remove rows, we do not pose constraints violations.
  1. Rename T to OLD(T), G(T) to T
  1. Drop OLD(T)
