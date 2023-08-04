# Setup Microsoft SQL Server Change Data Capture

## Why Log Based replication

Log based replication has a number of advantages over incremental key and full table replication.

Some of the key features include

1. Smaller batches of data to transfer - you are only shipping the changed data.
2. Incremental key can't detect deletes. If the application / database has deleted records. Then you must do a full table or CDC log based replication.
3. There is no special logic required to detect deletes. Delete records are shipped through when they occur.
4. There is an accurate timestamp available to indicate when records are changed in the database / application.

If you would like to learn more about Change Data Capture, please refer to the Microsoft Documentation below.

https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server

## Background

Microsoft has introduced two technologies for identifying and capturing changes to tables. The first technology introduced was Change Tracking with SQL Server 2005. This inbuilt technology
identifies rows which have changed. This approach has some downsides however as it doesn't capture the actual change data and it is a synchronous operation.

Change Data Capture (CDC) was introduced with SQL Server 2016 and is a free feature if you are licensed with an Enterprise Edition. The benefit of asynchronous processing and the ability to capture a
full image of the record is extremely useful.

## Log_Based Approach

This implication of MSSQL log_based replication approach uses Change Data Capture only. It does not support Change Tracking due to the obvious limitations and performance overheads.

## Primary Key Update

A primary key update is when the value of the primary key is altered. In many application the primary key for a table is an allocated number based on a sequence, so it is not possible to update. In some database schema's it is techniquely possible to update a primary key e.g. change an Employee ID from `joesmith` to `joesmith1`. This is generally frowned upon as it can have undesirable consequences.

It is important to note because of undesirables consequences, when CDC is enabled on a table you cannot update the primary key. This is noted in the Microsoft documentation here https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16 . If the primary key needs to be updated it the Change Data Capture must be disabled on the table.

## Setup Example

The following example works with the MSSQL AdventureWorks2016 database. If you would like to try the example, please download and install the AdventureWorks database from here.

https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure

Feel free to adjust a number of the steps as required (for example more restrictive privileges, database ownership, files, and roles). To access the cdc tables use the CDC user which has privileges
to obtain the CDC data.

```sql

-- Example for On-Premise databases - Tailor the script to your needs
-- ====
-- Enable Database for CDC template
-- ====

USE AdventureWorks2016 ;

EXEC sp_changedbowner 'sa';

EXEC sys.sp_cdc_enable_db;

-- ====
-- Create a new File Group for the CDC tables
-- ====

USE AdventureWorks2016;

ALTER DATABASE AdventureWorks2016 ADD FILEGROUP CDC_FILEGROUP;

-- Add files to the filegroup

ALTER DATABASE AdventureWorks2016 ADD FILE ( NAME = cdc_files, FILENAME = 'C:\MySQLFiles\CDC_FILES' ) to FILEGROUP CDC_FILEGROUP;

-- ====
-- Enable Snapshot Isolation - Highly recommended for read consistency and to avoid dirty commits.
-- ====

-- https://www.sqlservercentral.com/articles/what-when-and-who-auditing-101-part-2
-- Consider running one of these options to ensure read consistency. Do consider the impact however of enabling these features.

ALTER DATABASE AdventureWorks2016
SET READ_COMMITTED_SNAPSHOT ON;

ALTER DATABASE AdventureWorks2016
SET ALLOW_SNAPSHOT_ISOLATION ON;

SELECT DB_NAME(database_id),
    is_read_committed_snapshot_on,
    snapshot_isolation_state_desc
FROM sys.databases
WHERE database_id = DB_ID();

-- ====
-- Create a new Role for accessing the CDC tables
-- ====

USE AdventureWorks2016;

-- Create a new login

CREATE LOGIN CDC_USER WITH PASSWORD = '<cdc_user password>';

ALTER SERVER ROLE sysadmin ADD MEMBER CDC_USER;

-- Create a new user associated with the new login and access to the database

CREATE USER CDC_USER FOR LOGIN CDC_USER;

ALTER USER CDC_USER WITH DEFAULT_SCHEMA=dbo;

-- ====
-- Create a new Role for accessing the CDC tables
-- ====

CREATE ROLE CDC_Role;

ALTER ROLE CDC_Role ADD MEMBER CDC_USER;

-- Add explicit permissions to tracked tables, use this script to generate the required grant permissions

-- ====
-- Add required CDC permissions to the CDC role
-- ====



-- =========
-- Enable CDC Tracking on tables Specifying Filegroup Option Template
-- =========
USE AdventureWorks2016
;

EXEC sys.sp_cdc_enable_table
@source_schema = N'HumanResources',
@source_name   = N'Department',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1
;

EXEC sys.sp_cdc_enable_table
@source_schema = N'HumanResources',
@source_name   = N'Employee',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1
;

EXEC sys.sp_cdc_enable_table
@source_schema = N'Person',
@source_name   = N'Person',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1
;

EXEC sys.sp_cdc_enable_table
@source_schema = N'Person',
@source_name   = N'BusinessEntity',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1
;

-- =========
-- Grant permissions for each base database table (if required)
-- =========
-- GRANT SELECT on each source table to enable CDC table-valued function calls
GRANT SELECT ON [<sourceSchemaName>].[<sourceTableName>] TO CDC_Role;


-- =========
-- Fixing the logging if it was not working
-- =========

-- Stopping the Capture Process

-- Flushing the logs
EXEC sp_repldone @xactid = NULL, @xact_segno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sp_replflush;

-- Start the Capture Process

-- =========
-- CDC Queries to check what is happening
-- =========

-- Check for CDC errors
select *
from sys.dm_cdc_errors;

-- Stop the CDC Capture Job
exec sys.sp_cdc_stop_job;

-- Start the CDC Capture Job
EXEC sys.sp_cdc_start_job;

-- Check that CDC is enabled on the database
SELECT name, is_cdc_enabled
FROM sys.databases WHERE database_id = DB_ID();

-- Check tables which have CDC enabled on them
select s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc, t.object_id
from sys.tables t
join sys.schemas s on (s.schema_id = t.schema_id)
where t.is_tracked_by_cdc = 1
;

```

```sql

-- Example for Azure or AWS RDS databases - Tailor the example script to your needs

/*

Note:
Snapshot Isolation level HASN'T been implemented as part of the the recommendation below as it can cause:
https://www.sqlservercentral.com/articles/row-level-versioning
-Increases resource usage when modifying data since row versions are maintained in tempDB.
-Update and Delete transaction will use more resource since it has to create a snapshot in the tempDB. This could cause higher IO, CPU and Memory usage.
-TempDB must have enough space to handle all the additional requirements.
-14 Bytes will be added to the row in the database to keep track of the versions.
-If there are long version chains then Data Read performance will be affected.

*/


-- ====
-- Enable Database for CDC template
-- ====

USE [<database_name>];

EXEC sp_changedbowner 'sa';
--For AWS RDS dbowner is rdsa so no need to run the above statement.

EXEC sys.sp_cdc_enable_db;
--Equivalent command to above on Azure or AWS RDS as per below:

--AWS RDS:
exec msdb.dbo.rds_cdc_enable_db [<database_name>]
--Otherwise running above command: EXEC sys.sp_cdc_enable_db; on AWS RDS db will give you permission error: "Msg 22902, Level 16, State 1, Procedure sp_cdc_enable_db, Line 21 [Batch Start Line 9]
--Caller is not authorized to initiate the requested action. Sysadmin privileges are required."

--Azure: This is same as normal SQL command above.
sys.sp_cdc_enable_db;

--Query to check if cdc is enabled:
SELECT name, is_cdc_enabled
FROM sys.databases WHERE database_id = DB_ID();

-- ====
-- Create a new File Group for the CDC tables
-- ====

USE [<database_name>];

ALTER DATABASE [<database_name>] ADD FILEGROUP CDC_FILEGROUP;

-- Add files to the filegroup

ALTER DATABASE [<database_name>] ADD FILE ( NAME = cdc_files, FILENAME = 'C:\MySQLFiles\CDC_FILES' )to FILEGROUP CDC_FILEGROUP;

-- ====
-- Enable Snapshot Isolation - Highly recommended for read consistency and to avoid dirty commits.
-- ====

/*
-- https://www.sqlservercentral.com/articles/what-when-and-who-auditing-101-part-2
-- Consider running one of these options to ensure read consistency. Do consider the impact however of enabling these features.

ALTER DATABASE [<database_name>]
SET READ_COMMITTED_SNAPSHOT ON;

ALTER DATABASE [<database_name>]
SET ALLOW_SNAPSHOT_ISOLATION ON;

SELECT DB_NAME(database_id),
    is_read_committed_snapshot_on,
    snapshot_isolation_state_desc
FROM sys.databases
WHERE database_id = DB_ID();

Comments:
Configuring snapshots can also affect performance and have added I/O and CPU overhead:
With enabling Snapshots this would
1.	Increase resource usage when modifying data since row versions are maintained in tempDB.
2.	Update and Delete transaction will use more resource since it has to create a snapshot in the tempDB. This could cause higher IO, CPU and Memory usage.
3.	TempDB must have enough space to handle all the additional requirements.
4.	14 Bytes will be added to the row in the database to keep track of the versions.
5.	If there are long version chains then Data Read performance will be affected.

So I am suggesting/recommending that we leave snapshot isolation settings out of the mix and see if there are any concurrency issues during reads with cdc once that is implemented.


*/


-- ====
-- Create a new Role for accessing the CDC tables
-- ====

USE [<database_name>];

-- Create a new login

CREATE LOGIN CDC_USER WITH PASSWORD = '<EnterPassword>',
DEFAULT_DATABASE=[<database_name>], CHECK_EXPIRATION=OFF, CHECK_POLICY=ON;

--ALTER SERVER ROLE sysadmin ADD MEMBER SA_SQLDProv_CDCUser;
--Above is not required

-- Create a new user associated with the new login and access to the database

CREATE USER CDC_USER FOR LOGIN CDC_USER;

ALTER USER CDC_USER WITH DEFAULT_SCHEMA=dbo;

-- ====
-- Create a new Role for accessing the CDC tables
-- ====

CREATE ROLE CDC_Role;


ALTER ROLE CDC_Role ADD MEMBER CDC_USER;

-- Add explicit permissions to tracked tables, use this script to generate the required grant permissions

-- ====
-- Add required CDC permissions to the CDC role
-- ====
USE [<database_name>]
GO
ALTER ROLE [db_owner] ADD MEMBER [CDC_Role]
GO

--OR
----

USE [<database_name>]
GO
ALTER ROLE [db_datareader] ADD MEMBER [CDC_Role]
GO


USE [<database_name>]
GO

GRANT EXECUTE ON SCHEMA ::cdc TO [CDC_Role]
GO

GRANT VIEW DATABASE STATE TO [CDC_Role]
GO

-- =========
-- Enable CDC Tracking on tables Specifying Filegroup Option Template
-- =========


--https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver15 (for @supports_net_changes description)
--If 1, the functions that are needed to query for net changes are also generated.If supports_net_changes is set to 1, index_name must be specified, or the source table must have a defined primary key.
--Most tables have a primary key so index_name can be ommited.
/*
Table list to enable CDC:
1.	TABLE1
2.	TABLE2
3.	....

IMPORTANT: Please ensure that the user to access the CDC tables e.g. CDC_USER has access to the appropriate CDC Role e.g. CDC_Role. The role specified in the sys.sp_cdc_enable_table
           function must align with the CDC role allocated to the CDC_User.

*/



USE [<database_name>];

--1.	EXAM
=============
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'TABLE1',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1;

--2.	EXAM_CODES
=============
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'TABLE2',
@role_name     = N'CDC_Role',
@filegroup_name = N'CDC_FILEGROUP',
@supports_net_changes = 1;


-- =========
-- Fixing the logging if it was not working
-- =========

-- Stopping the Capture Process

-- Flushing the logs
EXEC sp_repldone @xactid = NULL, @xact_segno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sp_replflush;

-- Start the Capture Process

-- =========
-- CDC Queries to check what is happening
-- =========

-- Check for CDC errors
select *
from sys.dm_cdc_errors;

--To get cdc job information:
select * from msdb.dbo.sysjobs
--Or Use database that has cdc enabled:
Use <database_name>
Exec sys.sp_cdc_help_jobs

-- Stop the CDC Capture Job
exec sys.sp_cdc_stop_job;

-- Start the CDC Capture Job
EXEC sys.sp_cdc_start_job;

-- Check that CDC is enabled on the database
SELECT name, is_cdc_enabled
FROM sys.databases WHERE database_id = DB_ID();

-- Check tables which have CDC enabled on them
select s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc, t.object_id
from sys.tables t
join sys.schemas s on (s.schema_id = t.schema_id)
where t.is_tracked_by_cdc = 1
;

--Get cdc details:
Use <database_name>;
Exec sys.sp_cdc_help_change_data_capture;

--CDC System tables located under <database_name> > System Tables


--BACKOUT PLAN
--============================================
--To disable cdc on indvidual tables:
--The Change Data Capture can easily be disabled on a given table with the help of the sys.sp_cdc_disable_table system stored procedure, as stated below:
EXEC sys.sp_cdc_disable_table
@source_schema = N'dbo',
@source_name   = N'<Table_Name>',
@role_name     = N'CDC_Role',
@capture_instace =N'<Capture_instance>';
--Capture instance name can be found using the following query: Normally Capture_instance name is just Schema_TableName eg: dbo_Patient
Select * from cdc.change_tables

EXEC sys.sp_cdc_disable_table
@source_schema = N'dbo',
@source_name   = N'TABLE1',
@capture_instance =N'dbo_TABLE1';

EXEC sys.sp_cdc_disable_table
@source_schema = N'dbo',
@source_name   = N'TABLE2',
@capture_instance =N'dbo_TABLE2';

--To disable cdc on DB:
--You can disable SQL Server CDC completely at the database level, without the need to disable it on CDC-enabled tables one at a time.
exec msdb.dbo.rds_cdc_disable_db [<database_name>];

--Remove Logins, db_users, roles created above.

--Romove FILEGROUP 'CDC_FILEGROUP'

```


## Code to manage structural changes (DDL) to tables being captured.

If a new columns is added to a base table which has been enrolled for CDC, it will not be automatically propagated to the capture table. If a table is being altered, the CDC capture routines need to be re-issued to ensure all columns are being captured.

This script can be used to re-enrol a table for CDC, ideally immediately after any ALTER TABLE statements have been issued.

This makes use of the fact that a table can have two capture instances enabled on it simultaneously. In this description, the current capture instance will be called `A` and the temporary one called `B`

1. Enable a new capture instance on the table `B`
2. Get a list of columns that are common to both capture instances `A` and `B`
3. Insert the contents of the change table for `A` into `B`
4. Set the `start_lsn` for capture instance `B` to the `start_lsn` for capture instance `A` in `cdc.change_tables` (alternatively this value could be stored in a variable)
5. Disable capture instance `A`
6. Re-enable capture instance `A` - this resets the `start_lsn` in `cdc.change_tables`
7. Insert the contents of the change table for `B` into `A`
8. Set the `start_lsn` for capture instance `A` to the `start_lsn` for capture instance `B` in `cdc.change_tables`
9. Disable capture instance `B`

```sql

/* Re Enrolment of a table in CDC (ideally immediately) after an ALTER TABLE */
/* operation has occurred on the base table */
DECLARE @source_schema varchar(max) = 'dbo'
DECLARE @source_name varchar(max) = 'MyBaseTableName'
DECLARE @cdc_schema varchar(max) = 'cdc'
DECLARE @column_list varchar(max)
DECLARE @sql varchar(max)
DECLARE @capture_instance varchar(max)
SET @capture_instance = @source_schema+'_'+@source_name
DECLARE @ct_table varchar(max)
SET @ct_table = @capture_instance+'_CT'
DECLARE @capture_instance_temp varchar(max)
SET @capture_instance_temp = @capture_instance+'_temp'
DECLARE @ct_table_temp varchar(max)
SET @ct_table_temp = @capture_instance_temp+'_CT'

--Test alterations on the base table here
--SET @sql = 'ALTER TABLE '+@source_schema+'.'+@source_name+' DROP COLUMN TestNewColumn'
--EXEC(@sql)

/* Enrol the table with a temporary capture instance */
EXEC [sys].[sp_cdc_enable_table]
  @source_schema = @source_schema,
  @source_name   = @source_name,
  @role_name     = N'CDCDWHrole',
  @supports_net_changes = 1,
  @filegroup_name = N'CDC_FILEGROUP',
  @capture_instance = @capture_instance_temp

/* Get a list of the column names from both original and temp CDC CT tables */
/* IMPORTANT: For any removed columns that had values changed before the ALTER TABLE */
/* that were not previously synced, these values will be lost during this reenrolment process! */
SELECT @column_list = STUFF(( SELECT  ',' + column_name
      FROM (SELECT ct.column_name
                FROM
                  INFORMATION_SCHEMA.COLUMNS ct
                inner join
                  INFORMATION_SCHEMA.COLUMNS ctt
                  on ct.column_name = ctt.column_name
                where ct.table_name = @ct_table and ct.table_schema = @cdc_schema
                and ctt.table_name = @ct_table_temp and ctt.table_schema = @cdc_schema
              ) x
    FOR
      XML PATH('')
    ), 1, 1, '')

/* At this point the min LSN for the temp table is too high for Meltano to get changes */
/* and too high for the fn_cdc_get_all_changes to work too */
SET @sql = 'INSERT INTO '+@cdc_schema+'.'+@ct_table_temp+
'('+@column_list+') SELECT '+@column_list+
' FROM '+@cdc_schema+'.'+@ct_table

exec(@sql)

/* Artificially tell the temp table that its min LSN is the same as the base CT table */
SET @sql = 'UPDATE cdc.change_tables
SET start_lsn = (
select start_lsn from cdc.change_tables
where capture_instance = '''+@capture_instance+'''
)
where capture_instance = '''+@capture_instance+'_temp'''

exec(@sql)

/* Re-enrol the table */
EXEC [sys].[sp_cdc_disable_table]
      @source_schema = N'dbo',
      @capture_instance = @capture_instance,
      @source_name = @source_name

EXEC [sys].[sp_cdc_enable_table]
  @source_schema = @source_schema,
  @source_name   = @source_name,
  @role_name     = N'CDCDWHrole',
  @supports_net_changes = 1,
  @filegroup_name = N'CDC_FILEGROUP'

SET @sql = 'INSERT INTO '+@cdc_schema+'.'+@ct_table+
'('+@column_list+') SELECT '+@column_list+
' FROM '+@cdc_schema+'.'+@ct_table_temp

exec(@sql)

/* Artificially tell the original CT table that its min LSN is the same as the temp CT table */
/* effectively restoring the start_lsn to before the change */
SET @sql = 'UPDATE cdc.change_tables
SET start_lsn = (
select start_lsn from cdc.change_tables
where capture_instance = '''+@capture_instance+'_temp''
)
where capture_instance = '''+@capture_instance+''''

exec(@sql)

/* Drop the temporary capture instance */
EXEC [sys].[sp_cdc_disable_table]
  @source_schema = @source_schema,
  @source_name   = @source_name,
  @capture_instance = @capture_instance_temp

```