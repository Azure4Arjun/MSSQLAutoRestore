
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
Create PROCEDURE [dbo].[DatabaseRestoreTest]
      @Database NVARCHAR(4000), @RestoreDatabaseName NVARCHAR(MAX)= NULL, @BackupPath NVARCHAR(MAX), @MoveFiles NVARCHAR(MAX)= 'N', @MoveDataDrive NVARCHAR(MAX)= NULL, @MoveLogDrive NVARCHAR(MAX)= NULL, @TestRestore NVARCHAR(MAX)= 'N', @RunCheckDB NVARCHAR(MAX)= 'N', @LogToTable NVARCHAR(MAX)= 'N'
AS
     BEGIN
         SET NOCOUNT ON;


         -- 1 - Variable declaration 

         DECLARE @cmd NVARCHAR(MAX);
         DECLARE @cmd2 NVARCHAR(500);
         DECLARE @fileList TABLE
                                (
                                 backupFile NVARCHAR(255)
                                );
         DECLARE @lastFullBackup NVARCHAR(500);
         DECLARE @lastDiffBackup NVARCHAR(500);
         DECLARE @backupFile NVARCHAR(500);
         DECLARE @MoveDataLocation AS NVARCHAR(500);
         DECLARE @MoveDataLocationName AS NVARCHAR(500);
         DECLARE @MoveLogLocation AS NVARCHAR(500);
         DECLARE @MoveLogLocationName AS NVARCHAR(500);

         --Declare @MoveDataDrive AS NVARCHAR(500)
         --Declare @MoveLogDrive as NVARCHAR(500)
         --Declare @TestRestore as bit
         --Declare @RunCheckDB as bit
         --Enable option to move the files
         --SET @MoveFiles = 1
         --SET @dbName = 'Cleo' 
         IF @RestoreDatabaseName IS NULL
             BEGIN
                 SET @RestoreDatabaseName = @database;
             END;
         --SET @backupPath = '\\ash-report-s1\backups\prodcl1-c1$prod-ag2\CLEO\' 
         --SET @MoveDataDrive = 'X:\Data\'
         --SET @MoveLogDrive = 'X:\Logs\'
         --SET @TestRestore = 1
         --SET @RunCheckDB = 1
         -- 2 - Initialize variables 
         --Assume this is a restore from AG
         DECLARE @FullCopyPath AS NVARCHAR(500);
         DECLARE @TlogPath AS NVARCHAR(500);
         SET @FullCopyPath = @BackupPath+'FULL_COPY_ONLY\';
         SET @TlogPath = @BackupPath+'LOG\';

         -- 3 - get list of files 
         SET @cmd2 = 'DIR /b '+@FullCopyPath;
         INSERT INTO @fileList
                              (backupFile
                              )
         EXEC master.sys.xp_cmdshell
              @cmd2; 

         ---select * from @fileList
         -- 4 - Find latest full backup 
         SELECT @lastFullBackup = MAX(backupFile)
         FROM @fileList
         WHERE backupFile LIKE '%.bak'
               AND backupFile LIKE '%'+@database+'%';
         DECLARE @FileListParameters TABLE
                                          (
                                           LogicalName NVARCHAR(128) NOT NULL, PhysicalName NVARCHAR(260) NOT NULL, Type CHAR(1) NOT NULL, FileGroupName NVARCHAR(120) NULL, Size NUMERIC(20, 0) NOT NULL, MaxSize NUMERIC(20, 0) NOT NULL, FileID BIGINT NULL, CreateLSN NUMERIC(25, 0) NULL, DropLSN NUMERIC(25, 0) NULL, UniqueID UNIQUEIDENTIFIER NULL, ReadOnlyLSN NUMERIC(25, 0) NULL, ReadWriteLSN NUMERIC(25, 0) NULL, BackupSizeInBytes BIGINT NULL, SourceBlockSize INT NULL, FileGroupID INT NULL, LogGroupGUID UNIQUEIDENTIFIER NULL, DifferentialBaseLSN NUMERIC(25, 0) NULL, DifferentialBaseGUID UNIQUEIDENTIFIER NULL, IsReadOnly BIT NULL, IsPresent BIT NULL, TDEThumbprint VARBINARY(32) NULL
                                          );
         DECLARE @fullpath AS NVARCHAR(500);
         SET @fullpath = @FullCopyPath + @lastFullBackup;
         INSERT INTO @FileListParameters
         EXEC ('restore filelistonly from disk='''+@fullpath+'''');
         
	    
         DECLARE @MoveOption AS NVARCHAR(1000)= '';
         IF @MoveFiles = 'Y'
             BEGIN

;with Files as(
select ',MOVE ''' + LogicalName  + ''' TO ''' + case when type = 'D' then @MoveDataDrive when Type = 'L' Then @MoveLogDrive end + reverse(left(reverse(PhysicalName),
                    charindex('\',reverse(PhysicalName),
                              1) - 1)) + ''''
						 as logicalcmds from @FileListParameters
						)
select @Moveoption = @Moveoption + logicalcmds from Files


                 --SET @MoveOption = ',MOVE '''+@MoveDataLocationName+''' TO '''+@MoveDataLocation+''','+' MOVE '''+@MoveLogLocationName+''' TO '''+@MoveLOGLocation+'''';
             END;
         SET @cmd = 'RESTORE DATABASE '+@RestoreDatabaseName+' FROM DISK = '''+@FullCopyPath+@lastFullBackup+''' WITH NORECOVERY, REPLACE'+@MoveOption+CHAR(13);
         PRINT @Cmd
	    EXECUTE @cmd = [dbo].[CommandExecute]
                 @Command = @cmd,
                 @CommandType = 'RESTORE DATABASE',
                 @Mode = 1,
                 @DatabaseName = @database,
                 @LogToTable = 'Y',
                 @Execute = 'Y';
         --print @CMD
         --EXECUTE @cmd
         -- Greg: work on this and test for diffs
         -- 4 - Find latest diff backup 
         --SELECT @lastDiffBackup = MAX(backupFile)  
         --FROM @fileList  
         --WHERE backupFile LIKE '%.DIF'  
         --   AND backupFile LIKE @Database + '%' 
         --   AND backupFile > @lastFullBackup 
         ---- check to make sure there is a diff backup 
         --IF @lastDiffBackup IS NOT NULL 
         --BEGIN 
         --   SET @cmd = 'RESTORE DATABASE ' + @RestoreDatabaseName + ' FROM DISK = '''  
         --       + @backupPath + @lastDiffBackup + ''' WITH NORECOVERY' + CHAR(13)
         --   --EXECUTE @cmd = [dbo].[CommandExecute] @Command = @cmd, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @database, @LogToTable = 'Y', @Execute = 'Y'
         --   --PRINT @cmd 
         --   SET @lastFullBackup = @lastDiffBackup 
         --END 
         --Clear out table variables for translogs
         DELETE FROM @fileList;
         SET @cmd2 = 'DIR /b '+@TlogPath;
         INSERT INTO @fileList
                              (backupFile
                              )
         EXEC master.sys.xp_cmdshell
              @cmd2;


         --ok lets get the backup completed data so we can apply tlogs from that point forwards
         DECLARE @headers TABLE
                               (
                                BackupName VARCHAR(256), BackupDescription VARCHAR(256), BackupType VARCHAR(256), ExpirationDate VARCHAR(256), Compressed VARCHAR(256), Position VARCHAR(256), DeviceType VARCHAR(256), UserName VARCHAR(256), ServerName VARCHAR(256), DatabaseName VARCHAR(256), DatabaseVersion VARCHAR(256), DatabaseCreationDate VARCHAR(256), BackupSize VARCHAR(256), FirstLSN VARCHAR(256), LastLSN VARCHAR(256), CheckpointLSN VARCHAR(256), DatabaseBackupLSN VARCHAR(256), BackupStartDate VARCHAR(256), BackupFinishDate VARCHAR(256), SortOrder VARCHAR(256), CodePage VARCHAR(256), UnicodeLocaleId VARCHAR(256), UnicodeComparisonStyle VARCHAR(256), CompatibilityLevel VARCHAR(256), SoftwareVendorId VARCHAR(256), SoftwareVersionMajor VARCHAR(256), SoftwareVersionMinor VARCHAR(256), SoftwareVersionBuild VARCHAR(256), MachineName VARCHAR(256), Flags VARCHAR(256), BindingID VARCHAR(256), RecoveryForkID VARCHAR(256), Collation VARCHAR(256), FamilyGUID VARCHAR(256), HasBulkLoggedData VARCHAR(256), IsSnapshot VARCHAR(256), IsReadOnly VARCHAR(256), IsSingleUser VARCHAR(256), HasBackupChecksums VARCHAR(256), IsDamaged VARCHAR(256), BeginsLogChain VARCHAR(256), HasIncompleteMetaData VARCHAR(256), IsForceOffline VARCHAR(256), IsCopyOnly VARCHAR(256), FirstRecoveryForkID VARCHAR(256), ForkPointLSN VARCHAR(256), RecoveryModel VARCHAR(256), DifferentialBaseLSN VARCHAR(256), DifferentialBaseGUID VARCHAR(256), BackupTypeDescription VARCHAR(256), BackupSetGUID VARCHAR(256), CompressedBackupSize VARCHAR(256), Containment VARCHAR(256),
                                --
                                -- This field added to retain order by
                                --
                                Seq INT NOT NULL IDENTITY(1, 1)
                               );
         INSERT INTO @headers
         EXEC ('restore headeronly from disk = '''+@FullCopyPath+@lastFullBackup+'''');
         DECLARE @BackupCompletedstring AS VARCHAR(15);
         SELECT @BackupCompletedstring = REPLACE(REPLACE(REPLACE(CONVERT( VARCHAR(19), CONVERT(DATETIME, BackupFinishDate, 112), 126), '-', ''), 'T', '_'), ':', '')
         FROM @headers;





         -- 5 - check for log backups 
         DECLARE backupFiles CURSOR
         FOR SELECT backupFile
             FROM @fileList
             WHERE backupFile LIKE '%.TRN'
                   AND backupFile LIKE '%'+@Database+'%'
                   AND RIGHT(backupFile, 19) > @BackupCompletedstring;
         OPEN backupFiles;  

         -- Loop through all the files for the database  
         FETCH NEXT FROM backupFiles INTO @backupFile;
         WHILE @@FETCH_STATUS = 0
             BEGIN
                 SET @cmd = 'RESTORE LOG '+@RestoreDatabaseName+' FROM DISK = '''+@TlogPath+@backupFile+''' WITH NORECOVERY'+CHAR(13);
                 --EXEC (@CMD)
                 EXECUTE @cmd = [dbo].[CommandExecute]
                         @Command = @cmd,
                         @CommandType = 'RESTORE LOG',
                         @Mode = 1,
                         @DatabaseName = @database,
                         @LogToTable = 'Y',
                         @Execute = 'Y';
                 FETCH NEXT FROM backupFiles INTO @backupFile;
             END;
         CLOSE backupFiles;
         DEALLOCATE backupFiles;  

         -- 6 - put database in a useable state 
         SET @cmd = 'RESTORE DATABASE '+@RestoreDatabaseName+' WITH RECOVERY';
         EXECUTE @cmd = [dbo].[CommandExecute]
                 @Command = @cmd,
                 @CommandType = 'RESTORE DATABASE',
                 @Mode = 1,
                 @DatabaseName = @database,
                 @LogToTable = 'Y',
                 @Execute = 'Y';

         --EXEC (@CMD)
         IF @RunCheckDB = 'Y'
             BEGIN
                 --Run a checkdb against this database
                 EXECUTE [dbo].[DatabaseIntegrityCheck]
                         @Databases = @RestoreDatabaseName,
                         @LogToTable = 'Y';
             END;
         IF @TestRestore = 'Y'
             BEGIN
                 SET @Cmd = 'DROP DATABASE '+@RestoreDatabaseName;
                 EXECUTE sp_executesql
                         @CMD;
             END;
     END;
GO
