Use Master

GO

CREATE DATABASE IndexStats

GO

USE [IndexStats]
GO
/****** Object:  Table [dbo].[Messages]    Script Date: 6/4/2020 10:14:31 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Messages](
	[MessageID] [int] IDENTITY(1,1) NOT NULL,
	[Message] [varchar](max) NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_Messages] PRIMARY KEY CLUSTERED 
(
	[MessageID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ServerDatabases]    Script Date: 6/4/2020 10:14:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServerDatabases](
	[DatabaseID] [bigint] NOT NULL,
	[DatabaseName] [varchar](200) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ServerTables]    Script Date: 6/4/2020 10:14:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServerTables](
	[ServerTableID] [bigint] IDENTITY(1,1) NOT NULL,
	[DatabaseID] [bigint] NOT NULL,
	[Schema_id] [int] NULL,
	[schema_Name] [varchar](50) NULL,
	[TableName] [varchar](100) NOT NULL,
	[Object_id] [bigint] NULL,
	[Index_Name] [varchar](500) NULL,
	[Index_id] [bigint] NULL,
	[Index_Desc] [varchar](100) NULL,
	[Fragmentation] [decimal](10, 2) NULL,
	[Hint] [varchar](30) NULL,
	[Partition] [bigint] NULL,
	[Updated] [smallint] NULL,
	[DaateModified] [datetime] NULL,
 CONSTRAINT [PK_ServerTables] PRIMARY KEY CLUSTERED 
(
	[ServerTableID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Messages] ADD  DEFAULT (getdate()) FOR [DateModified]
GO
ALTER TABLE [dbo].[ServerTables] ADD  DEFAULT ((0)) FOR [Updated]
GO
ALTER TABLE [dbo].[ServerTables] ADD  DEFAULT (getdate()) FOR [DaateModified]
GO
/****** Object:  StoredProcedure [dbo].[sp_PopulateDatabases]    Script Date: 6/4/2020 10:14:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_PopulateDatabases]

/****** Developer Innocent Gumbo  May 2020 ******/

       --  @DatabaseName varchar(100)

AS 
  BEGIN
     TRUNCATE TABLE ServerDatabases
     TRUNCATE TABLE ServerTables
	 TRUNCATE TABLE [dbo].[Messages]
      
       INSERT INTO ServerDatabases(DatabaseID,DatabaseName)
       SELECT DB_ID([Name]), [name] FROM sys.databases  WHERE database_id > 4 AND [Name] NOT LIKE '%rdsadmin%'      --> 5 --WHERE  [name] = RTRIM(LTRIM(@DatabaseName))
     
  END

GO
/****** Object:  StoredProcedure [dbo].[sp_PopulateTables]    Script Date: 6/4/2020 10:14:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[sp_PopulateTables]

/****** Developer Innocent Gumbo  May 2020 ******/
AS 
  
     DECLARE @SQLText VARCHAR(MAX)
     declare @SQLTx varchar(max) 
     declare @Frag numeric(10,2)
     declare @Table varchar(50)
     declare @index_id bigint
     declare @databaseName varchar(50)
     declare @objectid bigint
     DECLARE @index_Name varchar(500)
     DECLARE @DatabaseID bigint
     DECLARE @Index_Desc varchar(50)
     DECLARE @SQLText2 varchar(MAX)
     Declare @TableID  bigint
     DECLARE @schemaName varchar(50)
     declare @schema_id bigint
     
     TRUNCATE TABLE dbo.ServerTables

     DECLARE CurDatabses CURSOR FOR 
             SELECT DatabaseID,DatabaseName
             FROM dbo.ServerDatabases 
             
             OPEN CurDatabses
     PRINT  '1 . Populating Tables........'
     FETCH NEXT FROM CurDatabses
        INTO @databaseID, @DatabaseName
 
     WHILE @@FETCH_STATUS = 0
     BEGIN
         SET @databaseName = '[' + @databaseName + ']'
         SET  @SQLText = ' select ' + convert(varchar(20),@databaseID) + ' ,  o.NAME  ,O.object_id,i.name  ,
        i.index_id , 
        i.type_desc ,o.schema_id'
    
      SET  @SQLText =  @SQLText +  ' FROM   '  + @DatabaseName +  '.sys.objects O 
                                   INNER JOIN  '     + @DatabaseName   + '.sys.indexes i 
                                   ON     o.object_id = i.object_id 
                                   AND O.type = ''U''
                                   AND i.index_id > 0 '

    SET @SQLTx = ' insert ServerTables(DatabaseID,TableName,[Object_ID],Index_Name,Index_Id,Index_Desc,[Schema_id]) '
    
    
    SET   @SQLText  = @SQLTx + @SQLText
   
    Exec   ( @SQLText )
   
    
     
     
    
--      UPDATE ServerTables SET schema_name = @SchemaName
--             WHERE ServerTableId = @TableID
    
     DECLARE CurTables CURSOR FOR 
            SELECT ServerTableID ,a.DatabaseID,TableName, index_ID,Index_Desc,Object_ID, DatabaseName
            FROM dbo.ServerTables a --WHERE DatabaseID = @databaseID
			JOIN ServerDatabases b ON a.DatabaseID = b.DatabaseID
      
      OPEN   CurTables 
     
      FETCH NEXT FROM CurTables
          INTO @TableID, @databaseID,@Table,@index_id,@Index_Desc,@objectid, @DatabaseName
      PRINT '2.  Updating Fragmentation ..... Database ID: ' +   CONVERT(VARCHAR(10),@databaseID ) + ':Table  :' + @Table
       WHILE @@FETCH_STATUS = 0
       BEGIN
--          
           CREATE TABLE #Temp
              (ServerTableID bigint,Frag float)
           SET @databaseName = '[' + @databaseName  + ']'
           SET  @SQLText =  ' SELECT ' + convert(varchar(20),@TableID) + ', avg_fragmentation_in_percent FROM '
          
             + @DatabaseName   + '.sys.dm_db_index_physical_stats( ' +convert(varchar(20),@DatabaseID) + ','
			                   +  convert(varchar(10),@objectid) + ',' + convert(varchar(20),@index_id) + ',' + 'NULL' + ',' +  
                               '''Limited'''  + ') '    
            SET @SQLText2 = 'INSERT #Temp '
            SET @SQLText = @SQLText2 +  @SQLText
          --  select  @SQLText  
            
            
            exec(@SQLText)
            
        
     
      UPDATE   ServerTables
              SET Fragmentation = Temp.Frag
              FROM #Temp   Temp
              WHERE Temp.ServerTableID = ServerTables.ServerTableID
           
         drop table #Temp
           FETCH NEXT FROM CurTables
          INTO @TableID, @databaseID,@Table,@index_id,@Index_Desc,@objectid, @DatabaseName
          
          END -- Tables within a database
         close CurTables
         deallocate CurTables
     FETCH NEXT FROM CurDatabses
     INTO @databaseID, @DatabaseName 
     END -- CurDatabses

     close CurDatabses
     deallocate CurDatabses

	 PRINT ' 3. Updating Rebuild Hint ....'
        UPDATE ServerTables 
             SET Hint  =
               ( CASE WHEN Fragmentation < 10 THEN 'NOTHING'
                      WHEN Fragmentation  < 31 THEN 'REORGANIZE'
                      WHEN Fragmentation > 30  THEN 'REBUILD'
                 END)

    


    DECLARE curSchema CURSOR
          FOR SELECT  DatabaseName, DatabaseID
              FROM ServerDatabases  where Len(DatabaseName)< 55
               
      
      
      OPEN curSchema
      FETCH NEXT FROM curSchema
          INTO @DatabaseName ,@DatabaseID
            
			PRINT ' 4. Updating Schema Name ......'
			WHILE @@FETCH_STATUS = 0
            BEGIN
           SET @DatabaseName = '[' + @DatabaseName + ']'
           SET @SQLText = 'UPDATE ServerTables
               SET schema_name =    convert(varchar(50),s.name) 
              from  ' +    @DatabaseName   + '.sys.schemas s
              where
                s.schema_id = ServerTables.schema_id AND
                ServerTables.DatabaseID = ' +  CONVERT(VARCHAR(20),@DatabaseID)
  
             EXEC (@SQLText)
             SET @DatabaseName = ''
    FETCH NEXT FROM curSchema
          INTO @DatabaseName ,@DatabaseID
  END
     CLOSE  curSchema
     DEALLOCATE curSchema

---------------------------

GO
/****** Object:  StoredProcedure [dbo].[sp_ReindexTables]    Script Date: 6/4/2020 10:14:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[sp_ReindexTables]
  
 /****** Developer Innocent Gumbo  May 2020 ******/ 
 AS
    DECLARE @SQLText VARCHAR(MAX)
    DECLARE @ServerDatabaseID bigint
    DECLARE @TableName VARCHAR(100)
    DECLARE @Index_Name VARCHAR(500)
    DECLARE @Fragmentation BIGINT
    DECLARE @Hint VARCHAR(20)
    DECLARE @schema VARCHAR(50)
	DECLARE @ServerTableID BIGINT
    DECLARE @DBCC VARCHAR(MAX)  
    DECLARE @SQL VARCHAR(MAX) 
	DECLARE @DatabaseName VARCHAR(100)
   
   DECLARE CurReindex CURSOR  LOCAL STATIC FORWARD_ONLY   FOR
          SELECT  s.DatabaseID,s.TableName, s.Index_Name, s.Fragmentation,s.Hint,s.Schema_Name , s.ServerTableID , d.DatabaseName
           FROM  ServerTables s
        INNER JOIN  ServerDatabases d
         ON  s.DatabaseID = d.DatabaseID
        -- AND d.DatabaseName = @DatabaseName
         AND s.Hint  IN('REORGANIZE' ,'REBUILD')
		 AND s.Updated <> 1      
       -- SET @DatabaseName = '[' + @DatabaseName + ']' 
    OPEN CurReindex
      
      FETCH NEXT FROM CurReindex 
      INTO @ServerDatabaseID,@TableName,@Index_Name,@Fragmentation,@Hint,@schema ,@ServerTableID, @DatabaseName
     
      WHILE @@FETCH_STATUS = 0
       BEGIN
	     SET @DatabaseName = '[' + @DatabaseName + ']'
         SET @SQL = ''
         SET @schema       = '[' + @schema + ']'
         SET @TableName    = '[' + @TableName + ']'
         SET @DBCC         =  '''' +  @DatabaseName + '.' + @schema + '.' + @TableName + ''''
         SET  @SQLText =
              (CASE   
               WHEN  @Hint= 'REORGANIZE' THEN 
                 'ALTER INDEX  ' +  @Index_Name  + ' ON ' +  @DatabaseName  + '.' +  @schema +  '.' + @TableName +  ' REORGANIZE'
                     
              WHEN @Hint =  'REBUILD' THEN  
                'ALTER INDEX  ' +  @Index_Name  + ' ON ' +  @DatabaseName  + '.' +  @schema +  '.' + @TableName + '  REBUILD' 
               
             END )-- End CASE
              INSERT [Messages] ([Message] )
                  VALUES (@SQLText)
                 SET @SQL =
                  (CASE
                    WHEN @hint  = 'REBUILD' THEN
                        ' DBCC DBREINDEX (' +  @DBCC  + ',' +   @Index_Name   + ') WITH NO_INFOMSGS  '  ---WITH NO_INFOMSGS
                     
                   END)
                  
               --PRINT @SQLText
               EXEC (@SQLText)
               EXEC (@SQL)
              --  PRINT @SQL   --Remember to remove/comment this when you run this as a job
			  UPDATE [dbo].[ServerTables] SET Updated = 1 WHERE ServerTableID = @ServerTableID
       FETCH NEXT FROM CurReindex 
       INTO @ServerDatabaseID,@TableName,@Index_Name,@Fragmentation,@Hint,@schema ,@ServerTableID, @DatabaseName  
      
      END
         close CurReindex
         deallocate CurReindex


--===================================================
  --For Testing purposes

		 --EXEC [dbo].[sp_PopulateDatabases]
		 --GO
		 --EXEC [dbo].[sp_PopulateTables]
		 --GO
		 --EXEC [dbo].[sp_ReindexTables]
--=====================================================
		 
GO
