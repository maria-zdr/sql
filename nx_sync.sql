/****** Object:  StoredProcedure [dbo].[spSync_Barcodes]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:
-- exec spSync_Barcodes @BranchID = 1, @IP = '192.168.1.1', @SessionID = '07A31996-8C97-42C8-BADC-5F801C904398', @FullSync = 1
-- =============================================

CREATE PROCEDURE [dbo].[spSync_Barcodes] 
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50) = NULL,
	@FullSync int = 0,
	@LinkedKeyID bigint = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Result INT = 0,  @msg varchar (max);
	DECLARE @Module sysname = OBJECT_NAME(@@PROCID);
	DECLARE @TableName VARCHAR(100) = REPLACE(@module, 'spSync_', '');
	DECLARE @SummaryOfChanges TABLE(Change varchar(20));

	DECLARE @LastSyncDate datetime 
	SET @LastSyncDate = isnull((SELECT max(CreatedOn) FROM [Log] WHERE Module = @TableName), '2000-01-01')

	BEGIN TRY
	BEGIN TRANSACTION

		MERGE Barcodes AS target
		USING (	SELECT
					b.ID,
					b.ProductID,
					b.Barcode,
					Active = case when (b.Active = -1 and DATEDIFF(d, b.LastEditOn , getdate()) <= 7) then 0 else b.Active end,
					b.LastEditOn
				FROM NationalExpress2.dbo.Barcodes b
				WHERE
					@FullSync = 1
					OR
					b.LastEditOn > @LastSyncDate) AS source ON (target.ID = source.ID)
		WHEN MATCHED AND source.Active >= 0 THEN 
			UPDATE SET
					ProductID = source.ProductID,
					Barcode = source.Barcode,
					Active = source.Active,
					LastEditOn = source.LastEditOn
		WHEN MATCHED AND source.Active = -1 THEN 
			DELETE
		WHEN NOT MATCHED BY SOURCE AND @FullSync = 1 THEN 
			DELETE 
		WHEN NOT MATCHED BY TARGET AND source.Active >= 0 THEN 
			INSERT (
						ID,
						ProductID,
						Barcode,
						Active,
						LastEditOn
					)
			VALUES (
						source.ID,
						source.ProductID,
						source.Barcode,
						source.Active,
						source.LastEditOn
					)
		OUTPUT $action INTO @SummaryOfChanges;

		SELECT @msg = isnull (@msg, '') + Change + ': ' + cast (count (*) as varchar) + ';'
		FROM @SummaryOfChanges
		GROUP BY Change

		IF (@FullSync = 1)
			SET @msg = @TableName + ' full synchronization: ' + IsNull(@msg,'No Changes')
		ELSE 
			SET @msg = @TableName + ' synchronization: ' + IsNull(@msg,'No Changes')

		EXEC dbo.[adminLogMessage] @Type = 1, @Module = @TableName, @Msg = @msg, @IP = @IP, @SessionID = @SessionID, @KeyID = @LinkedKeyID, @BranchID = @BranchID

		COMMIT
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRANSACTION;

		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorNumber INT;
		DECLARE @ErrorProcedure NVARCHAR(MAX);
		DECLARE @ErrorLine INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=ERROR_PROCEDURE(),@ErrorLine=ERROR_LINE();
		EXEC dbo.[adminLogMessage] @Type = 0, @Module = @ErrorProcedure, @Msg = @ErrorMessage, @IP = @IP, @SessionID = @SessionID
		SET @Result = (-(ABS(ISNULL(NULLIF(@ErrorNumber,0),-1))))
	END CATCH 

	RETURN @Result
END;

GO
/****** Object:  StoredProcedure [dbo].[spSync_BoardingTypes]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:
-- exec spSync_BoardingTypes @BranchID = 1, @IP = '192.168.1.1', @SessionID = '07A31996-8C97-42C8-BADC-5F801C904398', @FullSync = 1
-- =============================================

CREATE PROCEDURE [dbo].[spSync_BoardingTypes] 
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50) = NULL,
	@FullSync int = 0,
	@LinkedKeyID bigint = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Result INT = 0,  @msg varchar (max);
	DECLARE @Module sysname = OBJECT_NAME(@@PROCID);
	DECLARE @TableName VARCHAR(100) = REPLACE(@module, 'spSync_', '');
	DECLARE @SummaryOfChanges TABLE(Change varchar(20));

	DECLARE @LastSyncDate datetime 
	SET @LastSyncDate = isnull((SELECT max(CreatedOn) FROM [Log] WHERE Module = @TableName and BranchID = @BranchID), '2000-01-01')

	BEGIN TRY
	BEGIN TRANSACTION

		MERGE BoardingTypes AS target
		USING (	SELECT 
					UID,
					Name,
					Active = case when (Active = -1 and DATEDIFF(d, LastEditOn , getdate()) <= 7) then 0 else Active end,
					SortOrder,
					LastEditOn
				FROM
					NationalExpress2.dbo.BoardingTypes
				WHERE
					@FullSync = 1
					OR
					LastEditOn > @LastSyncDate ) AS source ON (target.UID = source.UID)
		WHEN MATCHED AND source.Active >= 0 THEN 
			UPDATE SET
					Name = source.Name,
					Active = source.Active,
					SortOrder = source.SortOrder,
					LastEditOn = source.LastEditOn
		WHEN MATCHED AND source.Active = -1 THEN
			DELETE
		WHEN NOT MATCHED BY SOURCE AND @FullSync = 1 THEN 
			DELETE
		WHEN NOT MATCHED BY TARGET AND source.Active >= 0 THEN 
			INSERT (
						UID,
						Name,
						Active,
						SortOrder,
						LastEditOn
					)
			VALUES (
						source.UID,
						source.Name,
						source.Active,
						source.SortOrder,
						source.LastEditOn
					)
		OUTPUT $action INTO @SummaryOfChanges;

		SELECT @msg = isnull (@msg, '') + Change + ': ' + cast (count (*) as varchar) + ';'
		FROM @SummaryOfChanges
		GROUP BY Change

		IF (@FullSync = 1)
			SET @msg = @TableName + ' full synchronization: ' + IsNull(@msg,'No Changes')
		ELSE 
			SET @msg = @TableName + ' synchronization: ' + IsNull(@msg,'No Changes')

		EXEC dbo.[adminLogMessage] @Type = 1, @Module = @TableName, @Msg = @msg, @IP = @IP, @SessionID = @SessionID, @KeyID = @LinkedKeyID, @BranchID = @BranchID

		COMMIT
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRANSACTION;

		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorNumber INT;
		DECLARE @ErrorProcedure NVARCHAR(MAX);
		DECLARE @ErrorLine INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=ERROR_PROCEDURE(),@ErrorLine=ERROR_LINE();
		EXEC dbo.[adminLogMessage] @Type = 0, @Module = @ErrorProcedure, @Msg = @ErrorMessage, @IP = @IP, @SessionID = @SessionID
		SET @Result = (-(ABS(ISNULL(NULLIF(@ErrorNumber,0),-1))))
	END CATCH

 
	RETURN @Result 
END;

GO

/****** Object:  StoredProcedure [dbo].[spSync_Departures]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Description:
-- exec spSync_Departures @BranchID = 1, @IP = '192.168.1.1', @SessionID = '07A31996-8C97-42C8-BADC-5F801C904398', @FullSync = 1
-- =============================================

CREATE PROCEDURE [dbo].[spSync_Departures] 
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50) = NULL,
	@FullSync int = 0,
	@LinkedKeyID bigint = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Result INT = 0,  @msg varchar (max);
	DECLARE @Module sysname = OBJECT_NAME(@@PROCID);
	DECLARE @TableName VARCHAR(100) = REPLACE(@module, 'spSync_', '');
	DECLARE @SummaryOfChanges TABLE(Change varchar(20));

	DECLARE @LastSyncDate datetime 
	SET @LastSyncDate = isnull((SELECT max(CreatedOn) FROM [Log] WHERE Module = @TableName and BranchID = @BranchID), '2000-01-01')

	BEGIN TRY
	BEGIN TRANSACTION

		MERGE Departures AS target
		USING (	SELECT 
					d.ID,
					d.RouteID,
					d.SysID,
					d.Code,
					d.RefNo,
					d.Description,
					d.FlightCode,
					d.Direction,
					d.DepartureTime,
					d.Notes,
					d.BrandID,
					d.BranchID,
					Active = case when (d.Active = -1 and DATEDIFF(d, d.LastEditOn , getdate()) <= 7) then 0 else d.Active end,
					d.SortOrder,
					d.LastEditOn
				FROM NationalExpress2.dbo.Departures d
					INNER JOIN NationalExpress2.dbo.Routes r on r.ID = d.RouteID
				WHERE
					(@FullSync = 1 AND (@BranchID = -1 OR d.BranchID in (0, @BranchID)))
					OR
					(d.LastEditOn > @LastSyncDate AND (@BranchID = -1 OR d.BranchID in (0, @BranchID)))
				) AS source ON (target.ID = source.ID)
		WHEN MATCHED AND source.Active >= 0 THEN 
			UPDATE SET
					RouteID = source.RouteID,
					SysID = source.SysID,
					Code = source.Code,
					RefNo = source.RefNo,
					Description = source.Description,
					FlightCode = source.FlightCode,
					Direction = source.Direction,
					DepartureTime = source.DepartureTime,
					Notes = source.Notes,
					BrandID =  source.BrandID,
					BranchID = source.BranchID,
					Active = source.Active,
					SortOrder = source.SortOrder,
					LastEditOn  = source.LastEditOn
		WHEN MATCHED AND source.Active = -1 THEN
			DELETE
		WHEN NOT MATCHED BY SOURCE AND (@FullSync = 1 AND (@BranchID = -1 OR BranchID in (0, @BranchID))) THEN 
			DELETE
		WHEN NOT MATCHED BY TARGET AND source.Active >= 0 THEN 
			INSERT (
						ID,
						RouteID,
						SysID,
						Code,
						RefNo,
						Description,
						FlightCode,
						Direction,
						DepartureTime,
						Notes,
						BrandID,
						BranchID,
						Active,
						SortOrder,
						LastEditOn
					)
			VALUES (
						source.ID,
						source.RouteID,
						source.SysID,
						source.Code,
						source.RefNo,
						source.Description,
						source.FlightCode,
						source.Direction,
						source.DepartureTime,
						source.Notes,
						source.BrandID,
						source.BranchID,
						source.Active,
						source.SortOrder,
						source.LastEditOn
					)
		OUTPUT $action INTO @SummaryOfChanges;

		SELECT @msg = isnull (@msg, '') + Change + ': ' + cast (count (*) as varchar) + ';'
		FROM @SummaryOfChanges
		GROUP BY Change

		IF (@FullSync = 1)
			SET @msg = @TableName + ' full synchronization: ' + IsNull(@msg,'No Changes')
		ELSE 
			SET @msg = @TableName + ' synchronization: ' + IsNull(@msg,'No Changes')

		EXEC dbo.[adminLogMessage] @Type = 1, @Module = @TableName, @Msg = @msg, @IP = @IP, @SessionID = @SessionID, @KeyID = @LinkedKeyID, @BranchID = @BranchID

		COMMIT
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRANSACTION;

		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorNumber INT;
		DECLARE @ErrorProcedure NVARCHAR(MAX);
		DECLARE @ErrorLine INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=ERROR_PROCEDURE(),@ErrorLine=ERROR_LINE();
		EXEC dbo.[adminLogMessage] @Type = 0, @Module = @ErrorProcedure, @Msg = @ErrorMessage, @IP = @IP, @SessionID = @SessionID
		SET @Result = (-(ABS(ISNULL(NULLIF(@ErrorNumber,0),-1))))
	END CATCH

 
	RETURN @Result 
END;

GO
/****** Object:  StoredProcedure [dbo].[spSync_PriceMap]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:
-- exec spSync_PriceMap @BranchID = 1, @IP = '192.168.1.1', @SessionID = '07A31996-8C97-42C8-BADC-5F801C904398', @FullSync = 1
-- =============================================

CREATE PROCEDURE [dbo].[spSync_PriceMap] 
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50) = NULL,
	@FullSync int = 0,
	@LinkedKeyID bigint = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Result INT = 0,  @msg varchar (max);
	DECLARE @Module sysname = OBJECT_NAME(@@PROCID);
	DECLARE @TableName VARCHAR(100) = REPLACE(@module, 'spSync_', '');
	DECLARE @SummaryOfChanges TABLE(Change varchar(20));

	DECLARE @LastSyncDate datetime 
	SET @LastSyncDate = isnull((SELECT max(CreatedOn) FROM [Log] WHERE Module = @TableName and BranchID = @BranchID), '2000-01-01')

	BEGIN TRY
	BEGIN TRANSACTION

		MERGE PriceMap AS target
		USING (	SELECT
					UID,
					FromLocationID,
					ToLocationID,
					RouteID,
					BrandID,
					Active = case when (Active = -1 and DATEDIFF(d, LastEditOn , getdate()) <= 7) then 0 else Active end,
					SortOrder,
					LastEditOn
				FROM NationalExpress2.dbo.PriceMap
				WHERE
					@FullSync = 1
					OR
					LastEditOn > @LastSyncDate ) AS source ON (target.UID = source.UID)
		WHEN MATCHED AND source.Active >= 0 THEN
			UPDATE SET
					FromLocationID = source.FromLocationID,
					ToLocationID = source.ToLocationID,
					RouteID = source.RouteID,
					BrandID = source.BrandID,
					Active = source.Active,
					SortOrder = source.SortOrder,
					LastEditOn = source.LastEditOn
		WHEN MATCHED AND source.Active = -1 THEN 
			DELETE 
		WHEN NOT MATCHED BY SOURCE AND @FullSync = 1 THEN DELETE
		WHEN NOT MATCHED BY TARGET AND source.Active >= 0 THEN 
			INSERT (
						UID,
						FromLocationID,
						ToLocationID,
						RouteID,
						BrandID,
						Active,
						SortOrder,
						LastEditOn
					)
			VALUES (
						source.UID,
						source.FromLocationID,
						source.ToLocationID,
						source.RouteID,
						source.BrandID,
						source.Active,
						source.SortOrder,
						source.LastEditOn
					)
		OUTPUT $action INTO @SummaryOfChanges;

		SELECT @msg = isnull (@msg, '') + Change + ': ' + cast (count (*) as varchar) + ';'
		FROM @SummaryOfChanges
		GROUP BY Change

		IF (@FullSync = 1)
			SET @msg = @TableName + ' full synchronization: ' + IsNull(@msg,'No Changes')
		ELSE 
			SET @msg = @TableName + ' synchronization: ' + IsNull(@msg,'No Changes')

		EXEC dbo.[adminLogMessage] @Type = 1, @Module = @TableName, @Msg = @msg, @IP = @IP, @SessionID = @SessionID, @KeyID = @LinkedKeyID, @BranchID = @BranchID

		COMMIT
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRANSACTION;

		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorNumber INT;
		DECLARE @ErrorProcedure NVARCHAR(MAX);
		DECLARE @ErrorLine INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=ERROR_PROCEDURE(),@ErrorLine=ERROR_LINE();
		EXEC dbo.[adminLogMessage] @Type = 0, @Module = @ErrorProcedure, @Msg = @ErrorMessage, @IP = @IP, @SessionID = @SessionID
		SET @Result = (-(ABS(ISNULL(NULLIF(@ErrorNumber,0),-1))))
	END CATCH
 
	RETURN @Result 
END;


GO


/****** Object:  StoredProcedure [dbo].[adminExecReletedSyncProcedures]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:	Synchronises related tables based on foreign key constraints; only related tables, not the main one
-- Useage:
--	EXEC [dbo].[adminExecReletedSyncProcedures] @IP = '192.168.1.1', @SessionID = '12331996-8C97-42C8-BADC-5F801C904398', @TableName = 'Routes'
-- =============================================

CREATE PROCEDURE [dbo].[adminExecReletedSyncProcedures]
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50),
	@FullSync int = 0,
	@TableName sysname = NULL
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @proc sysname = ''
	DECLARE @SQLString NVARCHAR(MAX)='';

	DECLARE @LinkedKeyID bigint
	SET @LinkedKeyID = (ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)))
	
	DECLARE allSyncProcs CURSOR LOCAL FOR
		SELECT distinct 
			'spSync_' + FK.TABLE_NAME
			--FK_Column = CU.COLUMN_NAME,
			--PK_Table = PK.TABLE_NAME,
			--Constraint_Name = C.CONSTRAINT_NAME
		FROM NationalExpress2.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
			INNER JOIN NationalExpress2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
			INNER JOIN NationalExpress2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
			INNER JOIN NationalExpress2.INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
		WHERE
			(@TableName is NULL or PK.TABLE_NAME = @TableName)
 
	OPEN allSyncProcs FETCH NEXT FROM allSyncProcs INTO @proc
 
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = @proc)
		BEGIN
			SET @SQLString = N'exec @proc @BranchID = @BranchID, @IP = @IP, @SessionID = @SessionID, @FullSync = @FullSync, @LinkedKeyID = @LinkedKeyID'
			print @SQLString

			DECLARE @Params nvarchar (max) = N'	@proc sysname,
												@BranchID INT,
												@IP varchar (50), 
												@SessionID varchar (50),
												@FullSync int,
												@LinkedKeyID bigint'

			BEGIN TRY
			
				EXEC sp_executesql @SQLString, @Params,	@proc,
														@BranchID,
														@IP,
														@SessionID,
														@FullSync,
														@LinkedKeyID


				END TRY
				BEGIN CATCH
					IF (@@TRANCOUNT > 0)
						ROLLBACK TRANSACTION;

					DECLARE @ErrorMessage NVARCHAR(MAX);
					DECLARE @ErrorNumber INT;
					DECLARE @ErrorProcedure NVARCHAR(MAX);
					DECLARE @ErrorLine INT;

					SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=ERROR_PROCEDURE(),@ErrorLine=ERROR_LINE();
					EXEC dbo.[adminLogMessage] @Type = 0, @Module = @ErrorProcedure, @Msg = @ErrorMessage, @IP = @IP, @SessionID = @SessionID
				END CATCH
			END
		FETCH NEXT FROM allSyncProcs INTO @proc
	END

	CLOSE allSyncProcs
	DEALLOCATE allSyncProcs 

	DECLARE @Result varchar (max) = ''

	--SELECT @Result = @Result + isnull(Description, '') + char(13) + char(10)
	select Description = isnull(l.Description, ''), l.CreatedOn
	FROM [Log] l
	WHERE LinkedKeyID = @LinkedKeyID

END


GO
/****** Object:  StoredProcedure [dbo].[adminExecSyncProcedures]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:	Executes procedures with "spSync" prefix, for tables that have new data
-- Useage:		EXEC [dbo].[adminExecSyncProcedures] @SessionID = '92331996-8C97-42C8-BADC-5F801C904398', @IP = '192.168.65.5'
-- =============================================

CREATE PROCEDURE [dbo].[adminExecSyncProcedures]
	@BranchID int = 0,
	@IP varchar (50) = NULL,
	@SessionID varchar (50),
	@FullSync int = 0
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @proc sysname = ''
	DECLARE @SQLString NVARCHAR(MAX)='';

	DECLARE @LinkedKeyID bigint
	SET @LinkedKeyID = (ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)))

	DECLARE @SummaryOfChanges TABLE(TableName sysname, ChangeCnt int); 
	DECLARE @cnt int = 0
	DECLARE @TblName varchar(200)
	DECLARE @LastSyncDate datetime

	DECLARE allTables CURSOR LOCAL FOR
		select ProcName = name, TableName = REPLACE (name , 'spSync_', '') 
		from sys.objects
		where type = 'P' and name like 'spSync%'
		order by name
  
	OPEN allTables FETCH NEXT FROM allTables INTO @proc, @TblName
 
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @LastSyncDate = isnull ((SELECT MAX (CreatedOn) FROM [Log] WHERE Module = @TblName AND Type = 1), 
									'2000-01-01')

		SET @SQLString = N'select @cnt = count (*) from NationalExpress2.dbo.'+ cast (@TblName as varchar) +' where LastEditOn > '''+ cast (@LastSyncDate as varchar) + ''''
		EXECUTE sp_executesql @SQLString, N' @cnt int OUTPUT', @cnt=@cnt OUTPUT;

		IF (@FullSync = 1 OR @cnt > 0)
		BEGIN
			SET @SQLString = N'exec @proc @BranchID = @BranchID, @IP = @IP, @SessionID = @SessionID, @FullSync = @FullSync, @LinkedKeyID = @LinkedKeyID'

			DECLARE @Params nvarchar (max) = N'	@proc sysname,
												@BranchID INT,
												@IP varchar (50), 
												@SessionID varchar (50),
												@FullSync int,
												@LinkedKeyID bigint'

			EXEC sp_executesql @SQLString, @Params,	@proc,
													@BranchID,
													@IP,
													@SessionID,
													@FullSync,
													@LinkedKeyID
		END
		FETCH NEXT FROM allTables INTO @proc, @TblName
	END
 
	CLOSE allTables
	DEALLOCATE allTables

	SELECT  Description = isnull(l.Description, ''), l.CreatedOn
	FROM [Log] l
	WHERE LinkedKeyID = @LinkedKeyID

END


GO

/****** Object:  StoredProcedure [dbo].[adminTableChanges]    Script Date: 2019-01-30 19:24:09 ч. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:	Returns number of changes per table, between main and sync DB
-- Useage:		EXEC [dbo].[adminTableChanges] @TableName = 'Products'
-- =============================================

CREATE PROCEDURE [dbo].[adminTableChanges]
	@TableName sysname = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @SQLString nvarchar(MAX) = ''
	--DECLARE @msg nvarchar(MAX) = ''
	DECLARE @SummaryOfChanges TABLE(TableName sysname, ChangeCnt int); 
	DECLARE @cnt int = 0

	DECLARE @TblName varchar(200)
	DECLARE @LastSyncDate datetime

	DECLARE allTables CURSOR LOCAL FOR
		select o.name, ISNULL(l.LastSyncDate, '2000-01-01')
		from sys.objects o
		LEFT JOIN
			(SELECT Module, LastSyncDate = MAX (CreatedOn)
			FROM [Log]
			WHERE Type = 1
			GROUP BY Module) l ON l.Module = o.name
		where 
			o.type = 'U' 
			--AND o.name <> 'Log'
			AND o.name IN (select name FROM NationalExpress2.sys.objects)
			AND (@TableName IS NULL OR o.name = @TableName)
		order by o.name
 
	OPEN allTables FETCH NEXT FROM allTables INTO @TblName, @LastSyncDate
 
	WHILE @@FETCH_STATUS = 0
	BEGIN

		SET @SQLString = N'select @cnt = count (*) from NationalExpress2.dbo.'+ cast (@TblName as varchar) +' where LastEditOn > '''+ cast (@LastSyncDate as varchar) + ''''

		EXECUTE sp_executesql @SQLString, N' @cnt int OUTPUT', @cnt=@cnt OUTPUT;
		--SET @msg = @msg + @TblName + ': Changes ' + cast (@cnt as varchar) + char(13) + char(10)
		
		INSERT INTO @SummaryOfChanges (TableName, ChangeCnt)
		VALUES ( @TblName, @cnt)

	   FETCH NEXT FROM allTables INTO @TblName, @LastSyncDate
	END
 
	CLOSE allTables
	DEALLOCATE allTables 

	--SELECT @msg
	SELECT * FROM @SummaryOfChanges

END
GO