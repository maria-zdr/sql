
-- =============================================
-- Description:	<Description,,>
-- Usage: exec [dbo].[spAdminSplitTable] @TableName = 'Sessions', @ColumnName = 'CreatedOn' , @monthsInteval = 1, @startSplitDate = '2014-04-22', @endSplitDate = '2014-05-12'
-- =============================================

ALTER PROCEDURE [dbo].[spAdminSplitTable]
(
	@TableName nvarchar(max), 
	@ColumnName nvarchar(max),
	@monthsInteval int,
	@startSplitDate datetime,
	@endSplitDate datetime
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @TableNameNew nvarchar(max) = ''
	DECLARE @TableSuffix nvarchar(max) = ''
	DECLARE @primaryKey nvarchar(max) = ''
	DECLARE @fieldList varchar(max) = ''
	DECLARE @ViewDefinition varchar(max) = ''

	DECLARE @CurrEndDate date
	DECLARE @year int
	DECLARE @SQL nvarchar(max) = ''
	DECLARE @nl varchar(max) = CHAR(13) + CHAR(10)

	BEGIN TRY
	BEGIN TRANSACTION 
		--first day of starting month
		set @startSplitDate  = DATEADD(month, DATEDIFF(month, 0, @startSplitDate), 0) 

		--Base table PK;
		SELECT @primaryKey = @primaryKey + COL_NAME(ic.OBJECT_ID,ic.column_id) + ', '  
		FROM sys.indexes AS i
		INNER JOIN sys.index_columns AS ic ON i.OBJECT_ID = ic.OBJECT_ID
		AND i.index_id = ic.index_id
		WHERE i.is_primary_key = 1 and  OBJECT_NAME(ic.OBJECT_ID) = @TableName

		SET @primaryKey = LEFT(@primaryKey, LEN(@primaryKey) -1)

		--Base table columns;
		SELECT @fieldList = @fieldList + name + ', ' 
		from sys.columns
		where OBJECT_NAME (object_id) = @TableName

		SET @fieldList = LEFT(@fieldList, LEN(@fieldList) -1)

		-- create vew 
		SET @ViewDefinition = 'CREATE VIEW ' + @TableName + ' AS ' + @nl


		WHILE (@startSplitDate < @endSplitDate)
		BEGIN
			SET @year = YEAR (@startSplitDate)
			-- end date restriction
			set @CurrEndDate = DATEADD (m , @monthsInteval - 1 ,@startSplitDate )
			-- last day of the month
			set @CurrEndDate = DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,@CurrEndDate)+1,0))

			set @TableSuffix = RIGHT('00'+ CONVERT(VARCHAR,month(@startSplitDate)),2) + '_' + RIGHT('00'+ CONVERT(VARCHAR,month(@CurrEndDate)),2)
			set @TableNameNew = @TableName + '_' + cast (@year as varchar) + '_' + @TableSuffix

			-- table structure
			SET @SQL = 'SELECT * INTO ' + @TableNameNew +' FROM ' +  @TableName +'  WHERE 1 = 2'
			exec (@SQL)

			-- Create PK
			SET @SQL = 'ALTER TABLE ' + @TableNameNew + ' ADD CONSTRAINT pk_' + @TableNameNew + ' PRIMARY KEY( ' + @primaryKey + ', '+ @ColumnName + ')'
			exec(@SQL)

			-- Adding partition column constraint
			SET @SQL = 'ALTER TABLE [dbo].[' + @TableNameNew + ']  WITH CHECK ADD  CONSTRAINT [CK_' + @TableNameNew + '] CHECK ((['+ @ColumnName + ']>='''+ convert (varchar(10), @startSplitDate, 121) + ''') and (['+ @ColumnName + ']<'''+ convert (varchar(10), dateadd (d , 1 , @CurrEndDate), 121) + '''))'
			exec(@SQL)
				
			-- Moving data
			SET @SQL = 'insert into ' + @TableNameNew + ' select * from ' + @TableName + ' where ' + @ColumnName + ' >= '''+ convert (varchar(10), @startSplitDate, 121) + ''' and  ' + @ColumnName + ' < '''+ convert (varchar(10), dateadd (d , 1 , @CurrEndDate), 121) + ''''
			exec(@SQL)
			
			-- adding table to the view
			SET @ViewDefinition = @ViewDefinition + ' select ' + @fieldList + ' from ' +  @TableNameNew + @nl
			SET @ViewDefinition = @ViewDefinition + ' union all ' + @nl

			set @startSplitDate = DATEADD (d , 1 , @CurrEndDate ) 
		END
		
		-- rename table 
		SET @SQL = 'sp_rename ''' + @TableName + ''', ''' + @TableName + '_BCK'''
		exec(@SQL)

		-- create view
		SET @ViewDefinition = LEFT(@ViewDefinition, LEN(@ViewDefinition) -12)
		exec (@ViewDefinition)

	COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK
			
		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorNumber INT;
		DECLARE @ErrorProcedure NVARCHAR(MAX);
		DECLARE @ErrorLine INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber=ERROR_NUMBER(),@ErrorProcedure=OBJECT_NAME(@@PROCID),@ErrorLine=ERROR_LINE();
		EXEC dbo.[spLogMessage] @ErrorProcedure, @ErrorMessage
	END CATCH
END
