
/* 
exec [dbo].[adminCreateNextPeriodTable] @ViewName='PDA_Sales'
*/

ALTER PROCEDURE [dbo].[adminCreateNextPeriodTable] 
(
@ViewName varchar (max),
@dayCnt int  =  10
)
AS
BEGIN

DECLARE @Result INT = 0;

	BEGIN TRY
	BEGIN TRANSACTION
		-- Partitioned views variables
		DECLARE @ViewID int
		DECLARE @PartitionInterval int
		DECLARE @BaseTableStructure varchar(max) = ''
		DECLARE @BaseTableTriggers varchar(max) = ''
		DECLARE @LastDate datetime
		
		--extracting partitioned view details
		SELECT @ViewID = ViewID, @PartitionInterval = PartitionInterval, @BaseTableStructure = BaseTableStructure, @BaseTableTriggers = BaseTableTriggers, @LastDate = LastDate
		from adminPartitionedViewsControl
		where ViewName = @ViewName

		--Date filter here
		IF (DATEDIFF (d, getdate(), @LastDate) < @dayCnt)
		BEGIN

			-- additional variables
			DECLARE @TableName varchar(max) = ''
			DECLARE @fieldList varchar(max) = ''
			DECLARE @sql varchar(max) = ''
			DECLARE @startDate date
			DECLARE @endDate date
			
			--getting next period interval
			set @startDate = DATEADD (m, 1, DATEADD(month, DATEDIFF(month, 0, @LastDate), 0)) 
			set @endDate = DATEADD(d, -1, DATEADD(m, DATEDIFF(m, 0, DATEADD (m , 6 , @LastDate )) + 1, 0))

			--generating new table name
			set @TableName = @ViewName + '_' + cast(year(@startDate) as varchar) + '_' + RIGHT('00'+ CONVERT(VARCHAR,month(@startDate)),2) + '_' + RIGHT('00'+ CONVERT(VARCHAR,month(@endDate)),2)
			
			-- prepare the scripts for the new table
			SET @BaseTableStructure = REPLACE(@BaseTableStructure,'##TABLENAME##',@TableName );
			SET @BaseTableStructure = REPLACE(@BaseTableStructure,'##STARTDATE##',@startDate );
			SET @BaseTableStructure = REPLACE(@BaseTableStructure, '##ENDDATE##', dateadd (d , 1 , @endDate));
			EXEC(@BaseTableStructure)
			
			-- prepare the triggers	
			if (@BaseTableTriggers is not null)
			begin 
				SET @BaseTableTriggers = REPLACE(@BaseTableTriggers,'##TA_LENAME##',@TableName ); --error
				EXEC(@BaseTableTriggers)
			end
			
			--Update PartitionedViews table
			update adminPartitionedViewsControl set LastDate= @endDate 
			where ViewName = @ViewName
			
			--retrieve columns
			SELECT @fieldList = @fieldList + COLUMN_NAME + ', '  
			FROM INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_NAME = @TableName
			
			SET @fieldList = LEFT(@fieldList, LEN(@fieldList) -1)
			
			--Alter view; If recreate view - need to change ViewID conception in adminPartitionedViewsControl
			set @sql = (	select definition
							from sys.objects     o
							join sys.sql_modules m on m.object_id = o.object_id
							where o.object_id = object_id( @ViewName) and o.type      = 'V')
			
			SET @sql = REPLACE(@sql,'CREATE VIEW','ALTER VIEW' );
			SET @sql = @sql  + CHAR(13) + CHAR(10)+  ' union all ' + CHAR(13) + CHAR(10)+ ' select ' + @fieldList + ' from ' +  @TableName  
			print @sql

			exec (@sql)
		END
	COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK
	END CATCH
END