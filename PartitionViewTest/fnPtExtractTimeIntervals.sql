Alter FUNCTION [dbo].[fnPtExtractTimeIntervals]
/*
	Description: Returns start and end date of a given month interval
*/
(
	@monthsInteval int,
	@year int,
	@yearPart int
)
RETURNS 
	@temptable TABLE 
	(
		startDate DateTime,
		endDate DateTime
	)
AS
BEGIN
	DECLARE @startDate DateTime;
	DECLARE @endDate DateTime;

	set @startDate = (select cast((cast (@year as varchar) + '-01-01') as DateTime))
	set @startDate = DATEADD (m , (@yearPart -1 ) * @monthsInteval ,@startDate )

	set @endDate = DATEADD (m , @monthsInteval - 1 ,@startDate )
	set @endDate = (SELECT CONVERT(VARCHAR(25),DATEADD(dd,-(DAY(DATEADD(mm,1,@endDate))),DATEADD(mm,1,@endDate)),101))
	
	INSERT INTO @temptable(startDate, endDate)
	VALUES(@startDate, @endDate);

	RETURN 
END
GO








