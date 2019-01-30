
ALTER FUNCTION [dbo].[fnPtSearchTimeIntervals]
(
	@monthsInteval int,
	@myDate datetime
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
	declare @cnt int  = 12 / @monthsInteval
	
	while (@cnt > 0)
		begin 
			SELECT @startDate = startDate, @endDate = endDate  
			from [dbo].[fnPtExtractTimeIntervals] ( @monthsInteval, year(@myDate), @cnt)
			
			if ( @myDate>= @startDate and @myDate<= @endDate )
				begin
					INSERT INTO @temptable(startDate, endDate)
					VALUES(@startDate, @endDate);

					BREAK
				end 
			set @cnt = @cnt - 1
		end
	RETURN 
END




