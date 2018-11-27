-- =============================================
-- Author:		MS
-- Create date: 2016-05-04
-- Description:
-- exec [dbo].[spDashboard_OperatorsRevenew] @DateFrom = '2015-04-01', @DateTo= '2015-05-31', @MaxRows = -10, @BranchID = 1
-- =============================================

CREATE PROCEDURE [dbo].[spDashboard_OperatorsRevenew]
	@DateFrom date = NULL,
	@DateTo date = NULL,
	@UserID INTEGER = NULL,
	@MaxRows INTEGER = 10,
	@BranchID INT = NULL
AS
BEGIN
	SET NOCOUNT ON;

	SET @DateFrom = coalesce (@DateFrom, Getdate())
	SET @DateTo = coalesce (@DateTo, Getdate())

	SELECT TOP(ABS(@MaxRows)) * 
	FROM (	SELECT
				Value = SUM(RowFinalPrice),
				Name = sesd.FirstName + ' ' +sesd.LastName,
				ToolTip =	'<div>Operator: ' + cast (sesd.FirstName + ' ' +sesd.LastName as varchar (100))+ 
							'<br />From: ' + convert(varchar(10), @DateFrom, 103) +
							'<br />To: ' + convert(varchar(10), @DateTo, 103) +
							'</div>' 
			FROM Sales s
				INNER JOIN SaleDetails sd on sd.SaleUID = s.UID
				INNER JOIN vwSessionDetails sesd on sesd.UID=s.ClosedSessionDetailUID
				INNER JOIN SaleItems si on si.UID = sd.SaleItemUID
				INNER JOIN Products p on si.ProductUID = p.ID
			WHERE 
				s.Voided = 0 AND sd.Voided = 0 
				AND s.ClosedOn >= @DateFrom AND s.ClosedOn < DATEADD (d , 1 , @DateTo)
				AND (@BranchID = -1 OR @BranchID = sesd.BranchID)
			GROUP BY sesd.FirstName, sesd.LastName) as tbl
	ORDER BY 
		CASE WHEN @MaxRows <0 THEN Value  END ASC,
		CASE WHEN @MaxRows >0 THEN Value  END DESC
END
GO