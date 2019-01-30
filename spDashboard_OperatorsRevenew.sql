/****** Object:  StoredProcedure [dbo].[spDashboard_OperatorsRevenew]    Script Date: 2019-01-30 20:18:06 ÷. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Description:	Sales by POS_Users for chart reports.
-- exec [dbo].[spDashboard_OperatorsRevenew] @DateFrom = '2015-04-01', @DateTo= '2015-04-30', @MaxRows = -10, @BranchID = 2
-- =============================================

ALTER PROCEDURE [dbo].[spDashboard_OperatorsRevenew]
	@DateFrom  datetime = NULL,
	@DateTo datetime = NULL,
	@UserID INTEGER=NULL,
	@MaxRows INTEGER=10,
	@BranchID INT = NULL
AS
BEGIN
	SET NOCOUNT ON;

	SET @DateFrom = coalesce (@DateFrom, convert(varchar(10), Getdate(), 121)) + ' 00:00:00';
	SET @DateTo = coalesce (@DateTo, convert(varchar(10), Getdate(), 121)) + ' 23:59:59';

	SELECT TOP(ABS(@MaxRows)) * 
	FROM (	SELECT 	
				Value = SUM (sd.RowFinalPrice),
				Name = u.FirstName + ' ' + u.LastName,
				ToolTip =	'<div>' + u.FirstName + ' ' + u.LastName + 
							'<br />Amount: ' + cast (sum(sd.RowFinalPrice) as varchar)+ 
							'<br />From: ' + convert(varchar(10), @DateFrom, 103) +
							'<br />To: ' + convert(varchar(10), @DateTo, 103) +
							'</div>'
			FROM NationalExpress2Transactions.dbo.SaleDetails sd 
				INNER JOIN NationalExpress2Transactions.dbo.Sales s on s.UID=sd.SaleUID
				INNER JOIN NationalExpress2Transactions.dbo.SessionDetails ss on ss.UID = s.ClosedSessionDetailUID
				INNER JOIN POS_Users u on u.ID= ss.CreatedBy
			WHERE 	s.voided = 0 and sd.voided = 0 and
					(@BranchID is null or ss.BranchID = @BranchID) and 
					sd.CreatedOn > @DateFrom and  
					sd.CreatedOn < @DateTo
			GROUP BY u.FirstName, u.LastName, u.ID) as tbl2
	Order BY 
	CASE WHEN @MaxRows <0 THEN Value  END ASC,
	CASE WHEN @MaxRows >0 THEN Value  END DESC
END
