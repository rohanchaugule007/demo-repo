USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Reins_Payment_Processing_ACE]    Script Date: 02-06-2022 11:47:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [IDS].[usp_Reins_Payment_Processing_ACE] @ProcessLogID int
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

 DECLARE @InsertRowCount Int = 0
 DECLARE @UpdateRowCount Int = 0
 DECLARE @DeleteRowCount int = 0
 DECLARE @return_value   int = 0

    -- Insert statements for procedure here
	
	
BEGIN TRY

Begin Tran T1


-- Process ACE Payments
set @return_value = 0
EXEC	@return_value = [IDS].usp_Reins_ACE_Identify_Payments

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Update EDW and reins percent data from Claim listing
set @return_value = 0
EXEC	@return_value = IDS.usp_Reins_ACE_Claim_Enrich_Rule1

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Calculate Reins and Residual Amounts
set @return_value = 0
Exec @return_value = [IDS].[usp_Reins_Layer_calcs]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Set Progress to Recovery Flag
set @return_value = 0
exec @return_value = IDS.usp_Reins_Progress_To_Recovery_Rule

set @UpdateRowCount = @UpdateRowCount + @return_value

Commit TRAN T1

	select @InsertRowCount InsertRowCount,@UpdateRowCount UpdateRowCount,@DeleteRowCount DeleteRowCount

END TRY

BEGIN CATCH
	
	IF @@TRANCOUNT > 0
	ROLLBACK TRAN T1;

	select @InsertRowCount InsertRowCount,@UpdateRowCount UpdateRowCount,@DeleteRowCount DeleteRowCount;

	THROW;

END CATCH

END


