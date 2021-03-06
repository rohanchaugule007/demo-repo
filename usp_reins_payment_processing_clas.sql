USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Reins_Payment_Processing_Clas]    Script Date: 02-06-2022 11:48:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Changed by:		<RC>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [IDS].[usp_Reins_Payment_Processing_Clas] @ProcessLogID int
	
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


-- Update Benefit details
UPDATE 
    [IDS].[Reins_Mercury_Trnx]
SET
 --   [Reins_Benefit_Category] = [Reins_Claim_Benefit_Type].[BENEFIT_CATEGORY] ,
    [Implicit_Benefit_Type_Code] =   
          Reins_Claim_Benefit_Type.[IMPLICIT_BENEFIT_TYPE]
FROM 
    [IDS].[Reins_Claim_Benefit_Type] ,
     [IDS].[Reins_Mercury_Trnx]
WHERE
      [Reins_Claim_Benefit_Type].[PRODUCT_SYSTEM_CODE] = 'FDA'
AND [Reins_Claim_Benefit_Type].[SOURCE_BENEFIT_CODE] =   
           [IDS].[Reins_Mercury_Trnx].[Trans_Ref_Key_1]
--AND    [IDS].[Reins_Mercury_Trnx].[Reins_BENEFIT_CATEGORY] IS NULL 
AND [Implicit_Benefit_Type_Code] is null
AND [IMPLICIT_BENEFIT_TYPE] is not null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Identify WOP Payments
set @return_value = 0
EXEC	@return_value = [IDS].[usp_Reins_FDA_Identify_Payments_WOP]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Identify Payments
set @return_value = 0
EXEC	@return_value = [IDS].[usp_Reins_FDA_Identify_Payments]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Claim Enrichment Pri match
UPDATE IDS.Reins_Claim_Expense_Trnx
SET
 [Product_Option_Code] = IDS.Reins_Claim_Listing.[EDW_Product_Option_Code] ,
 [Claim_Number] = IDS.Reins_Claim_Listing.[Claim_Number],
 [Layer_0_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_0_Reins_Percent],
 [Layer_1_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_1_Reins_Percent],
 [Layer_1_Treaty_Id] = IDS.Reins_Claim_Listing.[Layer_1_Treaty_Id],
 [Layer_2_Treaty_Id] = IDS.Reins_Claim_Listing.[Layer_2_Treaty_Id], 
 Claim_Source_id = IDS.Reins_Claim_Listing.Claim_Source_id ,
 Layer_2_Reins_Percent = IDS.Reins_Claim_Listing.Layer_2_Reins_Percent,
 [Claim_enrichment_rule] = 'CE_FDA_Pol_Claim_Ben',
 Updated_by = System_user,
Updated_date_time = getdate()
 FROM 
 IDS.Reins_Claim_Expense , 
 IDS.Reins_Claim_Expense_Trnx ,
 IDS.Reins_Claim_Listing
WHERE 
IDS.Reins_Claim_Expense.Claim_Expense_Id  = IDS.Reins_Claim_Expense_Trnx.Claim_Expense_Id
AND IDS.Reins_Claim_Expense_Trnx.Source_Claim_Number = IDS.Reins_Claim_Listing.Claim_Number
AND IDS.Reins_Claim_Expense_Trnx.EDW_Contract_Key = IDS.Reins_Claim_Listing.EDW_Contract_Key
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'FDA'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'FDA'
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Type in ( 'EXPENSE', 'Expense reversal', 'WOP')
AND Left(IDS.Reins_Claim_Listing.[EDW_Product_Option_Code], len([IDS].Reins_Claim_Expense_Trnx.[IMPLICIT_BENEFIT_TYPE_CODE])) = [IDS].Reins_Claim_Expense_Trnx.[IMPLICIT_BENEFIT_TYPE_CODE]
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('2899-12-31' as date))
AND  IDS.Reins_Claim_Listing.Row_End_Date = '3000-12-31'
AND IDS.Reins_Claim_Expense_Trnx.Claim_enrichment_rule is null
AND IDS.Reins_Claim_Expense_Trnx.[Reins_Benefit_Category] = IDS.Reins_Claim_Listing.[Reins_Benefit_Type]

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrichment IP Able Sec match
UPDATE IDS.Reins_Claim_Expense_Trnx
SET
 [Product_Option_Code] = IDS.Reins_Claim_Listing.[EDW_Product_Option_Code] ,
 [Claim_Number] = IDS.Reins_Claim_Listing.[Claim_Number],
 [Layer_0_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_0_Reins_Percent],
 [Layer_1_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_1_Reins_Percent],
 [Layer_1_Treaty_Id] = IDS.Reins_Claim_Listing.[Layer_1_Treaty_Id],
 [Layer_2_Treaty_Id] = IDS.Reins_Claim_Listing.[Layer_2_Treaty_Id], 
 Claim_Source_id = IDS.Reins_Claim_Listing.Claim_Source_id ,
 Layer_2_Reins_Percent = IDS.Reins_Claim_Listing.Layer_2_Reins_Percent,
 [Claim_enrichment_rule] = 'CE_FDA_Pol_Ben',
 Updated_by = System_user,
Updated_date_time = getdate()
 FROM 
 IDS.Reins_Claim_Expense , 
 IDS.Reins_Claim_Expense_Trnx ,
 IDS.Reins_Claim_Listing
WHERE 
IDS.Reins_Claim_Expense.Claim_Expense_Id  = IDS.Reins_Claim_Expense_Trnx.Claim_Expense_Id
--AND IDS.Reins_Claim_Expense_Trnx.Source_Claim_Number = IDS.Reins_Claim_Listing.Claim_Number
AND IDS.Reins_Claim_Expense_Trnx.EDW_Contract_Key = IDS.Reins_Claim_Listing.EDW_Contract_Key
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'FDA'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'FDA'
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Type in ( 'EXPENSE', 'Expense reversal', 'WOP')
AND Left(IDS.Reins_Claim_Listing.[EDW_Product_Option_Code], len([IDS].Reins_Claim_Expense_Trnx.[IMPLICIT_BENEFIT_TYPE_CODE])) = [IDS].Reins_Claim_Expense_Trnx.[IMPLICIT_BENEFIT_TYPE_CODE]
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('2899-12-31' as date))
AND  IDS.Reins_Claim_Listing.Row_End_Date = '3000-12-31'
AND IDS.Reins_Claim_Expense_Trnx.Claim_enrichment_rule is null
AND IDS.Reins_Claim_Expense_Trnx.[Reins_Benefit_Category] = IDS.Reins_Claim_Listing.[Reins_Benefit_Type]  

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrichment Rule 3
set @return_value = 0
EXEC	@return_value = [IDS].[usp_Reins_FDA_Claims_Enrich_Rule3]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Enrich Pay Date From and To from EDW for IP
set @return_value = 0
EXEC	@return_value = [IDS].[usp_Reins_FDA_EDW_Claims_Enrich]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Update Expense Records from Payment Records

  Update A
  SET PAY_FROM_TO_RULE = B.PAY_FROM_TO_RULE,
  PAY_DATE_FROM = B.PAY_DATE_FROM,
  PAY_DATE_To = B.PAY_DATE_To,
  Pay_From_To_EDW_Event_Num = B.Pay_From_To_EDW_Event_Num
  from [IDS].[Reins_Claim_Expense_Trnx] A, [IDS].[Reins_Claim_Expense_Trnx] B
  where A.Claim_expense_id = B.Claim_expense_id
  and A.Policy_number = B.Policy_number
  and A.Transaction_Process_Date = B.Transaction_Process_Date
  and A.Transaction_type in ( 'Expense', 'WOP')
  and B.Transaction_Type = 'Payment'
  and B.Pay_From_To_Rule is not null
  and (A.Pay_From_To_Rule is null)
  
  set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- usp_Reins_FDA_WOP_From_To_Date
set @return_value = 0
Exec @return_value = [IDS].usp_Reins_FDA_WOP_From_To_Date

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Exec Layer Calcs and look for rate changes
set @return_value = 0
Exec @return_value = [IDS].[usp_Reins_Layer_calcs]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Exclude Excess Payments
set @return_value = 0
Exec @return_value = IDS.usp_Reins_FDA_Exclude_Excess_Payments

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

