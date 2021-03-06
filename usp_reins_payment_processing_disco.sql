USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Reins_Payment_Processing_Disco]    Script Date: 02-06-2022 11:48:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [IDS].[usp_Reins_Payment_Processing_Disco] @ProcessLogID int
	
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

--Update Implcit Benefit Type Code

Update  IDS.Reins_GL_Journal_Trnx
SET Implicit_Benefit_Type_Code = B.Benefit_code,
   Updated_By = System_User,
   Updated_date_time = getdate()
from Reins_GL_Journal_Trnx A, 
(
Select GL_Journal_Trnx_Id, 
substring(GL_Journal_Line_Desc, charindex(' ',GL_Journal_Line_Desc)+1,charindex('-',GL_Journal_Line_Desc)-charindex(' ',GL_Journal_Line_Desc)+2) as Benefit_code
--,len(rtrim(substring(GL_Journal_Line_Desc, charindex(' ',GL_Journal_Line_Desc)+1,charindex('-',GL_Journal_Line_Desc)-charindex(' ',GL_Journal_Line_Desc)+3))) as lbf,
--Charindex(' ',substring(GL_Journal_Line_Desc, charindex(' ',GL_Journal_Line_Desc)+1,charindex('-',GL_Journal_Line_Desc)-charindex(' ',GL_Journal_Line_Desc)+2))
 from 
IDS.Reins_GL_Journal_Trnx
where  GL_Journal_Line_Desc like '%|-0%' escape '|' 
and len(substring(GL_Journal_Line_Desc, charindex(' ',GL_Journal_Line_Desc),charindex('-',GL_Journal_Line_Desc)-charindex(' ',GL_Journal_Line_Desc)+3)) <=11
and  EDW_Product_System_Code = 'DS'
) B
where A.GL_Journal_Trnx_Id = B.GL_Journal_Trnx_Id
and EDW_Product_System_Code = 'DS'
and A.Implicit_Benefit_Type_Code is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Update Source Claim Number

Update IDS.Reins_GL_Journal_Trnx
SET Source_Claim_Number = B.Source_Claim_Number,
   Updated_By = System_User,
   Updated_date_time = getdate()
from IDS.Reins_GL_Journal_Trnx A,
(
Select GL_Journal_Trnx_Id ,GL_Journal_Line_Desc,
rtrim(substring(GL_Journal_Line_Desc, charindex(' ',GL_Journal_Line_Desc)+1,charindex(' ',GL_Journal_Line_Desc))) as Source_Claim_Number
from 
IDS.Reins_GL_Journal_Trnx
where not( GL_Journal_Line_Desc like '%|-%' escape '|') 
and charindex(' ',GL_Journal_Line_Desc,(charindex(' ',GL_Journal_Line_Desc)+1)) <=15
) B
where A.GL_Journal_Trnx_Id = B.GL_Journal_Trnx_Id
and EDW_Product_System_Code = 'DS'
and A.Source_Claim_Number is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Identify DS Payments
set @return_value = 0
EXEC	@return_value = [IDS].usp_Reins_DS_Identify_Payments

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Claim Enrich based on Benefit Num

UPDATE IDS.Reins_Claim_Expense_Trnx
SET
 [Product_Option_Code] = IDS.Reins_Claim_Listing.[EDW_Product_Option_Code] ,
 [Claim_Number] = IDS.Reins_Claim_Listing.[Claim_Number],
 [Layer_0_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_0_Reins_Percent],
 [Layer_1_Reins_Percent] = IDS.Reins_Claim_Listing.[Layer_1_Reins_Percent],
 [Layer_1_Treaty_Id] = IDs.Reins_Claim_Listing.[Layer_1_Treaty_Id],
 [Layer_2_Treaty_Id] = IDS.Reins_Claim_Listing.[Layer_2_Treaty_Id], 
 Claim_Source_id = IDS.Reins_Claim_Listing.Claim_Source_id ,
 Layer_2_Reins_Percent = IDS.Reins_Claim_Listing.Layer_2_Reins_Percent,
 [Claim_enrichment_rule] = 'CE_DS_Pol_Ben',
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
AND IDS.Reins_Claim_Listing.[Benefit_Number] = IDS.Reins_Claim_Expense_Trnx.IMPLICIT_BENEFIT_TYPE_CODE
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('3000-12-31' as date))
AND IDS.Reins_Claim_Listing.Row_End_Date = cast('3000-12-31' as date)
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'DS'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'DS'
AND Claim_enrichment_rule is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrich based Src Claim No

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
 [Claim_enrichment_rule] = 'CE_DS_Pol_Ben',
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
AND IDS.Reins_Claim_Listing.[Benefit_Number] = IDS.Reins_Claim_Expense_Trnx.IMPLICIT_BENEFIT_TYPE_CODE
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('3000-12-31' as date))
AND IDS.Reins_Claim_Listing.Row_End_Date = cast('3000-12-31' as date)
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'DS'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'DS'
AND Claim_enrichment_rule is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrich based on Source Claim Number

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
 [Claim_enrichment_rule] = 'CE_DS_Pol_Claim',
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
--AND IDS.Reins_Claim_Listing.[Benefit_Number] = IDS.Reins_Claim_Expense_Trnx.IMPLICIT_BENEFIT_TYPE_CODE
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('3000-12-31' as date))
AND IDS.Reins_Claim_Listing.Row_End_Date =  cast('3000-12-31' as date)
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'DS'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'DS'
AND IDS.Reins_Claim_Expense_Trnx.Claim_enrichment_rule is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrich Pol No

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
 [Claim_enrichment_rule] = 'CE_DS_Pol_Ben',
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
AND IDS.Reins_Claim_Listing.[Benefit_Number] = IDS.Reins_Claim_Expense_Trnx.IMPLICIT_BENEFIT_TYPE_CODE
AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between IDS.Reins_Claim_Listing.Claim_Notify_date and ISNULL(IDS.Reins_Claim_Listing.Claim_Finalised_Date+14, cast('3000-12-31' as date))
AND IDS.Reins_Claim_Listing.Row_End_Date = cast('3000-12-31' as date)
AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'DS'
AND Reins_Claim_Listing.EDW_Product_System_Code = 'DS'
AND Claim_enrichment_rule is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Claim Enrich based on Policy only
set @return_value = 0
EXEC	@return_value = [IDS].[usp_Reins_DS_Claims_Enrich_Rule3]

set @UpdateRowCount = @UpdateRowCount + @return_value

-- Exec Layer Calcs and look for rate changes
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
