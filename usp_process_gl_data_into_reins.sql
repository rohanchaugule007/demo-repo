USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Process_GL_Data_into_Reins]    Script Date: 02-06-2022 11:15:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [IDS].[usp_Process_GL_Data_into_Reins] @ProcessLogID int
	
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


-- Load data into Reins GL JNL Trnx
INSERT INTO [IDS].[Reins_GL_Journal_Trnx]
           ([GL_Journal_Trnx_Id]
           ,[Month_Effective_Date]
           ,[Policy_Number]
           ,[EDW_Product_System_Code]
           ,[EDW_Contract_Key]
           ,[EDW_Product_Code]
           ,[EDW_Product_Key]
           ,[EDW_Product_Option_Code]
           ,[EDW_Product_Option_Key]
           ,[EDW_Contract_Id]
           ,[EDW_Policy_Number]
           ,[EDW_Product_Name]
           ,[EDW_Class_of_Business]
           ,[Claim_Number]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_id]
           ,[Implicit_Benefit_Type_Code]
           ,[Reins_Benefit_Category]
           ,[Source_Claim_Number]
           ,[Trace_Code]
           ,[GL_Business_Unit_Id]
           ,[GL_Journal_Id]
           ,[GL_Journal_Date]
           ,[GL_Journal_Line]
           ,[GL_Ledger_Code]
           ,[GL_Account_Id]
           ,[GL_Department_Id]
           ,[GL_Product_Id]
           ,[GL_Project_Id]
           ,[GL_Affiliate_Id]
           ,[GL_Currency_Code]
           ,[GL_Monetary_Amount]
           ,[GL_Posting_Ref_1]
           ,[GL_Journal_Line_Desc]
           ,[Status_Ref_1]
           ,[Source_System_Ref_1]
           ,[Source_System_Ref_2]
           ,[GL_Posting_Ref_2]
           ,[Status_Ref_2]
           ,[GL_Posting_Date]
           ,[User_Id]
           ,[Source_System_Narrative_1]
           ,[GL_Statistic_Amount]
           ,[GL_Foreign_Currency_Code]
           ,[GL_Foreign_Amount]
           ,[GL_Journal_Month]
           ,[Pcode]
           ,[Paid_From_Date]
           ,[Paid_To_Date]
           ,[Created_By]
           ,[Created_Date_Time]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_Id]
           ,[Exclude_Flag]
           ,[Exclude_Reason]
           ,[Policy_Number_Updated_Flg]
           ,[Old_Policy_Number])
     SELECT
	  'GL'+ cast(A.GL_Journal_trnx_id as varchar (18)) as GL_Journal_Trnx_Id,
	   A.Month_Eftv_Date,
       A.Policy_Number,
       A.EDW_Product_System_Code,
	   A.EDW_Contract_Key,
	   A.EDW_Product_Code,
	   A.EDW_Product_Key,
	   NULL as Product_Option_Code,
       NULL as Product_Option_Key,
	   A.EDW_Contract_Id,
	   A.EDW_Policy_Number,
	   A.EDW_Product_Name,
	   A.EDW_Class_of_Business,
	   NULL as Claim_Number,
	   A.GL_Journal_Date as Transaction_Effective_date,
	   A.GL_Posting_Date as Transaction_Process_Date,
	   A.GL_Monetary_Amount as Transaction_Amount,
	   NULL as Transaction_Type,
	   NULL as Claim_Expense_id,
	   NULL as Implicit_Benefit_Type_Code,
	   NULL as Reins_Benefit_Category,
	   NULL as Source_Claim_Number,
	   A.Trace_Code,
	   A.GL_Business_Unit_Id,
	   A.GL_Journal_Id,
	   A.GL_Journal_Date,
	   A.GL_Journal_Line,
	   A.GL_Ledger_Code,
	   A.GL_Account_Id,
	   A.GL_Department_Id,
	   A.GL_Product_Id,
	   A.GL_Project_Id,
	   A.GL_Affiliate_Id,
	   A.GL_Currency_Code,
	   A.GL_Monetary_Amount,
	   A.GL_Posting_Ref_1,
	   A.GL_Journal_Line_Desc,
	   A.Status_Ref_1,
	   A.Source_System_Ref_1,
	   A.Source_System_Ref_2,
	   A.GL_Posting_Ref_2,
	   A.Status_Ref_2,
	   A.GL_Posting_Date,
	   A.[User_Id],
	   A.Source_System_Narrative_1,
	   A.GL_Statistic_Amount,
	   A.GL_Foreign_Currency_Code,
	   A.GL_Foreign_Amount,
	   A.GL_Journal_Month,
	   A.Pcode,
	   A.Paid_From_Date,
	   A.Paid_To_Date,
	   SYSTEM_USER,
	   Getdate(),
	   NULL,
	   NULL,
	   @ProcessLogID,
	   NULL,
	   NULL,
	   NULL,
	   NULL
  FROM [IDS].[GL_Journal_Trnx] A
  Where NOT EXISTS (Select *  from IDS.Reins_GL_Journal_Trnx B 
                              Where 'GL'+ cast(A.GL_Journal_trnx_id as varchar (18)) = B.GL_journal_Trnx_id
)
and ISNULL(Source_System_Ref_2, 'NA') != 'MER'
and ISNULL(Source_System_Ref_2, 'NA') != '151'  

set @insertRowCount = @insertRowCount + @@Rowcount

-- Update Payment Item Type Lt 0
UPDATE 
     [IDS].[Reins_GL_Journal_Trnx] 
SET
   Transaction_Type = [Trnx_Type_LT_0],
   Updated_By = System_User,
   Updated_date_time = getdate()
FROM 
    [IDS].[Reins_Claim_Expense_Trnx_Type] ,
    [IDS].[Reins_GL_Journal_Trnx] 
WHERE
    [IDS].[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_GL_Journal_Trnx] .GL_ACCOUNT_ID
AND     ((([IDS].[Reins_GL_Journal_Trnx] .GL_Monetary_Amount)<0))
AND     [IDS].[Reins_GL_Journal_Trnx] .[Transaction_Type] IS NULL 
--AND     [IDS].[Reins_GL_Journal_Trnx] .[Product_System_code] = 'DS'

set @UpdateRowCount = @UpdateRowCount + @@Rowcount

-- Update Payment Item Type Gt 0
UPDATE 
      [IDS].[Reins_GL_Journal_Trnx] 
SET
   Transaction_Type = [Trnx_Type_GT_0],
   Updated_By = System_User,
   Updated_date_time = getdate()
FROM 
    [IDS].[Reins_Claim_Expense_Trnx_Type] ,
    [IDS].[Reins_GL_Journal_Trnx] 
WHERE
   [IDS].[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_GL_Journal_Trnx] .GL_ACCOUNT_ID
AND      ((([IDS].[Reins_GL_Journal_Trnx] .GL_Monetary_Amount)>=0))
AND     [IDS].[Reins_GL_Journal_Trnx] .[Transaction_Type] IS NULL 
--AND     [IDS].[Reins_GL_Journal_Trnx] .[Product_System_code] = 'DS'

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Exclude Out of Scope Prods
SET @return_value = 0
exec @return_value = [IDS].[usp_Reins_Exclude_Out_Of_Scope_Trnx_GL]  


set @UpdateRowCount = @UpdateRowCount + @return_value

-- Update Journal and processing dates where they are null

UPDATE [IDS].Reins_GL_Journal_Trnx 
SET [IDS].Reins_GL_Journal_Trnx.GL_Posting_Date = GL_Journal_Date
,Transaction_Process_date = GL_Journal_date
WHERE [IDS].Reins_GL_Journal_Trnx.GL_Posting_Date Is Null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Update Reins Benefit Cat from GL Prod Mapping

Update Reins_GL_Journal_Trnx
set [Reins_Benefit_Category] = B.[Reins_Benefit_Type]
from Reins_GL_Journal_Trnx A ,[IDS].[Reins_GL_Product] B 
where A.GL_Product_Id = B.GL_Product_Code  and
 [Reins_Benefit_Category] is null


set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Update correct Trnx Type sql

Update Reins_GL_journal_Trnx
set Transaction_type = 'Expense'
where Source_System_Narrative_1 like 'ABLE Claim accrual%'
and Transaction_type = 'Expense reversal'
and Transaction_amount < 0
and GL_journal_trnx_id not like '%REV'

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT


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



