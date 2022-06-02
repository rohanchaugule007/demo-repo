USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Process_ACE_Data_into_Reins]    Script Date: 02-06-2022 11:15:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [IDS].[usp_Process_ACE_Data_into_Reins] @ProcessLogID int
	
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


-- Load data into Reins ACE Trnx

INSERT INTO [IDS].[Reins_Able_Claim_Expense_Trnx]
           ([Month_Effective_Date]
           ,[Able_Claim_Expense_Trnx_Id]
           ,[Claim_Number]
           ,[Policy_Number]
           ,[Transaction_Type]
           ,[Transaction_Amount]
           ,[Transaction_Effective_Date]
           ,[Transaction_Process_Date]
           ,[Claim_Expense_Id]
           ,[Business_Unit_Id]
           ,[Account_Id]
           ,[Department_Id]
           ,[Payee]
           ,[Invoice_Type]
           ,[Invoice_Number]
           ,[Amount_inc_GST]
           ,[GST]
           ,[Payment_Method]
           ,[Vendor_Id]
           ,[Admin_Initials]
           ,[Payment_Creation_Date]
           ,[Payment_Reference]
           ,[Authorised_By]
           ,[Authorisation_Date]
           ,[GUID]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_flag]
           ,[Exclude_Reason])
    SELECT 
	  A.Month_Eftv_Date,
	  'AB'+cast(A.[Able_Claim_Expense_Trnx_Id] as varchar(18)) as [Able_Claim_Expense_Trnx_Id],
	  A.Claim_Number,
	  NULL as Policy_Number,
	  NULL as Transaction_Type,
	  A.Amount_inc_GST as Transaction_Amount,
	  A.Payment_Creation_Date as Transaction_Effective_Date,
	  A.Authorisation_Date as Transaction_Process_Date,
	  NULL as Claim_Expense_Id,
	  NULL as Business_Unit_Id,
	  NULL as Account_Id,
	  NULL as Department_Id,
	  A.Payee,
	  A.Invoice_Type,
	  A.Invoice_Number,
	  A.Amount_Inc_GST,
	  A.GST,
	  A.Payment_Method,
	  A.Vendor_Id,
	  A.Admin_Initials,
	  A.Payment_Creation_Date,
	  A.Payment_Reference,
	  A.Authorised_By,
	  A.Authorisation_Date,
	  A.[GUID],
	  getdate(),
	  SYSTEM_USER,
	  NULL,
	  NULL,
	  @ProcessLogID,
	  NULL,
	  NULL
FROM [IDS].[Able_Claim_Expense_Trnx] A
  Where NOT EXISTS (Select *  from IDS.Reins_Able_Claim_Expense_Trnx B 
                              Where B.[Able_Claim_Expense_Trnx_Id] = 'AB'+cast(A.[Able_Claim_Expense_Trnx_Id] as varchar) )
AND Exclude_Flag = 'N'

set @insertRowCount = @insertRowCount + @@Rowcount
--print 'Insert into [Reins_Able_Claim_Expense_Trnx] transactions frm [Able_Claim_Expense_Trnx]'

-- Update Trnx type, acct dept BU

Update IDS.Reins_Able_Claim_Expense_Trnx
SET Transaction_Type = Case when Invoice_Type = 'Medical' then 'Medical' else 'Non-Medical' END,
Business_Unit_Id = 'A023',
Account_Id = Case when Invoice_Type = 'Medical' then '725373' Else '731981' END,
Department_Id = 'FC141'
Where Transaction_type is null

set @UpdateRowCount = @UpdateRowCount + @@Rowcount

-- Update exclude flag
/*
Update Reins_Able_Claim_Expense_Trnx
Set Exclude_Flag = 'Y',
Exclude_Reason = 'Product or Product_system out of scope'
from Reins_Able_Claim_Expense_Trnx A
where not exists (Select * from IDS.Reins_Claim_Listing B, dbo.Reins_Treaty_Product C
     Where B.EDW_Product_System_Code = C.Product_System_Code
     and B.EDW_Product_Code = C.Product_Code
     AND B.EDW_Class_Of_Business = C.Class_Of_Business
     and A.Claim_Number = B.Claim_Number
     )
and exclude_flag is null
*/ ------------------------------------------------------------------------------------Dependency on dbo.Reins_Claim_Listing and dbo.Reins_Treaty_Product
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

Update IDS.Reins_Able_Claim_Expense_Trnx
Set Exclude_Flag = 'N'
where exclude_flag is null

set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

-- Update Policy_Number from Reins Claim listing
SET @return_value = 0
exec @return_value = [IDS].[usp_Reins_ACE_Policy_LKP] 

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

-- Exec [IDS].[usp_Process_ACE_Data_into_Reins] 1




