USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Process_Merc_Data_into_Reins]    Script Date: 02-06-2022 11:15:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [IDS].[usp_Process_Merc_Data_into_Reins] @ProcessLogID int
	
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

-- Load U2 Data into Reins Merc Trnx

INSERT INTO [IDS].[Reins_Mercury_Trnx]
           ([Month_Effective_Date]
           ,[Mercury_Trnx_Id]
           ,[Product_System_Code]
           ,[Policy_Number]
           ,[EDW_Contract_Key]
           ,[EDW_Policy_Number]
           ,[EDW_Contract_Id]
           ,[EDW_Product_Key]
           ,[EDW_Product_Code]
           ,[EDW_Class_Of_Business]
           ,[Reins_Benefit_Category]
           ,[Implicit_Benefit_Type_Code]
           ,[Claim_Reference_Id]
           ,[Source_Claim_Number]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_Id]
           ,[Trace_Code]
           ,[Admin_System_File_Id]
           ,[Admin_System_Id]
           ,[File_Record_Number]
           ,[GL_Posting_Date]
           ,[Stat_Fund]
           ,[Sequence_Number]
           ,[Primary_Secondary_Ind]
           ,[Client_Ref_Id_1]
           ,[Client_Ref_Id_2]
           ,[Client_Ref_Id_3]
           ,[Client_Ref_Id_4]
           ,[Trans_Ref_Key_2]
           ,[Trans_Ref_Key_3]
           ,[Trans_Ref_Key_4]
           ,[Trans_Ref_Key_5]
           ,[Trans_Ref_Key_6]
           ,[GL_Business_Unit_Id]
           ,[GL_Account_Id]
           ,[GL_Department_Id]
           ,[GL_Product_Code]
           ,[GL_Project_Id]
           ,[GL_Affiliate_Id]
           ,[GL_Posting_Ref_Text]
           ,[Ledger_Movement_Amount]
           ,[GL_Posting_File_Id]
           ,[Trans_Ref_Key_1]
           ,[Client_Ref_Id_5]
           ,[Product_Ref_Key_1]
           ,[Product_Ref_Key_2]
           ,[Product_Ref_Key_3]
           ,[Bus_Line_Ref_Key_1]
           ,[Bus_Line_Ref_Key_2]
           ,[Bus_Line_Ref_Key_3]
           ,[Business_Line_Location]
           ,[Business_Line_Planner_Id]
           ,[Business_Tax_Classn]
           ,[Stat_Fund_Code]
           ,[Investment_Pool_Id]
           ,[Off_Investment_Pool_Id]
           ,[Investment_Effective_Date]
           ,[Movement_Effective_Date]
           ,[GL_Currency_Code]
           ,[Trans_Ref_Key_7]
           ,[Trans_Ref_Key_8]
           ,[Trans_Ref_Key_9]
           ,[Trans_Ref_Key_10]
           ,[Trans_Ref_Key_11]
           ,[Business_Product_Line_Code]
           ,[Business_Product_Owner_Code]
           ,[Business_Product_Code]
           ,[Business_Line_Code]
           ,[Business_Trans_Type]
           ,[Source_Sys_Reference_1]
           ,[Source_Sys_Narrative_1]
           ,[Source_Sys_Reference_2]
           ,[Source_Sys_Narrative_2]
           ,[Admin_File_Ref_Text]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_Flag]
           ,[Exclude_Reason])
    select 
		A.Month_Effective_Date,
		cast(('MR' + cast(A.Mercury_Trnx_Id as varchar(18))) as varchar(20)) as Mercury_Trnx_Id,
		A.Product_System_Code,
		A.Policy_Number,
		A.EDW_Contract_Key,
		A.EDW_Policy_Number,
		A.EDW_Contract_Id,
		A.EDW_Product_Key,
		A.EDW_Product_Code,
		A.EDW_Class_Of_Business,
		NULL,
		(CAST((case when A.Trans_Ref_Key_2 = 'JNM' then A.Trans_Ref_Key_4 else
		  (case when A.Trans_Ref_Key_2 = 'CLM' then A.Trans_Ref_Key_5 else 'Unknown' end) end) as varchar(20))) as Implicit_Benefit_Type_Code,
		  cast (A.Client_Ref_Id_5 as varchar(15)) as Claim_Reference_Id,
		  NUll,
		  A.Movement_Effective_Date,
		  A.Transaction_Process_Date,
		  A.Transaction_Amount,
		  NULL,
		  NULL,
		  A.Trace_Code,
		  A.Admin_System_File_Id,
		  A.Admin_System_Id,
		  A.File_Record_number,
		  A.GL_Posting_Date,
		  A.Stat_Fund,
		  A.Sequence_Number,
		  A.Primary_Secondary_Ind,
		  A.Client_Ref_id_1,
		  A.Client_Ref_Id_2,
		  A.Client_Ref_Id_3,
		  A.Client_Ref_Id_4,
		  A.Trans_Ref_Key_2,
		  A.Trans_Ref_Key_3,
		  A.Trans_Ref_Key_4,
		  A.Trans_Ref_Key_5,
		  A.Trans_Ref_Key_6,
		  A.GL_Business_Unit_Id,
		  A.GL_Account_Id,
		  A.GL_Department_Id,
		  A.GL_Product_Code,
		  A.gl_Project_id,
		  A.GL_Affiliate_Id,
		  A.GL_Posting_Ref_Text,
		  A.Ledger_Movement_Amount,
		  A.GL_Posting_File_Id,
		  A.Trans_Ref_Key_1,
		  A.Client_Ref_Id_5,
		  A.Product_Ref_Key_1,
		  A.Product_Ref_Key_2,
		  A.Product_Ref_Key_3,
		  A.Bus_Line_Ref_Key_1,
		  A.Bus_Line_Ref_Key_2,
		  A.Bus_Line_Ref_Key_3,
		  A.business_Line_location,
		  A.Business_Line_Planner_Id,
		  A.Business_Tax_Classn,
		  A.Stat_Fund_Code,
		  A.Investment_Pool_Id,
		  A.Off_Investment_Pool_Id,
		  A.Investment_Effective_Date,
		  A.Movement_Effective_Date,
		  A.GL_Currency_Code,
		  A.Trans_Ref_Key_7,
		  A.Trans_Ref_Key_8,
		  A.Trans_Ref_Key_9,
		  A.Trans_Ref_Key_10,
		  A.Trans_Ref_Key_11,
		  A.Business_Product_Line_Code,
		  A.Business_Product_Owner_Code,
		  A.business_product_code,
		  A.Business_Line_Code,
		  A.Business_Trans_Type,
		  A.Source_Sys_Reference_1,
		  A.Source_Sys_Narrative_1,
		  A.Source_Sys_Reference_2,
		  A.Source_Sys_Narrative_2,
		  A.Admin_File_Ref_Text,
		  getdate(),
		  SYSTEM_USER,
		  NULL,
		  NULL,
		  @ProcessLogID,
		 NULL,
		 NULL
	from [IDS].[Mercury_Trnx] A
  where Product_System_Code = 'U2'
   and Policy_number is Not NULL
AND
  NOT EXISTS (Select *  from [IDS].Reins_Mercury_Trnx B 
				Where 'MR'+ cast(A.Mercury_trnx_id as varchar (18)) = B.Mercury_Trnx_id)

set @insertRowCount = @insertRowCount + @@Rowcount


-- Load FDA Data into Reins Merc Trnx

INSERT INTO [IDS].[Reins_Mercury_Trnx]
           ([Month_Effective_Date]
           ,[Mercury_Trnx_Id]
           ,[Product_System_Code]
           ,[Policy_Number]
           ,[EDW_Contract_Key]
           ,[EDW_Policy_Number]
           ,[EDW_Contract_Id]
           ,[EDW_Product_Key]
           ,[EDW_Product_Code]
           ,[EDW_Class_Of_Business]
           ,[Reins_Benefit_Category]
           ,[Implicit_Benefit_Type_Code]
           ,[Claim_Reference_Id]
           ,[Source_Claim_Number]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_Id]
           ,[Trace_Code]
           ,[Admin_System_File_Id]
           ,[Admin_System_Id]
           ,[File_Record_Number]
           ,[GL_Posting_Date]
           ,[Stat_Fund]
           ,[Sequence_Number]
           ,[Primary_Secondary_Ind]
           ,[Client_Ref_Id_1]
           ,[Client_Ref_Id_2]
           ,[Client_Ref_Id_3]
           ,[Client_Ref_Id_4]
           ,[Trans_Ref_Key_2]
           ,[Trans_Ref_Key_3]
           ,[Trans_Ref_Key_4]
           ,[Trans_Ref_Key_5]
           ,[Trans_Ref_Key_6]
           ,[GL_Business_Unit_Id]
           ,[GL_Account_Id]
           ,[GL_Department_Id]
           ,[GL_Product_Code]
           ,[GL_Project_Id]
           ,[GL_Affiliate_Id]
           ,[GL_Posting_Ref_Text]
           ,[Ledger_Movement_Amount]
           ,[GL_Posting_File_Id]
           ,[Trans_Ref_Key_1]
           ,[Client_Ref_Id_5]
           ,[Product_Ref_Key_1]
           ,[Product_Ref_Key_2]
           ,[Product_Ref_Key_3]
           ,[Bus_Line_Ref_Key_1]
           ,[Bus_Line_Ref_Key_2]
           ,[Bus_Line_Ref_Key_3]
           ,[Business_Line_Location]
           ,[Business_Line_Planner_Id]
           ,[Business_Tax_Classn]
           ,[Stat_Fund_Code]
           ,[Investment_Pool_Id]
           ,[Off_Investment_Pool_Id]
           ,[Investment_Effective_Date]
           ,[Movement_Effective_Date]
           ,[GL_Currency_Code]
           ,[Trans_Ref_Key_7]
           ,[Trans_Ref_Key_8]
           ,[Trans_Ref_Key_9]
           ,[Trans_Ref_Key_10]
           ,[Trans_Ref_Key_11]
           ,[Business_Product_Line_Code]
           ,[Business_Product_Owner_Code]
           ,[Business_Product_Code]
           ,[Business_Line_Code]
           ,[Business_Trans_Type]
           ,[Source_Sys_Reference_1]
           ,[Source_Sys_Narrative_1]
           ,[Source_Sys_Reference_2]
           ,[Source_Sys_Narrative_2]
           ,[Admin_File_Ref_Text]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_Flag]
           ,[Exclude_Reason])
    select 
		A.Month_Effective_Date,
		cast(('MR' + cast(A.Mercury_Trnx_Id as varchar(10))) as varchar(20)) as Mercury_Trnx_Id,
		A.Product_System_Code,
		A.Policy_Number,
		A.EDW_Contract_Key,
		A.EDW_Policy_Number,
		A.EDW_Contract_Id,
		A.EDW_Product_Key,
		A.EDW_Product_Code,
		A.EDW_Class_Of_Business,
		NULL,
		NUll as Implicit_Benefit_Type_Code,
		  cast (A.Client_Ref_Id_1 as varchar(15)) as Claim_Reference_Id,
		  cast (LEFT(A.Client_Ref_Id_1,8) as varchar(20)) AS Source_Claim_Number,
		  A.Movement_Effective_Date,
		  A.Transaction_Process_Date,
		  A.Transaction_Amount,
		  NULL,
		  NULL,
		  A.Trace_Code,
		  A.Admin_System_File_Id,
		  A.Admin_System_Id,
		  A.File_Record_number,
		  A.GL_Posting_Date,
		  A.Stat_Fund,
		  A.Sequence_Number,
		  A.Primary_Secondary_Ind,
		  A.Client_Ref_id_1,
		  A.Client_Ref_Id_2,
		  A.Client_Ref_Id_3,
		  A.Client_Ref_Id_4,
		  A.Trans_Ref_Key_2,
		  A.Trans_Ref_Key_3,
		  A.Trans_Ref_Key_4,
		  A.Trans_Ref_Key_5,
		  A.Trans_Ref_Key_6,
		  A.GL_Business_Unit_Id,
		  A.GL_Account_Id,
		  A.GL_Department_Id,
		  A.GL_Product_Code,
		  A.gl_Project_id,
		  A.GL_Affiliate_Id,
		  A.GL_Posting_Ref_Text,
		  A.Ledger_Movement_Amount,
		  A.GL_Posting_File_Id,
		  A.Trans_Ref_Key_1,
		  A.Client_Ref_Id_5,
		  A.Product_Ref_Key_1,
		  A.Product_Ref_Key_2,
		  A.Product_Ref_Key_3,
		  A.Bus_Line_Ref_Key_1,
		  A.Bus_Line_Ref_Key_2,
		  A.Bus_Line_Ref_Key_3,
		  A.business_Line_location,
		  A.Business_Line_Planner_Id,
		  A.Business_Tax_Classn,
		  A.Stat_Fund_Code,
		  A.Investment_Pool_Id,
		  A.Off_Investment_Pool_Id,
		  A.Investment_Effective_Date,
		  A.Movement_Effective_Date,
		  A.GL_Currency_Code,
		  A.Trans_Ref_Key_7,
		  A.Trans_Ref_Key_8,
		  A.Trans_Ref_Key_9,
		  A.Trans_Ref_Key_10,
		  A.Trans_Ref_Key_11,
		  A.Business_Product_Line_Code,
		  A.Business_Product_Owner_Code,
		  A.business_product_code,
		  A.Business_Line_Code,
		  A.Business_Trans_Type,
		  A.Source_Sys_Reference_1,
		  A.Source_Sys_Narrative_1,
		  A.Source_Sys_Reference_2,
		  A.Source_Sys_Narrative_2,
		  A.Admin_File_Ref_Text,
		  getdate() as Created_Date_Time,
		  SYSTEM_USER as Created_By,
		  NULL as Updated_Date_Time ,
		  NULL as Updated_By,
		  @ProcessLogID as Process_Log_ID,
		  'N',       ------------------------ Pass NULL - Changed by KP
		  NULL       ------------------------ Pass NULL - Changed by KP
		 
	from [IDS].[Mercury_Trnx] A
  where Product_System_Code = 'FDA'
AND
  NOT EXISTS (Select *  from [IDS].Reins_Mercury_Trnx B 
				Where 'MR'+ cast(A.Mercury_trnx_id as varchar (18)) = B.Mercury_Trnx_id)

set @InsertRowCount = @InsertRowCount + @@Rowcount


-- Load Incom Data into Reins Merc Trnx


INSERT INTO [IDS].[Reins_Mercury_Trnx]
           ([Month_Effective_Date]
           ,[Mercury_Trnx_Id]
           ,[Product_System_Code]
           ,[Policy_Number]
           ,[EDW_Contract_Key]
           ,[EDW_Policy_Number]
           ,[EDW_Contract_Id]
           ,[EDW_Product_Key]
           ,[EDW_Product_Code]
           ,[EDW_Class_Of_Business]
           ,[Reins_Benefit_Category]
           ,[Implicit_Benefit_Type_Code]
           ,[Claim_Reference_Id]
           ,[Source_Claim_Number]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_Id]
           ,[Trace_Code]
           ,[Admin_System_File_Id]
           ,[Admin_System_Id]
           ,[File_Record_Number]
           ,[GL_Posting_Date]
           ,[Stat_Fund]
           ,[Sequence_Number]
           ,[Primary_Secondary_Ind]
           ,[Client_Ref_Id_1]
           ,[Client_Ref_Id_2]
           ,[Client_Ref_Id_3]
           ,[Client_Ref_Id_4]
           ,[Trans_Ref_Key_2]
           ,[Trans_Ref_Key_3]
           ,[Trans_Ref_Key_4]
           ,[Trans_Ref_Key_5]
           ,[Trans_Ref_Key_6]
           ,[GL_Business_Unit_Id]
           ,[GL_Account_Id]
           ,[GL_Department_Id]
           ,[GL_Product_Code]
           ,[GL_Project_Id]
           ,[GL_Affiliate_Id]
           ,[GL_Posting_Ref_Text]
           ,[Ledger_Movement_Amount]
           ,[GL_Posting_File_Id]
           ,[Trans_Ref_Key_1]
           ,[Client_Ref_Id_5]
           ,[Product_Ref_Key_1]
           ,[Product_Ref_Key_2]
           ,[Product_Ref_Key_3]
           ,[Bus_Line_Ref_Key_1]
           ,[Bus_Line_Ref_Key_2]
           ,[Bus_Line_Ref_Key_3]
           ,[Business_Line_Location]
           ,[Business_Line_Planner_Id]
           ,[Business_Tax_Classn]
           ,[Stat_Fund_Code]
           ,[Investment_Pool_Id]
           ,[Off_Investment_Pool_Id]
           ,[Investment_Effective_Date]
           ,[Movement_Effective_Date]
           ,[GL_Currency_Code]
           ,[Trans_Ref_Key_7]
           ,[Trans_Ref_Key_8]
           ,[Trans_Ref_Key_9]
           ,[Trans_Ref_Key_10]
           ,[Trans_Ref_Key_11]
           ,[Business_Product_Line_Code]
           ,[Business_Product_Owner_Code]
           ,[Business_Product_Code]
           ,[Business_Line_Code]
           ,[Business_Trans_Type]
           ,[Source_Sys_Reference_1]
           ,[Source_Sys_Narrative_1]
           ,[Source_Sys_Reference_2]
           ,[Source_Sys_Narrative_2]
           ,[Admin_File_Ref_Text]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_Flag]
           ,[Exclude_Reason])
    select 
		A.Month_Effective_Date,
		cast(('MR' + cast(A.Mercury_Trnx_Id as varchar(10))) as varchar(20)) as Mercury_Trnx_Id,
		A.Product_System_Code,
		A.Policy_Number,
		A.EDW_Contract_Key,
		A.EDW_Policy_Number,
		A.EDW_Contract_Id,
		A.EDW_Product_Key,
		A.EDW_Product_Code,
		A.EDW_Class_Of_Business,
		NULL,
		NUll as Implicit_Benefit_Type_Code,
		  NULL Claim_Reference_Id,
		  NULL AS Source_Claim_Number,
		  A.Movement_Effective_Date,
		  A.Transaction_Process_Date,
		  A.Transaction_Amount,
		  NULL,
		  NULL,
		  A.Trace_Code,
		  A.Admin_System_File_Id,
		  A.Admin_System_Id,
		  A.File_Record_number,
		  A.GL_Posting_Date,
		  A.Stat_Fund,
		  A.Sequence_Number,
		  A.Primary_Secondary_Ind,
		  A.Client_Ref_id_1,
		  A.Client_Ref_Id_2,
		  A.Client_Ref_Id_3,
		  A.Client_Ref_Id_4,
		  A.Trans_Ref_Key_2,
		  A.Trans_Ref_Key_3,
		  A.Trans_Ref_Key_4,
		  A.Trans_Ref_Key_5,
		  A.Trans_Ref_Key_6,
		  A.GL_Business_Unit_Id,
		  A.GL_Account_Id,
		  A.GL_Department_Id,
		  A.GL_Product_Code,
		  A.gl_Project_id,
		  A.GL_Affiliate_Id,
		  A.GL_Posting_Ref_Text,
		  A.Ledger_Movement_Amount,
		  A.GL_Posting_File_Id,
		  A.Trans_Ref_Key_1,
		  A.Client_Ref_Id_5,
		  A.Product_Ref_Key_1,
		  A.Product_Ref_Key_2,
		  A.Product_Ref_Key_3,
		  A.Bus_Line_Ref_Key_1,
		  A.Bus_Line_Ref_Key_2,
		  A.Bus_Line_Ref_Key_3,
		  A.business_Line_location,
		  A.Business_Line_Planner_Id,
		  A.Business_Tax_Classn,
		  A.Stat_Fund_Code,
		  A.Investment_Pool_Id,
		  A.Off_Investment_Pool_Id,
		  A.Investment_Effective_Date,
		  A.Movement_Effective_Date,
		  A.GL_Currency_Code,
		  A.Trans_Ref_Key_7,
		  A.Trans_Ref_Key_8,
		  A.Trans_Ref_Key_9,
		  A.Trans_Ref_Key_10,
		  A.Trans_Ref_Key_11,
		  A.Business_Product_Line_Code,
		  A.Business_Product_Owner_Code,
		  A.business_product_code,
		  A.Business_Line_Code,
		  A.Business_Trans_Type,
		  A.Source_Sys_Reference_1,
		  A.Source_Sys_Narrative_1,
		  A.Source_Sys_Reference_2,
		  A.Source_Sys_Narrative_2,
		  A.Admin_File_Ref_Text,
		  getdate(),
		  SYSTEM_USER,
		  NULL,
		  NULL,
		  @ProcessLogID,
		 NULL,
		 NULL
	from [IDS].[Mercury_Trnx] A
  where Product_System_Code = 'OR'
AND
  NOT EXISTS (Select *  from [IDS].Reins_Mercury_Trnx B 
				Where 'MR'+ cast(A.Mercury_trnx_id as varchar (18)) = B.Mercury_Trnx_id)

set @InsertRowCount = @InsertRowCount + @@Rowcount


-- Load Ultimate Data into Reins Merc Trnx



INSERT INTO [IDS].[Reins_Mercury_Trnx]
           ([Month_Effective_Date]
           ,[Mercury_Trnx_Id]
           ,[Product_System_Code]
           ,[Policy_Number]
           ,[EDW_Contract_Key]
           ,[EDW_Policy_Number]
           ,[EDW_Contract_Id]
           ,[EDW_Product_Key]
           ,[EDW_Product_Code]
           ,[EDW_Class_Of_Business]
           ,[Reins_Benefit_Category]
           ,[Implicit_Benefit_Type_Code]
           ,[Claim_Reference_Id]
           ,[Source_Claim_Number]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_Id]
           ,[Trace_Code]
           ,[Admin_System_File_Id]
           ,[Admin_System_Id]
           ,[File_Record_Number]
           ,[GL_Posting_Date]
           ,[Stat_Fund]
           ,[Sequence_Number]
           ,[Primary_Secondary_Ind]
           ,[Client_Ref_Id_1]
           ,[Client_Ref_Id_2]
           ,[Client_Ref_Id_3]
           ,[Client_Ref_Id_4]
           ,[Trans_Ref_Key_2]
           ,[Trans_Ref_Key_3]
           ,[Trans_Ref_Key_4]
           ,[Trans_Ref_Key_5]
           ,[Trans_Ref_Key_6]
           ,[GL_Business_Unit_Id]
           ,[GL_Account_Id]
           ,[GL_Department_Id]
           ,[GL_Product_Code]
           ,[GL_Project_Id]
           ,[GL_Affiliate_Id]
           ,[GL_Posting_Ref_Text]
           ,[Ledger_Movement_Amount]
           ,[GL_Posting_File_Id]
           ,[Trans_Ref_Key_1]
           ,[Client_Ref_Id_5]
           ,[Product_Ref_Key_1]
           ,[Product_Ref_Key_2]
           ,[Product_Ref_Key_3]
           ,[Bus_Line_Ref_Key_1]
           ,[Bus_Line_Ref_Key_2]
           ,[Bus_Line_Ref_Key_3]
           ,[Business_Line_Location]
           ,[Business_Line_Planner_Id]
           ,[Business_Tax_Classn]
           ,[Stat_Fund_Code]
           ,[Investment_Pool_Id]
           ,[Off_Investment_Pool_Id]
           ,[Investment_Effective_Date]
           ,[Movement_Effective_Date]
           ,[GL_Currency_Code]
           ,[Trans_Ref_Key_7]
           ,[Trans_Ref_Key_8]
           ,[Trans_Ref_Key_9]
           ,[Trans_Ref_Key_10]
           ,[Trans_Ref_Key_11]
           ,[Business_Product_Line_Code]
           ,[Business_Product_Owner_Code]
           ,[Business_Product_Code]
           ,[Business_Line_Code]
           ,[Business_Trans_Type]
           ,[Source_Sys_Reference_1]
           ,[Source_Sys_Narrative_1]
           ,[Source_Sys_Reference_2]
           ,[Source_Sys_Narrative_2]
           ,[Admin_File_Ref_Text]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_Flag]
           ,[Exclude_Reason])
    select 
		A.Month_Effective_Date,
		cast(('MR' + cast(A.Mercury_Trnx_Id as varchar(10))) as varchar(20)) as Mercury_Trnx_Id,
		A.Product_System_Code,
		A.Policy_Number,
		A.EDW_Contract_Key,
		A.EDW_Policy_Number,
		A.EDW_Contract_Id,
		A.EDW_Product_Key,
		A.EDW_Product_Code,
		A.EDW_Class_Of_Business,
		NULL,
		NUll as Implicit_Benefit_Type_Code,
		  NULL Claim_Reference_Id,
		  NULL AS Source_Claim_Number,
		  A.Movement_Effective_Date,
		  A.Transaction_Process_Date,
		  A.Transaction_Amount,
		  NULL,
		  NULL,
		  A.Trace_Code,
		  A.Admin_System_File_Id,
		  A.Admin_System_Id,
		  A.File_Record_number,
		  A.GL_Posting_Date,
		  A.Stat_Fund,
		  A.Sequence_Number,
		  A.Primary_Secondary_Ind,
		  A.Client_Ref_id_1,
		  A.Client_Ref_Id_2,
		  A.Client_Ref_Id_3,
		  A.Client_Ref_Id_4,
		  A.Trans_Ref_Key_2,
		  A.Trans_Ref_Key_3,
		  A.Trans_Ref_Key_4,
		  A.Trans_Ref_Key_5,
		  A.Trans_Ref_Key_6,
		  A.GL_Business_Unit_Id,
		  A.GL_Account_Id,
		  A.GL_Department_Id,
		  A.GL_Product_Code,
		  A.gl_Project_id,
		  A.GL_Affiliate_Id,
		  A.GL_Posting_Ref_Text,
		  A.Ledger_Movement_Amount,
		  A.GL_Posting_File_Id,
		  A.Trans_Ref_Key_1,
		  A.Client_Ref_Id_5,
		  A.Product_Ref_Key_1,
		  A.Product_Ref_Key_2,
		  A.Product_Ref_Key_3,
		  A.Bus_Line_Ref_Key_1,
		  A.Bus_Line_Ref_Key_2,
		  A.Bus_Line_Ref_Key_3,
		  A.business_Line_location,
		  A.Business_Line_Planner_Id,
		  A.Business_Tax_Classn,
		  A.Stat_Fund_Code,
		  A.Investment_Pool_Id,
		  A.Off_Investment_Pool_Id,
		  A.Investment_Effective_Date,
		  A.Movement_Effective_Date,
		  A.GL_Currency_Code,
		  A.Trans_Ref_Key_7,
		  A.Trans_Ref_Key_8,
		  A.Trans_Ref_Key_9,
		  A.Trans_Ref_Key_10,
		  A.Trans_Ref_Key_11,
		  A.Business_Product_Line_Code,
		  A.Business_Product_Owner_Code,
		  A.business_product_code,
		  A.Business_Line_Code,
		  A.Business_Trans_Type,
		  A.Source_Sys_Reference_1,
		  A.Source_Sys_Narrative_1,
		  A.Source_Sys_Reference_2,
		  A.Source_Sys_Narrative_2,
		  A.Admin_File_Ref_Text,
		  getdate(),
		  SYSTEM_USER,
		  NULL,
		  NULL,
		  @ProcessLogID,
		 NULL,
		 NULL
	from [IDS].[Mercury_Trnx] A
  where Product_System_Code = 'CP'
AND
  NOT EXISTS (Select *  from [IDS].Reins_Mercury_Trnx B 
				Where 'MR'+ cast(A.Mercury_trnx_id as varchar (18)) = B.Mercury_Trnx_id)


INSERT INTO [IDS].[Reins_Claims_Error_Log]
           ([Process_log_Id]
           ,[ProcedureName]
           ,[Error_Column_Identifier]
           ,[Error_Reason]
           ,[CreatedDate])     
Select 
@ProcessLogID as [Process_log_Id],
'usp_Process_Merc_Data_into_Reins' as [ProcedureName],
  A.Mercury_Trnx_Id as [Error_Column_Identifier],
 'Policy_Number is Null in Source. Cannot insert Null policy number in Reins table' as [Error_Reason],
 getdate() as [CreatedDate] 
 from [IDS].[Mercury_Trnx] A
where Product_System_Code in ('U2','FDA','CP','OR')
AND
NOT EXISTS (Select *  from [IDS].Reins_Mercury_Trnx B 
			Where 'MR'+ cast(A.Mercury_trnx_id as varchar (18)) = B.Mercury_Trnx_id)

set @InsertRowCount = @InsertRowCount + @@Rowcount


--Update Payment Item Type Lt 0

UPDATE 
    [IDS].[Reins_Mercury_Trnx]
SET
   Transaction_Type = [Trnx_Type_LT_0],
Updated_by = System_user,
Updated_date_time = getdate()
FROM 
    [IDS].[Reins_Claim_Expense_Trnx_Type] ,
    [IDS].[Reins_Mercury_Trnx]
WHERE
    [IDS].[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_Mercury_Trnx].GL_ACCOUNT_ID
AND     ((([IDS].[Reins_Mercury_Trnx].[Ledger_Movement_Amount])<0))
AND     [IDS].[Reins_Mercury_Trnx].[Transaction_Type] IS NULL 
--AND     [IDS].[Reins_Mercury_Trnx].[Product_System_code] = 'U2'


set @UpdateRowCount = @UpdateRowCount + @@Rowcount

--Update Payment Item Type Gt 0

UPDATE 
     [IDS].[Reins_Mercury_Trnx]
SET
   Transaction_Type = [Trnx_Type_GT_0]
FROM 
     [IDS].[Reins_Claim_Expense_Trnx_Type] ,
    [IDS].[Reins_Mercury_Trnx]
WHERE
    [IDS].[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_Mercury_Trnx].GL_ACCOUNT_ID
AND     ((([IDS].[Reins_Mercury_Trnx].[Ledger_Movement_Amount])>=0))
AND     [IDS].[Reins_Mercury_Trnx].[Transaction_Type] IS NULL
--AND     [IDS].[Reins_Mercury_Trnx].[Product_System_code] = 'U2'
 

set @UpdateRowCount = @UpdateRowCount + @@Rowcount

--Mark out of scope products as Ignore

Exec IDS.[usp_Reins_Exclude_Out_Of_Scope_Trnx_MR]  
set @UpdateRowCount = @UpdateRowCount + @@Rowcount

--Update Reins benefit Cat based on GL Product Mapping

Update Reins_Mercury_Trnx
set [Reins_Benefit_Category] = B.[Reins_Benefit_Type]
from Reins_Mercury_Trnx A, [IDS].[Reins_GL_Product] B
where A.GL_Product_Code = B.GL_Product_Code
and [Reins_Benefit_Category] is null
--and Transaction_Type not like 'Pay%'

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

