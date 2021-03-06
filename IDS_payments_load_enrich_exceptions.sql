USE [IDS_Reinsurance]
GO
/****** Object:  Table [dbo].[Able_Claim_Benefit]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Able_Claim_Benefit](
	[Month_Eftv_Date] [datetime] NOT NULL,
	[Claim_Benefit_Id] [int] IDENTITY(1,1) NOT NULL,
	[Claim_Number] [varchar](30) NOT NULL,
	[Claim_Status] [varchar](100) NULL,
	[Given_Name] [varchar](120) NULL,
	[Surname] [varchar](100) NULL,
	[Sex] [varchar](10) NULL,
	[Date_of_Birth] [datetime] NULL,
	[Date_of_Death] [datetime] NULL,
	[Occupation] [varchar](150) NULL,
	[Hours_Worked_Per_Week] [numeric](5, 2) NULL,
	[Pre_Disability_Income] [numeric](28, 2) NULL,
	[State] [varchar](40) NULL,
	[Post_Code] [varchar](40) NULL,
	[Case_Type] [varchar](50) NULL,
	[Current_Claim_Owner] [varchar](100) NULL,
	[Claim_Team] [varchar](60) NULL,
	[Registration_Date] [datetime] NULL,
	[First_Contact_Date] [datetime] NULL,
	[Incurred_Date] [datetime] NULL,
	[Age_At_Incurred_Date] [numeric](18, 0) NULL,
	[Claim_Event_Type] [varchar](50) NULL,
	[Primary_Cause_Code] [varchar](20) NULL,
	[Primary_Cause_Description] [varchar](500) NULL,
	[Primary_Cause_Date] [datetime] NULL,
	[Secondary_Cause_Code] [varchar](20) NULL,
	[Secondary_Cause_Description] [varchar](500) NULL,
	[Secondary_Cause_Date] [datetime] NULL,
	[Expected_Return_To_Work_Date] [datetime] NULL,
	[Ceased_Work_Date] [datetime] NULL,
	[Claim_Finalised_Date] [datetime] NULL,
	[Claim_Finalised_Reason] [varchar](100) NULL,
	[Claim_Reopen_Date] [datetime] NULL,
	[Claim_Reopen_Reason] [varchar](100) NULL,
	[Policy_Number] [varchar](50) NOT NULL,
	[Contract_Start_Date] [datetime] NULL,
	[Contract_Status] [varchar](10) NULL,
	[Product_Name] [varchar](100) NULL,
	[Benefit_Number] [varchar](30) NULL,
	[Target_Benefit_Code] [varchar](20) NULL,
	[Target_Benefit_Description] [varchar](100) NULL,
	[Benefit_Status] [varchar](100) NULL,
	[Risk_Commenced_Date] [datetime] NULL,
	[Risk_Expiry_Date] [datetime] NULL,
	[Waiting_Period_Accident] [varchar](30) NULL,
	[Waiting_Period_Sickness] [varchar](30) NULL,
	[Benefit_Period_Accident] [varchar](30) NULL,
	[Benefit_Period_Sickness] [varchar](30) NULL,
	[Sum_Insured] [numeric](28, 2) NULL,
	[Calculation_Start_Type] [varchar](100) NULL,
	[Calculation_End_Type] [varchar](100) NULL,
	[Decision] [varchar](100) NULL,
	[Accepted] [varchar](1) NULL,
	[Decision_Date] [datetime] NULL,
	[Decision_Reason] [varchar](100) NULL,
	[Finalised_Date] [datetime] NULL,
	[Finalised_Reason] [varchar](100) NULL,
	[Benefit_Reopen_Date] [datetime] NULL,
	[Benefit_Reopen_Reason] [varchar](100) NULL,
	[Super_Contribution_Percent] [numeric](18, 4) NULL,
	[Indexation_Flag] [varchar](1) NULL,
	[Agreed_Value] [varchar](1) NULL,
	[Chronic_Condition_Option] [varchar](1) NULL,
	[Day_1_Accident_Option] [varchar](1) NULL,
	[Reinsurer_Name] [varchar](100) NULL,
	[Reinsurance_Treaty_Type] [varchar](100) NULL,
	[Reinsurance_Percent] [numeric](18, 4) NULL,
	[Adviser_Sales_ID] [varchar](50) NULL,
	[Group_Plan_Name] [varchar](100) NULL,
	[Group_Plan_Number] [varchar](50) NULL,
	[Risk_Category] [varchar](100) NULL,
	[Override_Risk_Category] [varchar](1) NULL,
	[Override_Risk_Category_Reason] [varchar](200) NULL,
	[Primary_Occupation_Start_Date] [datetime] NULL,
	[Primary_Occupation_End_Date] [datetime] NULL,
	[Date_of_Diagnosis] [datetime] NULL,
	[External_Member_Number] [varchar](50) NULL,
	[Benefit_Creation_Date] [datetime] NULL,
	[Class_of_Business] [varchar](50) NULL,
	[Initial_Sum_Insured_x12] [numeric](28, 2) NULL,
	[Contract_End_Date] [datetime] NULL,
	[Sum_Reinsured] [numeric](28, 2) NULL,
	[Partial_Earnings_Income] [numeric](28, 2) NULL,
	[Benefit_Start_Date] [datetime] NULL,
	[Benefit_End_Date] [datetime] NULL,
	[Maximum_Indexation_Rate] [numeric](18, 4) NULL,
	[Source] [varchar](50) NULL,
	[Incident_Occurred_Overseas] [varchar](1) NULL,
	[Country_of_Incident] [varchar](100) NULL,
	[Source_System] [varchar](50) NULL,
	[Claim_Type] [varchar](50) NULL,
	[Source_Benefit_Code] [varchar](20) NULL,
	[Source_Benefit_Description] [varchar](100) NULL,
	[Initial_Sum_Insured] [numeric](28, 2) NULL,
	[Initial_Sum_Insured_Freq] [varchar](50) NULL,
	[Primary_Occ_Self_Employed_Ind] [varchar](1) NULL,
	[TPD_Definition] [varchar](100) NULL,
	[TPD_Sub_Definition] [varchar](100) NULL,
	[Trauma_Definition] [varchar](100) NULL,
	[Source_Benefit_Type] [varchar](100) NULL,
	[Product_Code] [varchar](20) NULL,
	[Lumpsum_IP_Indicator] [varchar](7) NULL,
	[Party_Id] [varchar](50) NULL,
	[Declined] [varchar](1) NULL,
	[Qualifying_Period] [varchar](100) NULL,
	[Expected_Resolution_Date] [datetime] NULL,
	[Target_Benefit_Type] [varchar](100) NULL,
	[Source_Benefit_Selected] [varchar](1) NULL,
	[Concurrent_Claim_Indicator] [varchar](1) NULL,
	[Concurrent_Claim_Numbers] [varchar](255) NULL,
	[Date_Returned_To_Work] [datetime] NULL,
	[Expected_RTW_FT_Range] [varchar](50) NULL,
	[Admit_Advance_Pay_and_Finalise] [varchar](50) NULL,
	[Non_Disclosure] [varchar](50) NULL,
	[CMP_Required] [varchar](50) NULL,
	[Urgent_Financial_Needs_Flag] [numeric](1, 0) NULL,
	[Special_Arrangement_Flag] [numeric](1, 0) NULL,
	[Special_Arrangement_Duration] [varchar](50) NULL,
	[Unexpected_Circumstances] [varchar](100) NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Updated_Date_Time] [datetime] NULL,
	[Updated_By] [varchar](30) NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[Process_Log_ID] [int] NOT NULL,
	[Exclude_Flag] [varchar](1) NULL,
	[Exclude_Reason] [varchar](255) NULL,
	[Implied_Source_Benefit_Selected] [varchar](1) NULL,
	[Coverage_Number] [varchar](10) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Able_Claim_Expense]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Able_Claim_Expense](
	[Month_Eftv_Date] [datetime] NOT NULL,
	[Claim_Expense_Id] [int] IDENTITY(1,1) NOT NULL,
	[Claim_Number] [varchar](30) NULL,
	[Payee] [varchar](120) NULL,
	[Policy_Number] [varchar](120) NULL,
	[Invoice_Type] [varchar](20) NULL,
	[Invoice_Number] [varchar](120) NULL,
	[Amount_inc_GST] [numeric](28, 2) NULL,
	[GST] [numeric](28, 2) NULL,
	[Payment_Method] [varchar](20) NULL,
	[Vendor_Id] [varchar](120) NULL,
	[Admin_Initials] [varchar](40) NULL,
	[Payment_Creation_Date] [datetime] NULL,
	[Payment_Reference] [varchar](120) NULL,
	[Authorised_By] [varchar](120) NULL,
	[Authorisation_Date] [datetime] NULL,
	[Lumpsum_IP_Indicator] [varchar](7) NULL,
	[Claim_Type] [varchar](50) NULL,
	[Class_of_Business] [varchar](50) NULL,
	[Benefit_Code] [varchar](20) NULL,
	[Product_Code] [varchar](20) NULL,
	[Target_Benefit_Type] [varchar](100) NULL,
	[GUID] [numeric](10, 0) NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Updated_Date_Time] [datetime] NULL,
	[Updated_By] [varchar](30) NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[Process_Log_ID] [int] NOT NULL,
	[Exclude_Flag] [varchar](1) NULL,
	[Exclude_Reason] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Able_Claim_Expense_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Able_Claim_Expense_Trnx](
	[Month_Eftv_Date] [datetime] NOT NULL,
	[Able_Claim_Expense_Trnx_Id] [int] NULL,
	[Claim_Number] [varchar](30) NULL,
	[Payee] [varchar](120) NULL,
	[Invoice_Type] [varchar](20) NULL,
	[Invoice_Number] [varchar](120) NULL,
	[Amount_inc_GST] [numeric](28, 2) NULL,
	[GST] [numeric](28, 2) NULL,
	[Payment_Method] [varchar](120) NULL,
	[Vendor_Id] [varchar](120) NULL,
	[Admin_Initials] [varchar](40) NULL,
	[Payment_Creation_Date] [datetime] NULL,
	[Payment_Reference] [varchar](120) NULL,
	[Authorised_By] [varchar](120) NULL,
	[Authorisation_Date] [datetime] NULL,
	[GUID] [numeric](10, 0) NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Updated_Date_Time] [datetime] NULL,
	[Updated_By] [varchar](30) NULL,
	[Process_Log_ID] [int] NOT NULL,
	[Exclude_Flag] [varchar](1) NULL,
	[Exclude_Reason] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[GL_Journal_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GL_Journal_Trnx](
	[Month_Eftv_Date] [datetime] NOT NULL,
	[GL_Journal_Trnx_Id] [int] IDENTITY(1,1) NOT NULL,
	[Policy_Number] [varchar](50) NULL,
	[EDW_Product_System_Code] [varchar](3) NULL,
	[EDW_Policy_Number] [varchar](50) NULL,
	[EDW_Contract_Id] [varchar](50) NULL,
	[EDW_Contract_Key] [numeric](10, 0) NULL,
	[EDW_Product_Code] [varchar](20) NULL,
	[EDW_Product_Name] [varchar](100) NULL,
	[EDW_Class_of_Business] [varchar](50) NULL,
	[EDW_Product_Key] [numeric](10, 0) NULL,
	[Trace_Code] [varchar](70) NULL,
	[GL_Business_Unit_Id] [varchar](5) NULL,
	[GL_Journal_Id] [varchar](20) NULL,
	[GL_Journal_Date] [datetime] NULL,
	[GL_Journal_Line] [numeric](10, 0) NULL,
	[GL_Ledger_Code] [varchar](10) NULL,
	[GL_Account_Id] [varchar](6) NULL,
	[GL_Department_Id] [varchar](10) NULL,
	[GL_Product_Id] [varchar](6) NULL,
	[GL_Project_Id] [varchar](15) NULL,
	[GL_Affiliate_Id] [varchar](5) NULL,
	[GL_Currency_Code] [varchar](3) NULL,
	[GL_Monetary_Amount] [numeric](15, 2) NULL,
	[GL_Posting_Ref_1] [varchar](30) NULL,
	[GL_Journal_Line_Desc] [varchar](100) NULL,
	[Status_Ref_1] [varchar](1) NULL,
	[Source_System_Ref_1] [varchar](3) NULL,
	[Source_System_Ref_2] [varchar](3) NULL,
	[GL_Posting_Ref_2] [varchar](30) NULL,
	[Status_Ref_2] [varchar](1) NULL,
	[GL_Posting_Date] [datetime] NULL,
	[User_Id] [varchar](30) NULL,
	[Source_System_Narrative_1] [varchar](250) NULL,
	[GL_Statistic_Amount] [numeric](15, 2) NULL,
	[GL_Foreign_Currency_Code] [varchar](3) NULL,
	[GL_Foreign_Amount] [numeric](15, 2) NULL,
	[GL_Date_Time] [datetime] NULL,
	[GL_Journal_Month] [varchar](2) NULL,
	[Pcode] [varchar](15) NULL,
	[Paid_From_Date] [datetime] NULL,
	[Paid_To_Date] [datetime] NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Updated_Date_Time] [datetime] NULL,
	[Updated_By] [varchar](30) NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[Process_Log_ID] [int] NOT NULL,
	[Exclude_Flag] [varchar](1) NULL,
	[Exclude_Reason] [varchar](255) NULL,
	[EDW_Country_Code] [varchar](3) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Mercury_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Mercury_Trnx](
	[Month_Effective_Date] [datetime] NOT NULL,
	[Mercury_Trnx_Id] [int] IDENTITY(1,1) NOT NULL,
	[Product_System_Code] [varchar](3) NULL,
	[Policy_Number] [varchar](50) NULL,
	[EDW_Contract_Key] [numeric](10, 0) NULL,
	[EDW_Policy_Number] [varchar](50) NULL,
	[EDW_Contract_Id] [varchar](50) NULL,
	[EDW_Contract_Start_Date] [datetime] NULL,
	[EDW_Class_Of_Business] [varchar](50) NULL,
	[EDW_Product_Key] [numeric](10, 0) NULL,
	[EDW_Product_Code] [varchar](20) NULL,
	[EDW_Product_Name] [varchar](100) NULL,
	[Trace_Code] [varchar](70) NULL,
	[Admin_System_File_Id] [numeric](10, 0) NOT NULL,
	[Admin_System_Id] [varchar](15) NOT NULL,
	[File_Record_Number] [numeric](10, 0) NOT NULL,
	[GL_Posting_Date] [datetime] NOT NULL,
	[Stat_Fund] [varchar](1) NOT NULL,
	[Sequence_Number] [varchar](1) NOT NULL,
	[Primary_Secondary_Ind] [varchar](1) NOT NULL,
	[Client_Ref_Id_1] [varchar](20) NULL,
	[Client_Ref_Id_2] [varchar](10) NULL,
	[Client_Ref_Id_3] [varchar](10) NULL,
	[Client_Ref_Id_4] [varchar](10) NULL,
	[Trans_Ref_Key_2] [varchar](10) NULL,
	[Trans_Ref_Key_3] [varchar](10) NULL,
	[Trans_Ref_Key_4] [varchar](10) NULL,
	[Trans_Ref_Key_5] [varchar](10) NULL,
	[Trans_Ref_Key_6] [varchar](10) NULL,
	[GL_Business_Unit_Id] [varchar](5) NULL,
	[GL_Account_Id] [varchar](6) NULL,
	[GL_Department_Id] [varchar](10) NULL,
	[GL_Product_Code] [varchar](6) NULL,
	[GL_Project_Id] [varchar](15) NULL,
	[GL_Affiliate_Id] [varchar](5) NULL,
	[GL_Posting_Ref_Text] [varchar](30) NULL,
	[Ledger_Movement_Amount] [numeric](15, 2) NULL,
	[GL_Posting_File_Id] [numeric](10, 0) NULL,
	[Trans_Ref_Key_1] [varchar](10) NULL,
	[Client_Ref_Id_5] [varchar](15) NULL,
	[Product_Ref_Key_1] [varchar](10) NULL,
	[Product_Ref_Key_2] [varchar](10) NULL,
	[Product_Ref_Key_3] [varchar](10) NULL,
	[Bus_Line_Ref_Key_1] [varchar](10) NULL,
	[Bus_Line_Ref_Key_2] [varchar](10) NULL,
	[Bus_Line_Ref_Key_3] [varchar](10) NULL,
	[Business_Line_Location] [varchar](3) NULL,
	[Business_Line_Planner_Id] [varchar](10) NULL,
	[Business_Tax_Classn] [varchar](1) NULL,
	[Stat_Fund_Code] [varchar](1) NULL,
	[Investment_Pool_Id] [varchar](5) NULL,
	[Off_Investment_Pool_Id] [varchar](5) NULL,
	[Transaction_Amount] [numeric](15, 2) NULL,
	[Transaction_Process_Date] [datetime] NULL,
	[Investment_Effective_Date] [datetime] NULL,
	[Movement_Effective_Date] [datetime] NULL,
	[GL_Currency_Code] [varchar](3) NULL,
	[Trans_Ref_Key_7] [varchar](10) NULL,
	[Trans_Ref_Key_8] [varchar](10) NULL,
	[Trans_Ref_Key_9] [varchar](10) NULL,
	[Trans_Ref_Key_10] [varchar](10) NULL,
	[Trans_Ref_Key_11] [varchar](10) NULL,
	[Business_Product_Line_Code] [varchar](15) NULL,
	[Business_Product_Owner_Code] [varchar](15) NULL,
	[Business_Product_Code] [varchar](15) NULL,
	[Business_Line_Code] [varchar](15) NULL,
	[Business_Trans_Type] [varchar](10) NULL,
	[Source_Sys_Reference_1] [varchar](50) NULL,
	[Source_Sys_Narrative_1] [varchar](50) NULL,
	[Source_Sys_Reference_2] [varchar](50) NULL,
	[Source_Sys_Narrative_2] [varchar](50) NULL,
	[Admin_File_Ref_Text] [varchar](15) NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Updated_Date_Time] [datetime] NULL,
	[Updated_By] [varchar](30) NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[Process_Log_ID] [int] NOT NULL,
	[Exclude_Flag] [varchar](1) NULL,
	[Exclude_Reason] [varchar](255) NULL,
	[EDW_Country_Code] [varchar](3) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[Stg_Able_Claim_Expense]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[Stg_Able_Claim_Expense](
	[Claim_Number] [varchar](30) NULL,
	[Payee] [varchar](120) NULL,
	[Policy_Number] [varchar](120) NULL,
	[Invoice_Type] [varchar](120) NULL,
	[Invoice_Number] [varchar](120) NULL,
	[Amount_inc_GST] [numeric](28, 6) NULL,
	[GST] [numeric](28, 6) NULL,
	[Payment_Method] [varchar](120) NULL,
	[Vendor_Id] [varchar](120) NULL,
	[Admin_Initials] [varchar](40) NULL,
	[Payment_Creation_Date] [datetime] NULL,
	[Payment_Reference] [varchar](120) NULL,
	[Authorised_By] [varchar](120) NULL,
	[Authorisation_Date] [datetime] NULL,
	[Lumpsum_IP_Indicator] [varchar](7) NULL,
	[Claim_Type] [varchar](4000) NULL,
	[Class_of_Business] [varchar](4000) NULL,
	[Benefit_Code] [varchar](20) NULL,
	[Product_Code] [varchar](4000) NULL,
	[Target_Benefit_Type] [varchar](4000) NULL,
	[Created_Date_Time] [datetime] NOT NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[GUID] [numeric](10, 0) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[Stg_AMPL_GL_Journal_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[Stg_AMPL_GL_Journal_Trnx](
	[GL_Business_Unit_Id] [varchar](5) NULL,
	[GL_Journal_Id] [varchar](20) NULL,
	[GL_Journal_Date] [datetime] NULL,
	[GL_Journal_Line] [numeric](10, 0) NULL,
	[GL_Ledger_Code] [varchar](10) NULL,
	[GL_Account_Id] [varchar](6) NULL,
	[GL_Department_Id] [varchar](10) NULL,
	[GL_Product_Id] [varchar](6) NULL,
	[GL_Project_Id] [varchar](15) NULL,
	[GL_Affiliate_Id] [varchar](5) NULL,
	[GL_Currency_Code] [varchar](3) NULL,
	[GL_Monetary_Amount] [numeric](15, 2) NULL,
	[GL_Posting_Ref_1] [varchar](30) NULL,
	[GL_Journal_Line_Desc] [varchar](100) NULL,
	[Status_Ref_1] [varchar](1) NULL,
	[Source_System_Ref_1] [varchar](3) NULL,
	[Source_System_Ref_2] [varchar](3) NULL,
	[GL_Posting_Ref_2] [varchar](30) NULL,
	[Status_Ref_2] [varchar](1) NULL,
	[GL_Posting_Date] [datetime] NULL,
	[User_Id] [varchar](30) NULL,
	[Source_System_Narrative_1] [varchar](250) NULL,
	[GL_Statistic_Amount] [numeric](15, 2) NULL,
	[GL_Foreign_Currency_Code] [varchar](3) NULL,
	[GL_Foreign_Amount] [numeric](15, 2) NULL,
	[GL_Journal_Month] [varchar](2) NULL,
	[Pcode] [varchar](15) NULL,
	[Policy_Number] [varchar](50) NULL,
	[Paid_From_Date] [datetime] NULL,
	[Paid_To_Date] [datetime] NULL,
	[Created_Date_Time] [datetime] NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[Stg_AMPN_GL_Journal_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[Stg_AMPN_GL_Journal_Trnx](
	[GL_Business_Unit_Id] [varchar](5) NULL,
	[GL_Journal_Id] [varchar](20) NULL,
	[GL_Journal_Date] [datetime] NULL,
	[GL_Journal_Line] [numeric](10, 0) NULL,
	[GL_Ledger_Code] [varchar](10) NULL,
	[GL_Account_Id] [varchar](6) NULL,
	[GL_Department_Id] [varchar](10) NULL,
	[GL_Product_Id] [varchar](6) NULL,
	[GL_Project_Id] [varchar](15) NULL,
	[GL_Affiliate_Id] [varchar](5) NULL,
	[GL_Currency_Code] [varchar](3) NULL,
	[GL_Monetary_Amount] [numeric](15, 2) NULL,
	[GL_Posting_Ref_1] [varchar](30) NULL,
	[GL_Journal_Line_Desc] [varchar](100) NULL,
	[Status_Ref_1] [varchar](1) NULL,
	[Source_System_Ref_1] [varchar](3) NULL,
	[Source_System_Ref_2] [varchar](3) NULL,
	[GL_Posting_Ref_2] [varchar](30) NULL,
	[Status_Ref_2] [varchar](1) NULL,
	[GL_Posting_Date] [datetime] NULL,
	[User_Id] [varchar](30) NULL,
	[Source_System_Narrative_1] [varchar](250) NULL,
	[GL_Statistic_Amount] [numeric](15, 2) NULL,
	[GL_Foreign_Currency_Code] [varchar](3) NULL,
	[GL_Foreign_Amount] [numeric](15, 2) NULL,
	[GL_Date_Time] [datetime] NULL,
	[GL_Journal_Month] [varchar](2) NULL,
	[Pcode] [varchar](15) NULL,
	[Policy_Number] [varchar](50) NULL,
	[Paid_From_Date] [datetime] NULL,
	[Paid_To_Date] [datetime] NULL,
	[Created_Date_Time] [datetime] NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQProcessing_Able_Claim_Expense_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_DQProcessing_Able_Claim_Expense_Trnx]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS
DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
---------
BEGIN TRY;
BEGIN TRAN T1;
UPDATE dbo.[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'N',
[Exclude_Reason] = NULL
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear) 

 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
---
UPDATE dbo.[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Claim Number is Mandatory',
[Updated_Date_Time] = Getdate(),
Updated_By = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear) 
and [Exclude_Flag] = 'N'
and Claim_Number IS NULL;

 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
--
UPDATE dbo.[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Authorisation Date is Mandatory',
[Updated_Date_Time] = Getdate(),
Updated_By = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear) 
and [Exclude_Flag] = 'N'
and Authorisation_Date IS NULL;

 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
--
UPDATE dbo.[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'GUID is Mandatory',
[Updated_Date_Time] = Getdate(),
Updated_By = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear) 
and [Exclude_Flag] = 'N'
and [GUID] IS NULL;

 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
--
;with w_src
AS (
select DISTINCT [Claim_Number]
from dbo.[Able_Claim_Expense_Trnx]
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and [Exclude_Flag] = 'N'
except
select DISTINCT [Claim_Number]
from dbo.[Able_Claim_Benefit]
)
UPDATE [dbo].[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Claim Number not available in Claim_Benefit',
[Updated_Date_Time] = Getdate(),
Updated_By = system_user
FROM [dbo].[Able_Claim_Expense_Trnx] trnx, w_src reject
WHERE trnx.[Claim_Number] = reject.[Claim_Number];
--
 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

--1. Duplicate Record
;with w_src
AS (
select [Claim_Number]
      ,ISNULL([Invoice_Number],'ooNVLoo') [Invoice_Number]
      ,ISNULL([Payment_Reference],'ooNVLoo') [Payment_Reference]
      ,[Authorisation_Date]
from dbo.[Able_Claim_Expense_Trnx]
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and [Exclude_Flag] = 'N'
group by [Claim_Number]
		  ,ISNULL([Invoice_Number],'ooNVLoo')
		  ,ISNULL([Payment_Reference],'ooNVLoo')
		  ,[Authorisation_Date]
having count(*) > 1
),
w_reject_records
AS
(
SELECT trnx.*
       ,ROW_NUMBER() OVER (PARTITION BY trnx.[Month_Eftv_Date],
									  trnx. [Claim_Number]
									  ,ISNULL(trnx.[Invoice_Number],'ooNVLoo')
									  ,ISNULL(trnx.[Payment_Reference],'ooNVLoo')
									  ,trnx.[Authorisation_Date]
					ORDER BY ISNULL(trnx.[Payment_Creation_Date],convert(datetime,'01/jan/1753')) DESC, trnx.[GUID] ASC) row_num
FROM dbo.[Able_Claim_Expense_Trnx] trnx,w_src reject 
where trnx.[Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and trnx.[Exclude_Flag] = 'N'
and trnx.[Claim_Number] = reject.[Claim_Number]
  AND ISNULL(trnx.[Invoice_Number],'ooNVLoo') = reject.[Invoice_Number]
  AND ISNULL(trnx.[Payment_Reference] ,'ooNVLoo') = reject.[Payment_Reference]
  AND trnx.[Authorisation_Date] = reject.[Authorisation_Date]
)
UPDATE [dbo].[Able_Claim_Expense_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Duplicate Expense',
[Updated_Date_Time] = Getdate(),
Updated_By = system_user
FROM [dbo].[Able_Claim_Expense_Trnx] trnx, w_reject_records reject
WHERE trnx.[Able_Claim_Expense_Trnx_Id] = reject.[Able_Claim_Expense_Trnx_Id]
AND reject.row_num > 1;

 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
--
COMMIT TRAN T1;
--
--SELECT @UpdateRowCount = count(*)
--FROM [dbo].[Able_Claim_Expense_Trnx]
--WHERE [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
--and updated_date_time > (Select StartTime from CTL.ProcessLog where ProcessLogID = @ProcessLogID)
--
SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
--
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON














GO
/****** Object:  StoredProcedure [dbo].[usp_DQProcessing_GL_Journal_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_DQProcessing_GL_Journal_Trnx]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS
---------
DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	BEGIN TRY;
BEGIN TRAN T1;
UPDATE [dbo].[GL_Journal_Trnx]
SET [Exclude_Flag] = 'N',
[Exclude_Reason] = NULL
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

UPDATE [dbo].[GL_Journal_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Policy Number is NULL',
updated_date_time = GETDATE(),
updated_by = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and policy_number is null
and [Exclude_Flag] = 'N'
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

UPDATE [dbo].[GL_Journal_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Source System is Mercury, hence ignore'
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and source_system_Ref_2 = 'MER'
and [Exclude_Flag] = 'N'
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

UPDATE [dbo].[GL_Journal_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'EDW Policy details are unavailable',
updated_date_time = GETDATE(),
updated_by = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and edw_contract_key is null
and [Exclude_Flag] = 'N'
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

UPDATE [dbo].[GL_Journal_Trnx]
SET [Exclude_Flag] = 'Y',
[Exclude_Reason] = 'Country Code is not AUS',
updated_date_time = GETDATE(),
updated_by = system_user
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
and edw_country_Code <> 'AUS'
and [Exclude_Flag] = 'N'
set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

COMMIT TRAN T1;
--SELECT @UpdateRowCount = count(*)
--FROM [dbo].[GL_Journal_Trnx]
--where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
--and updated_date_time > ( SELECT startTime from CTL.ProcessLog where ProcessLogID = @ProcessLogID)
--
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
--------------------
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON













GO
/****** Object:  StoredProcedure [dbo].[usp_DQProcessing_Mercury_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- RRNAA2
-- Opus Claims Payment Enhancement Change
-- RGU-575 - 22/06/2020 - Added new DQ rules for Opus transactions.
-- =============================================


CREATE PROCEDURE [dbo].[usp_DQProcessing_Mercury_Trnx]
   @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS
   DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	--
	BEGIN TRY;
	BEGIN TRAN T1;
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = NULL
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		;

		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--	
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = 'Policy Number is NULL',
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		AND [dbo].[Mercury_Trnx].Policy_Number IS NULL
		AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		;
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--	
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = 'EDW Policy Details are not available',
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		AND [dbo].[Mercury_Trnx].EDW_Contract_Key IS NULL
		AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		;
		--
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--	
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = 'Country Code is not AUS',
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		AND [dbo].[Mercury_Trnx].EDW_Country_Code <> 'AUS'
		AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		;
		--
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	----
	--exec [dbo].[usp_EDW_Risk_For_Mercury] @ProcessingMonthYear
	----
	--UPDATE [dbo].[Mercury_Trnx]
	--	SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
	--		[dbo].[Mercury_Trnx].[Exclude_Reason] = 'No Risk available for this policy in EDW',
	--		[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
	--		[dbo].[Mercury_Trnx].updated_by = system_user
	--	WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
	--		AND [dbo].[Mercury_Trnx].EDW_Contract_Key NOT IN (SELECT DISTINCT EDW_contract_key
	--													FROM dbo.Temp_Contract_Risk)
	--	    AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
	--	;
 --    --
	--	SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	 --
	 --UPDATE [dbo].[Mercury_Trnx]
		--SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
		--	[dbo].[Mercury_Trnx].[Exclude_Reason] = 'Product is not in scope for Reinsurance',
		--	[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
		--	[dbo].[Mercury_Trnx].updated_by = system_user
		--WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		--	AND [dbo].[Mercury_Trnx].EDW_Product_Code NOT IN (SELECT DISTINCT Product_Code
		--												FROM dbo.Reins_Treaty_Product)
		--	AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		--	AND [dbo].[Mercury_Trnx].GL_Product_Code NOT IN (SELECT DISTINCT GL_Product_Code
		--												FROM dbo.Reins_GL_Product
		--												WHERE In_Scope_For_Reinsurance = 'Y')
		--;
		--
		--SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--	

	-- Exclusions for Opus

	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = 'SURR transaction type not in scope',
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		AND [dbo].[Mercury_Trnx].Client_ref_id_2 = 'SURR'
		and dbo.Mercury_Trnx.Product_System_Code = 'LS'
		AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		;
		--
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

	--
		UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[Exclude_Flag] = 'Y',
			[dbo].[Mercury_Trnx].[Exclude_Reason] = 'Adjustment amt Trans_Ref_Key not in scope',
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
		WHERE [dbo].[Mercury_Trnx].Month_Effective_Date = convert(datetime,@ProcessingMonthYear)
		AND [dbo].[Mercury_Trnx].Trans_Ref_Key_1  in ('109','110','162','0109','0180','0120','0160','0182')
		and dbo.Mercury_Trnx.Product_System_Code = 'LS'
		AND [dbo].[Mercury_Trnx].[Exclude_Flag] = 'N'
		;
		--
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

	COMMIT TRAN T1;		
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
---
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON
--
















GO
/****** Object:  StoredProcedure [dbo].[usp_EDW_Contract_For_GL]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_EDW_Contract_For_GL]
     @ProcessLogID Int
  ,@ProcessingMonthYear VARCHAR(100)
AS
---------
-- 
SET NOCOUNT ON
--
	DECLARE @SQL VARCHAR(MAX)
	DECLARE @Policy_Number VARCHAR(50)
	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	DECLARE @No_of_recs Int = 0
	--
	DECLARE Columns_Lists  CURSOR LOCAL  FORWARD_ONLY  READ_ONLY
	FOR 
	select distinct RTRIM(LTRIM(UPPER(Policy_Number))) Policy_Number
	from [dbo].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	--and Source_System_Ref_2 NOT IN ('SPE','SF9')
	--
	DECLARE Columns_Lists_2  CURSOR LOCAL  FORWARD_ONLY  READ_ONLY
	FOR 
	select distinct RTRIM(LTRIM(UPPER(Policy_Number)))+'-' Policy_Number
	from [dbo].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	and Policy_Number LIKE '%[^0-9]%'
	and charindex('-',RTRIM(LTRIM(UPPER(policy_number)))) =  0
	and right(RTRIM(LTRIM(UPPER(Policy_Number))),1) NOT LIKE '%[^0-9]%'
	UNION ALL
	select distinct left(RTRIM(LTRIM(UPPER(Policy_Number))),len(RTRIM(LTRIM(UPPER(Policy_Number))))-1)+'-'+right(RTRIM(LTRIM(UPPER(Policy_Number))),1) Policy_Number
	from [dbo].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and Policy_Number LIKE '%[^0-9]%'
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	and charindex('-',RTRIM(LTRIM(UPPER(policy_number)))) =  0
	and left(right(RTRIM(LTRIM(UPPER(Policy_Number))),2),1) NOT LIKE '%[^0-9]%'
	and right(RTRIM(LTRIM(UPPER(Policy_Number))),1) LIKE '%[^0-9]%'
	UNION ALL
	select distinct REPLACE(RTRIM(LTRIM(UPPER(Policy_Number))),'-','') Policy_Number
	from [dbo].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and Policy_Number LIKE '%[^0-9]%'
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	and charindex('-',RTRIM(LTRIM(UPPER(policy_number)))) >  0
	and left(right(RTRIM(LTRIM(UPPER(Policy_Number))),3),1) NOT LIKE '%[^0-9]%'
	and right(RTRIM(LTRIM(UPPER(Policy_Number))),1) LIKE '%[^0-9]%'
	--
	BEGIN TRY;
	BEGIN TRAN T1;
	--
	UPDATE [dbo].[GL_Journal_Trnx]
	SET   [EDW_Contract_Key] = NULL,
		  [EDW_Product_System_Code] = NULL,
		  [EDW_Policy_Number] = NULL,
		  [EDW_Contract_Id] = NULL,
		  [EDW_Product_Key] = NULL,
		  [EDW_Product_Code] = NULL,
		  [EDW_Product_Name] =NULL,
		  [EDW_Class_Of_Business] = NULL,
		  [EDW_Country_Code] = NULL,
		  [Updated_Date_Time] = NULL,
		  [Updated_By] = NULL
	WHERE [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear);	
	--
	;WITH w_source
	AS
	( 
		select      RTRIM(LTRIM(UPPER(Policy_Number))) Policy_Number,
					[EDW_Product_System_Code],
					EDW_Contract_Key,
					[EDW_Policy_Number],
					[EDW_Contract_Id],
					[EDW_Product_Key],
					[EDW_Product_Code],
					[EDW_Product_Name],
					[EDW_Class_Of_Business],
					[EDW_Country_Code]
		from [dbo].[GL_Journal_Trnx]
		WHERE [Month_Eftv_Date] = EOMONTH(DATEADD(month,-1, convert(datetime,@ProcessingMonthYear)))
		and [EDW_Contract_Key] IS NOT NULL
			GROUP BY RTRIM(LTRIM(UPPER(Policy_Number))),
					[EDW_Product_System_Code],
					EDW_Contract_Key,
					[EDW_Policy_Number],
					[EDW_Contract_Id],
					[EDW_Product_Key],
					[EDW_Product_Code],
					[EDW_Product_Name],
					[EDW_Class_Of_Business],
					[EDW_Country_Code]
	)
	UPDATE [dbo].[GL_Journal_Trnx]
		SET [dbo].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [dbo].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[EDW_Product_System_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[GL_Journal_Trnx].updated_by = system_user
	FROM [dbo].[GL_Journal_Trnx] main, w_source src
	WHERE main.[Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
		AND RTRIM(LTRIM(UPPER(main.Policy_Number))) = src.Policy_Number
		AND ISNULL(main.source_system_Ref_2,'ooNVLoo') <> 'MER'
	;
	SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--
	 CREATE TABLE #Temp_EDW_Contract (
			[EDW_Contract_Key] [numeric](10, 0) NULL,
			[EDW_Policy_Number] [varchar](50) NULL,
			[EDW_Contract_Id] [varchar](50) NULL,
			[Product_System_Code] VARCHAR(50) NULL,
			[EDW_Contract_Start_Date] [datetime] NULL,
			[EDW_Product_Key] [numeric](10, 0) NULL,
			[EDW_Product_Code] [varchar](4000) NULL,			
			[EDW_Product_Name] [varchar](300) NULL,
			EDW_Class_Of_Business VARCHAR(50) NULL,
			EDW_Country_Code VARCHAR(3) NULL
	 )
	--
	OPEN Columns_Lists
	FETCH NEXT FROM Columns_Lists into 
	@Policy_Number
	--
	SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.contract_id,
							c.product_system_code,
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code NOT IN ( ''ZB'',''U1'',''EC'',''EXR'',''SM'',''BK'',''CS'',''F4'',''L7'',''L8'',''YC'',''S2'',''TG'',''TZ'',''AFG'')
							 and c.product_key <> -1
							 and ( c.product_system_code NOT IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
								or
									(c.product_system_code IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
									and c.super_class_type_Code IN (''MBR'') 
									)
								  )
							and  ( (1 = 2) or '
	WHILE @@FETCH_STATUS = 0 
	BEGIN 
		
		If len(@SQL) > 7800
		BEGIN
			SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'
		    INSERT INTO #Temp_EDW_Contract
			EXEC Utility.dbo.usp_queryedw @SQL
			SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.contract_id,
							c.product_system_code,
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code NOT IN ( ''ZB'',''U1'',''EC'',''EXR'',''SM'',''BK'',''CS'',''F4'',''L7'',''L8'',''YC'',''S2'',''TG'',''TZ'',''AFG'')
							 and c.product_key <> -1
							 and ( c.product_system_code NOT IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
								or
									(c.product_system_code IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
									and c.super_class_type_Code IN (''MBR'') 
									)
								  )
							and  ( (1 = 2) or '
		END
		--
		SET @SQL =  @SQL + '( c.display_contract_id = '''+ @Policy_Number+''') or (c.contract_id = '''+ @Policy_Number+''') or '		
		--
		FETCH NEXT FROM Columns_Lists into 
		@Policy_Number
	END

	CLOSE Columns_Lists
	Deallocate Columns_Lists
	--
	SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'
		    INSERT INTO #Temp_EDW_Contract
			EXEC Utility.dbo.usp_queryedw @SQL
	--
	
	UPDATE [dbo].[GL_Journal_Trnx]
		SET [dbo].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [dbo].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[GL_Journal_Trnx].updated_by = system_user
	FROM [dbo].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
	WHERE main.[Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
		AND ( RTRIM(LTRIM(UPPER(main.Policy_Number))) = src.EDW_Policy_Number
		  OR RTRIM(LTRIM(UPPER(main.Policy_Number)))= src.[EDW_Contract_Id])
		  AND ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	;
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--
	--
	TRUNCATE TABLE #Temp_EDW_Contract;
	--
	OPEN Columns_Lists_2
	FETCH NEXT FROM Columns_Lists_2 into 
	@Policy_Number
	--
	SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.contract_id,
							c.product_system_code,
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code IN (''CP'',''OR'')
							and  ( (1 = 2) or '
	WHILE @@FETCH_STATUS = 0 
	BEGIN 
		
		If len(@SQL) > 7800
		BEGIN
			SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'
		    INSERT INTO #Temp_EDW_Contract
			EXEC Utility.dbo.usp_queryedw @SQL
			SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.contract_id,
							c.product_system_code,
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code IN (''CP'',''OR'')
							and  ( (1 = 2) or '
		END
		--
		SET @SQL =  @SQL + '(SUBSTR(c.display_contract_id,1,length(c.display_contract_id)-1) = '''+ @Policy_Number+''') or (c.display_contract_id = '''+ @Policy_Number+''')  or '		
		--
		FETCH NEXT FROM Columns_Lists_2 into 
		@Policy_Number
	END

	CLOSE Columns_Lists_2
	Deallocate Columns_Lists_2
	--
	SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'
		    INSERT INTO #Temp_EDW_Contract
			EXEC Utility.dbo.usp_queryedw @SQL
	--
	UPDATE [dbo].[GL_Journal_Trnx]
		SET [dbo].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [dbo].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[GL_Journal_Trnx].updated_by = system_user
	FROM [dbo].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
	WHERE main.[Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
		AND ( RTRIM(LTRIM(UPPER(main.Policy_Number)))+'-' = left(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))),len(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))))-1)
		     OR left(RTRIM(LTRIM(UPPER(main.Policy_Number))),len(RTRIM(LTRIM(UPPER(main.Policy_Number))))-1)+'-'+right(RTRIM(LTRIM(UPPER(main.Policy_Number))),1) = left(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))),len(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))))-1)
			 )
		and main.EDW_Contract_Key IS NULL
		and charindex('-',RTRIM(LTRIM(UPPER(main.policy_number)))) =  0
		and Policy_Number LIKE '%[^0-9]%'
		AND ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	;
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT 

	UPDATE [dbo].[GL_Journal_Trnx]
		SET [dbo].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [dbo].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[GL_Journal_Trnx].updated_by = system_user
	FROM [dbo].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
	WHERE main.[Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
		AND REPLACE(RTRIM(LTRIM(UPPER(main.Policy_Number))),'-','')= left(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))),len(RTRIM(LTRIM(UPPER(src.[EDW_Policy_Number]))))-1)
		and main.EDW_Contract_Key IS NULL
		and charindex('-',RTRIM(LTRIM(UPPER(main.policy_number)))) >  0
		and Policy_Number LIKE '%[^0-9]%'
		AND ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	;
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT 
	--
	COMMIT TRAN T1;
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
---
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON
----





GO
/****** Object:  StoredProcedure [dbo].[usp_EDW_Contract_For_Mercury]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- RRNAA2
-- Opus Claims Payment Enhancement Change
-- RGU-575 - 22/06/2020


CREATE PROCEDURE [dbo].[usp_EDW_Contract_For_Mercury]
     @ProcessLogID Int
  ,@ProcessingMonthYear VARCHAR(100)
AS
---------
-- 
SET NOCOUNT ON
--
	DECLARE @SQL NVARCHAR(MAX)
	DECLARE @Policy_Number VARCHAR(50)
	DECLARE @Product_System_Code VARCHAR(50)
	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	DECLARE @No_of_recs Int = 0
	--
	BEGIN TRY;
	BEGIN TRAN T1;
	--
	UPDATE [dbo].[Mercury_Trnx]
	SET   [EDW_Contract_Key] = NULL,
		  [EDW_Policy_Number] = NULL,
		  [EDW_Contract_Id] = NULL,
		  [EDW_Contract_Start_Date] = NULL,
		  [EDW_Product_Key] = NULL,
		  [EDW_Product_Code] = NULL,
		  [EDW_Product_Name] = NULL,
		  EDW_Class_Of_Business = NULL,
		  [EDW_Country_Code] = NULL,
		  [Updated_Date_Time] = NULL,
		  [Updated_By] = NULL
	WHERE [Month_Effective_Date] = convert(datetime,@ProcessingMonthYear)
	;
	DECLARE Columns_Lists  CURSOR LOCAL  FORWARD_ONLY  READ_ONLY
	FOR 
	select distinct RTRIM((LTRIM (Policy_Number))) Policy_Number ,
					Product_System_Code
	from [dbo].[Mercury_Trnx]
	where [Month_Effective_Date] = convert(datetime,@ProcessingMonthYear)
	AND Policy_Number IS NOT NULL
	AND EDW_Contract_Key IS NULL;	
	--	
	;WITH w_source
	AS
	( 
		select      RTRIM((LTRIM (Policy_Number))) Policy_Number,
					Product_System_Code,
					EDW_Contract_Key,
					[EDW_Policy_Number],
					[EDW_Contract_Id],
					[EDW_Contract_Start_Date],
					[EDW_Product_Key],
					[EDW_Product_Code],
					[EDW_Product_Name],
					[EDW_Class_Of_Business],
					[EDW_Country_Code]
		from [dbo].[Mercury_Trnx]
		WHERE [Month_Effective_Date] = EOMONTH(DATEADD(month,-1, convert(datetime,@ProcessingMonthYear)))
		and [EDW_Contract_Key] IS NOT NULL
			GROUP BY RTRIM((LTRIM (Policy_Number))),
					Product_System_Code,
					EDW_Contract_Key,
					[EDW_Policy_Number],
					[EDW_Contract_Id],
					[EDW_Contract_Start_Date],
					[EDW_Product_Key],
					[EDW_Product_Code],
					[EDW_Product_Name],
					[EDW_Class_Of_Business],
					[EDW_Country_Code]
	)
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
			[dbo].[Mercury_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[Mercury_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[Mercury_Trnx].[EDW_Contract_Start_Date] = src.[EDW_Contract_Start_Date],
			[dbo].[Mercury_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[Mercury_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[Mercury_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[Mercury_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[Mercury_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
	FROM [dbo].[Mercury_Trnx] main, w_source src
	WHERE main.[Month_Effective_Date] = convert(datetime,@ProcessingMonthYear)
		AND RTRIM((LTRIM (main.Policy_Number))) = src.Policy_Number
		AND main.Product_System_Code = src.Product_System_Code
	;
	SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

	IF OBJECT_ID('tempdb.dbo.#Temp_EDW_Contract', 'U') IS NOT NULL
     DROP TABLE #Temp_EDW_Contract
	 CREATE TABLE #Temp_EDW_Contract (
			[EDW_Contract_Key] [numeric](10, 0) NULL,
			[EDW_Policy_Number] [varchar](50) NULL,			
			[Product_System_Code] VARCHAR(50) NULL,
			[EDW_Contract_Id] [varchar](50) NULL,
			[EDW_Contract_Start_Date] [datetime] NULL,
			[EDW_Product_Key] [numeric](10, 0) NULL,
			[EDW_Product_Code] [varchar](4000) NULL,			
			[EDW_Product_Name] [varchar](300) NULL,
			EDW_Class_Of_Business VARCHAR(50) NULL,
			EDW_Country_Code VARCHAR(3) NULL
	 )

	--
	OPEN Columns_Lists
	FETCH NEXT FROM Columns_Lists into 
	@Policy_Number,
	@Product_System_Code
	--
	PRINT '@Policy_Number,@Product_System_Code' + @Policy_Number + @Product_System_Code
	SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.product_system_code,
							c.contract_id,  
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code NOT IN ( ''ZB'',''U1'',''EC'',''EXR'',''SM'',''BK'',''CS'',''F4'',''L7'',''L8'',''YC'',''S2'',''TG'',''TZ'',''AFG'')
							 and c.product_key <> -1
							 and ( c.product_system_code NOT IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
								or (c.product_system_code IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
									and c.super_class_type_Code IN (''MBR'') 
									) ) and  ( (1 = 2) or '

	WHILE @@FETCH_STATUS = 0 
	BEGIN 
		
		If len(@SQL) > 7800
		BEGIN
			SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'
		    INSERT INTO #Temp_EDW_Contract
			EXEC Utility.dbo.usp_queryedw @SQL
			SET @SQL =  'select  c.contract_key, 
							c.display_contract_id, 
							c.product_system_code,
							c.contract_id,  
							c.contract_start_date, 
							p.product_key,							
							p.product_code edw_product_code,
							p.product_desc product_name,
							cb.class_of_Busn_desc,
							c.country_code
							 from dw_contract c,
							dw_product p,
							dw_class_of_busn cb
							where c.product_key = p.product_key
							and p.class_of_Busn_code = cb.class_of_busn_code
							and c.product_system_code NOT IN ( ''ZB'',''U1'',''EC'',''EXR'',''SM'',''BK'',''CS'',''F4'',''L7'',''L8'',''YC'',''S2'',''TG'',''TZ'',''AFG'')
							 and c.product_key <> -1
							 and ( c.product_system_code NOT IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
								or (c.product_system_code IN (''U2'',''RT'',''AS'',''CM'',''KS'',''RP'')
									and c.super_class_type_Code IN (''MBR'') 
									)) and ( (1 = 2) or '
		END
		--
		if @Product_System_Code = 'LS'
		BEGIN
		SET @SQL =  @SQL + '(c.display_contract_id = '''+ @Policy_Number+''' and c.product_system_code = '''+@Product_System_Code+''' ) or (c.contract_id = '''+ @Policy_Number+''' and c.product_system_code = '''+@Product_System_Code+''') or'		
		END
		ELSE
		BEGIN
		SET @SQL =  @SQL + '(c.display_contract_id = '''+ @Policy_Number+''' and c.product_system_code = '''+@Product_System_Code+''' ) or'
		END
		-- 

		FETCH NEXT FROM Columns_Lists into 
		@Policy_Number,
		@Product_System_Code
	END
	PRINT '@Policy_Number,@Product_System_Code' + @Policy_Number + @Product_System_Code

	CLOSE Columns_Lists
	Deallocate Columns_Lists
	--
	SET @SQL = substring(@SQL,1,len(@SQL)-3) + ')'

		    		--print '@SQL ' + @SQL
			INSERT INTO #Temp_EDW_Contract	
			EXEC Utility.dbo.usp_queryedw @SQL
	
	select * from #Temp_EDW_Contract
	print 'policies for EDW check'
	--
	UPDATE [dbo].[Mercury_Trnx]
		SET [dbo].[Mercury_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
			[dbo].[Mercury_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[dbo].[Mercury_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[dbo].[Mercury_Trnx].[EDW_Contract_Start_Date] = src.[EDW_Contract_Start_Date],
			[dbo].[Mercury_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[dbo].[Mercury_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[dbo].[Mercury_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[dbo].[Mercury_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[dbo].[Mercury_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[dbo].[Mercury_Trnx].Updated_Date_Time = getDATE(),
			[dbo].[Mercury_Trnx].updated_by = system_user
	FROM [dbo].[Mercury_Trnx] main, #Temp_EDW_Contract src
	WHERE main.[Month_Effective_Date] = convert(datetime,@ProcessingMonthYear)
		AND RTRIM((LTRIM (main.Policy_Number))) = src.EDW_Policy_Number or RTRIM((LTRIM (main.Policy_Number))) = src.EDW_Contract_Id
		AND main.Product_System_Code = src.Product_System_Code
	;
		SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

	COMMIT TRAN T1;
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
---
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON


GO
/****** Object:  StoredProcedure [dbo].[usp_Load_Able_Claim_Expense_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_Load_Able_Claim_Expense_Trnx]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS

	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
 --
 BEGIN TRY;
 BEGIN TRAN T1;
 delete from dbo.Able_Claim_Expense_Trnx
 where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear);
--
 SET @DeleteRowCount = @@ROWCOUNT;
 --
 ;WITH w_delta_records
AS
(
SELECT   [Claim_Number]
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
  FROM [dbo].[Able_Claim_Expense]
   where 1=1
   AND month_eftv_date = convert(datetime,@ProcessingMonthYear)
   GROUP BY  [Claim_Number]
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
   except
   SELECT   [Claim_Number]
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
  FROM [dbo].[Able_Claim_Expense]
   where 1=1
   AND month_eftv_date = EOMONTH(DATEADD(month,-1, convert(datetime,@ProcessingMonthYear)))
   GROUP BY  [Claim_Number]
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
)
 insert into dbo.Able_Claim_Expense_Trnx
(	   [Month_Eftv_Date]
      ,[Claim_Number]
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
      ,[Updated_Date_Time]
      ,[Updated_By]
      ,[Process_Log_ID]
      ,[Exclude_Flag]
      ,[Exclude_Reason])
SELECT convert(datetime,@ProcessingMonthYear) [Month_Eftv_Date]
      ,a.[Claim_Number]
      ,a.[Payee]
      ,a.[Invoice_Type]
      ,a.[Invoice_Number]
      ,a.[Amount_inc_GST]
      ,a.[GST]
      ,a.[Payment_Method]
      ,a.[Vendor_Id]
      ,a.[Admin_Initials]
      ,a.[Payment_Creation_Date]
      ,a.[Payment_Reference]
      ,a.[Authorised_By]
      ,a.[Authorisation_Date]
      ,a.[GUID]
      ,GETDATE()
	  ,NULL
	  ,NULL
	  ,@ProcessLogID [Process_Log_ID]
      ,NULL [Exclude_Flag]
	  ,NULL [Exclude_Reason]
  FROM w_delta_records a
   where 1=1
 --
 SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT
	--
	;WITH w_existing_claims
	 AS
	 (
	 select distinct [Able_Claim_Expense_Trnx_Id],[GUID] 
	 from dbo.Able_Claim_Expense_Trnx [nolock] 
	 where month_Eftv_date < convert(datetime,@ProcessingMonthYear)
	 )
	 UPDATE dbo.Able_Claim_Expense_Trnx
	 SET dbo.Able_Claim_Expense_Trnx.[Able_Claim_Expense_Trnx_Id] = src.[Able_Claim_Expense_Trnx_Id],
	     updated_date_time = GETDATE(),
		 Updated_By = system_user
	 FROM dbo.Able_Claim_Expense_Trnx trg,w_existing_claims src
	 where trg.month_Eftv_date = convert(datetime,@ProcessingMonthYear)
	 and trg.[GUID] = src.[GUID]

	 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	 --
	 UPDATE dbo.Able_Claim_Expense_Trnx
	 SET [Able_Claim_Expense_Trnx_Id] = NEXT VALUE FOR [dbo].[Able_Claim_Expense_Trnx_Seq],
	     updated_date_time = GETDATE(),
		 Updated_By = system_user
	 WHERE month_Eftv_date = convert(datetime,@ProcessingMonthYear)
	 and [Able_Claim_Expense_Trnx_Id] IS NULL
	 --
 	 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT

	COMMIT TRAN T1;
	--SELECT @UpdateRowCount = count(*)
	--FROM [dbo].Able_Claim_Expense_Trnx
	--where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	--and updated_date_time > ( SELECT startTime from CTL.ProcessLog where ProcessLogID = @ProcessLogID)
	--
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON





GO
/****** Object:  StoredProcedure [dbo].[usp_Load_GL_Journal_Trnx]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_Load_GL_Journal_Trnx]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS
	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	DECLARE @BatchLogID Int = 0
	DECLARE @MasterBatchLogID Int = 0

	BEGIN TRY;
	--SELECT @BatchLogID = BatchLogID FROM [CTL].[ProcessLog] WHERE ProcessLogID = @ProcessLogID
	--SELECT @MasterBatchLogID = MasterBatchLogID FROM [CTL].[BatchLog] WHERE BatchLogID = @BatchLogID
	BEGIN TRAN T1;
delete from [dbo].[GL_Journal_Trnx]
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear);
--
 SET @DeleteRowCount = @DeleteRowCount + @@ROWCOUNT;
insert into [dbo].[GL_Journal_Trnx]
(      [Month_Eftv_Date]
      ,[Policy_Number]
      ,[EDW_Product_System_Code]
      ,[EDW_Policy_Number]
      ,[EDW_Contract_Id]
      ,[EDW_Contract_Key]
      ,[EDW_Product_Code]
      ,[EDW_Product_Name]
      ,[EDW_Class_of_Business]
      ,[EDW_Product_Key]
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
      ,[GL_Date_Time]
      ,[GL_Journal_Month]
      ,[Pcode]
      ,[Paid_From_Date]
      ,[Paid_To_Date]
      ,[Created_Date_Time]
      ,[Updated_Date_Time]
      ,[Updated_By]
      ,[Import_Log_ID]
      ,[Import_File_Name]
      ,[Process_Log_ID]
      ,[Exclude_Flag]
      ,[Exclude_Reason])
SELECT convert(datetime,@ProcessingMonthYear) [Month_Eftv_Date]
      ,[Policy_Number]
      ,NULL [EDW_Product_System_Code]
      ,NULL [EDW_Policy_Number]
      ,NULL [EDW_Contract_Id]
      ,NULL [EDW_Contract_Key]
      ,NULL [EDW_Product_Code]
      ,NULL [EDW_Product_Name]
      ,NULL [EDW_Class_of_Business]
      ,NULL [EDW_Product_Key]
      ,'GL:'+ [GL_Business_Unit_Id]+':'+[GL_Journal_Id]+':'+format([GL_Journal_Date],'yyyyMMdd','en-AU')+':'+CAST([GL_Journal_Line] as VARCHAR)[Trace_Code]
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
      ,NULL [GL_Date_Time]
      ,[GL_Journal_Month]
      ,[Pcode]
      ,[Paid_From_Date]
      ,[Paid_To_Date]
      ,GETDATE()
	  ,NULL
	  ,NULL	  
      ,[Import_Log_ID]
      ,[Import_File_Name]
      ,@ProcessLogID [Process_Log_ID]
      ,NULL [Exclude_Flag]
      ,NULL [Exclude_Reason]	  
  FROM [STG].[Stg_AMPL_GL_Journal_Trnx]
  where 1=1
   --AND Import_Log_ID IN (SELECT MAX([CTL].[ImportLog].[ImportLogID]) 
	  --                           from  [CTL].[ImportLog],
			--						   [CTL].[PackageLog],
			--						   [CTL].[BatchLog],
			--						   [CTL].[PackageConfig],
			--							[CTL].[FileConfig],
			--							[CTL].[FileLog]
			--				   where [CTL].[ImportLog].PackageLogID = [CTL].[PackageLog].PackageLogID
			--					 and [CTL].[PackageConfig].PackageName = [CTL].[PackageLog].PackageName
			--					 and [CTL].[PackageConfig].TaskName = 'ProcessingMonthYear'
			--					  and [CTL].[PackageLog].BatchLogID = [CTL].[BatchLog].BatchLogID
			--					 and [CTL].[BatchLog].MasterBatchLogID = @MasterBatchLogID
			--					 and [CTL].[FileLog].PackageLogID = [CTL].[PackageLog].PackageLogID
			--					 and ( REPLACE([CTL].[FileConfig].[FileName],'yyyy-mm-dd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyy-MM-dd','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymmdd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyyMMdd','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymm',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue),'yyyyMM','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   )
			--					 --
			--					 and [CTL].[PackageConfig].TaskValue = @ProcessingMonthYear
	  --                           and [CTL].[ImportLog].[TableName] = 'Stg.Stg_AMPL_GL_Journal_Trnx')
  UNION ALL
  SELECT convert(datetime,@ProcessingMonthYear) [Month_Eftv_Date]
      ,[Policy_Number]
      ,NULL [EDW_Product_System_Code]
      ,NULL [EDW_Policy_Number]
      ,NULL [EDW_Contract_Id]
      ,NULL [EDW_Contract_Key]
      ,NULL [EDW_Product_Code]
      ,NULL [EDW_Product_Name]
      ,NULL [EDW_Class_of_Business]
      ,NULL [EDW_Product_Key]
      ,'GL:'+ [GL_Business_Unit_Id]+':'+[GL_Journal_Id]+':'+format([GL_Journal_Date],'yyyyMMdd','en-AU')+':'+CAST([GL_Journal_Line] as VARCHAR)[Trace_Code]
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
      ,[GL_Date_Time]
      ,[GL_Journal_Month]
      ,[Pcode]
      ,[Paid_From_Date]
      ,[Paid_To_Date]
      ,GETDATE()
	  ,NULL
	  ,NULL	  
      ,[Import_Log_ID]
      ,[Import_File_Name]
      ,@ProcessLogID [Process_Log_ID]
      ,NULL [Exclude_Flag]
      ,NULL [Exclude_Reason]	  
  FROM [STG].[Stg_AMPN_GL_Journal_Trnx]
  where 1=1
  --AND Import_Log_ID IN (SELECT MAX([CTL].[ImportLog].[ImportLogID]) 
	 --                            from  [CTL].[ImportLog],
		--							   [CTL].[PackageLog],
		--							   [CTL].[BatchLog],
		--							   [CTL].[PackageConfig],
		--								[CTL].[FileConfig],
		--								[CTL].[FileLog]
		--					   where [CTL].[ImportLog].PackageLogID = [CTL].[PackageLog].PackageLogID
		--						 and [CTL].[PackageConfig].PackageName = [CTL].[PackageLog].PackageName
		--						 and [CTL].[PackageConfig].TaskName = 'ProcessingMonthYear'
		--						  and [CTL].[PackageLog].BatchLogID = [CTL].[BatchLog].BatchLogID
		--						 and [CTL].[BatchLog].MasterBatchLogID = @MasterBatchLogID
		--						 and [CTL].[FileLog].PackageLogID = [CTL].[PackageLog].PackageLogID
		--						 and ( REPLACE([CTL].[FileConfig].[FileName],'yyyy-mm-dd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyy-MM-dd','en-AU') ) = [CTL].[FileLog].[FileName]
		--						   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymmdd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyyMMdd','en-AU') ) = [CTL].[FileLog].[FileName]
		--						   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymm',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue),'yyyyMM','en-AU') ) = [CTL].[FileLog].[FileName]
		--						   )
		--						 --
		--						 and [CTL].[PackageConfig].TaskValue = @ProcessingMonthYear
	 --                            and [CTL].[ImportLog].[TableName] = 'Stg.Stg_AMPN_GL_Journal_Trnx');
 --
 SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT
	--
	exec [dbo].[usp_Remove_Garbage_Characters] @ProcessLogID,'dbo','GL_Journal_Trnx'
	--
	COMMIT TRAN T1;
	SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	--SELECT @UpdateRowCount = count(*)
	--FROM [dbo].[GL_Journal_Trnx]
	--where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	--and updated_date_time > ( SELECT startTime from CTL.ProcessLog where ProcessLogID = @ProcessLogID)
	--
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
SET ANSI_NULLS ON





GO
/****** Object:  StoredProcedure [dbo].[usp_Load_Mercury_Transactions]    Script Date: 1/05/2022 7:15:09 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- RRNAA2
-- Opus Claims Payment Enhancement Change
-- RGU-575 - 23/06/2020
-- The change involves identification of Opus records in the columns [Product_System_Code] and Policy_Number


CREATE PROCEDURE [dbo].[usp_Load_Mercury_Transactions]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS

	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	--DECLARE @BatchLogID Int = 0
	--DECLARE @MasterBatchLogID Int = 0
 --
 BEGIN TRY;
 --SELECT @BatchLogID = BatchLogID FROM [CTL].[ProcessLog] WHERE ProcessLogID = @ProcessLogID
 --SELECT @MasterBatchLogID = MasterBatchLogID FROM [CTL].[BatchLog] WHERE BatchLogID = @BatchLogID
 BEGIN TRAN T1;
 delete from dbo.mercury_trnx
 where [Month_Effective_Date] = convert(datetime,@ProcessingMonthYear);
--
 SET @DeleteRowCount = @DeleteRowCount + @@ROWCOUNT;
 insert into dbo.mercury_trnx
(	   [Month_Effective_Date]
      ,[Product_System_Code]
      ,[Policy_Number]
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
      ,[Transaction_Amount]
      ,[Transaction_Process_Date]
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
      ,[Updated_Date_Time]
      ,[Updated_By]
      ,[Import_Log_ID]
      ,[Import_File_Name]
      ,[Process_Log_ID]
      ,[Exclude_Flag]
      ,[Exclude_Reason])
SELECT  convert(datetime,@ProcessingMonthYear) eftv_date,       
       case [Admin_System_Id] when 'CLAS' then 'FDA' when 'INCOM' then 'OR' when 'ULTIMATE' then 'CP' when 'ULTIMAAS' then 'U2' when 'PBSS' then 'LS' end Product_System_code,
	   case when [Admin_System_Id] = 'ULTIMAAS' and [Client_Ref_Id_4] is null and client_ref_id_2 = 'FLSPLAN' 
	   then client_ref_id_1
	   else 
	   case [Admin_System_Id] when 'CLAS' then [Client_Ref_Id_2] when 'INCOM' then [Client_Ref_Id_1] when 'ULTIMATE' then [Client_Ref_Id_1] 
	   when 'ULTIMAAS' then [Client_Ref_Id_4] when 'PBSS' then [Client_Ref_Id_1] end 
	   end Policy_Number,
	   'MR:'+CAST([Admin_System_File_Id] as varchar)+':'+CAST([Admin_System_Id] as varchar) +':'+CAST([File_Record_Number] as varchar) +':'
	   +LEFT(CONVERT(VARCHAR, [GL_Posting_Date], 120), 10)+':'+CAST(ISNULL([Stat_Fund],'-') as varchar) +':'+CAST(ISNULL([Sequence_Number],'-') as varchar) +':'+ CAST(ISNULL([Primary_Secondary_Ind],'-') as varchar)  trace_code,
	   --'MR:'+CAST([Admin_System_File_Id] as varchar)+':'+CAST([Admin_System_Id]as varchar) +':'+CAST([File_Record_Number]as varchar) +':'
	   --+LEFT(CONVERT(VARCHAR, [GL_Posting_Date], 120), 10)+':'+CAST([Stat_Fund]as varchar) +':'+CAST([Sequence_Number]as varchar) +':'+ CAST([Primary_Secondary_Ind] as varchar)  trace_code,
	   [Admin_System_File_Id]
      ,[Admin_System_Id]
      ,[File_Record_Number]
	  ,[GL_Posting_Date]
      ,ISNULL([Stat_Fund],'-') as [Stat_Fund]
      ,ISNULL([Sequence_Number],'-') as [Sequence_Number]
      ,ISNULL([Primary_Secondary_Ind],'-') as [Primary_Secondary_Ind]
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
      ,[Transaction_Amount]
      ,[Transaction_Process_Date]
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
      ,GETDATE()
	  ,NULL
	  ,NULL
	  ,[Import_Log_ID]
	  ,[Import_File_Name]
	  ,@ProcessLogID [Process_Log_ID]
      ,NULL
	  ,NULL
  FROM [STG].[Stg_Mercury_Trnx]
   where 1=1
   --AND Import_Log_ID IN (SELECT MAX([CTL].[ImportLog].[ImportLogID]) 
	  --                           from  [CTL].[ImportLog],
			--						   [CTL].[PackageLog],
			--						   [CTL].[BatchLog],
			--						   [CTL].[PackageConfig],
			--							[CTL].[FileConfig],
			--							[CTL].[FileLog]
			--				   where [CTL].[ImportLog].PackageLogID = [CTL].[PackageLog].PackageLogID
			--					 and [CTL].[PackageConfig].PackageName = [CTL].[PackageLog].PackageName
			--					 and [CTL].[PackageConfig].TaskName = 'ProcessingMonthYear'
			--					 and [CTL].[PackageLog].BatchLogID = [CTL].[BatchLog].BatchLogID
			--					 and [CTL].[BatchLog].MasterBatchLogID = @MasterBatchLogID
			--					 and [CTL].[FileLog].PackageLogID = [CTL].[PackageLog].PackageLogID
			--					 and ( REPLACE([CTL].[FileConfig].[FileName],'yyyy-mm-dd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyy-MM-dd','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymmdd',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue)+1,'yyyyMMdd','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   OR REPLACE([CTL].[FileConfig].[FileName],'yyyymm',format(Convert(datetime,[CTL].[PackageConfig].Taskvalue),'yyyyMM','en-AU') ) = [CTL].[FileLog].[FileName]
			--					   )

			--					 --
			--					 and [CTL].[PackageConfig].TaskValue = @ProcessingMonthYear
	  --                           and [CTL].[ImportLog].[TableName] = 'Stg.Stg_Mercury_Trnx')
 --
 SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT
	--
	--
	exec [dbo].[usp_Remove_Garbage_Characters] @ProcessLogID,'dbo','Mercury_Trnx'
	--
	COMMIT TRAN T1;
	--
	--SELECT @UpdateRowCount = count(*)
	--FROM [dbo].Mercury_Trnx
	--where [Month_Effective_Date] = convert(datetime,@ProcessingMonthYear)
	--and updated_date_time > ( SELECT startTime from CTL.ProcessLog where ProcessLogID = @ProcessLogID)
	--
	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH

SET ANSI_NULLS ON


GO
