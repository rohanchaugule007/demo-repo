-- Master script for database payments
-- Just checking if i can get all the objects along with the defination out in one script.

-- daily task is to generate scripts of reinsurance database. 
USE [master]
GO
/****** Object:  Database [Payments]    Script Date: 25-05-2022 00:38:51 ******/
CREATE DATABASE [Payments]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Payments', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\Payments.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'Payments_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\Payments_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT
GO
ALTER DATABASE [Payments] SET COMPATIBILITY_LEVEL = 150
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [Payments].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [Payments] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [Payments] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [Payments] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [Payments] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [Payments] SET ARITHABORT OFF 
GO
ALTER DATABASE [Payments] SET AUTO_CLOSE ON 
GO
ALTER DATABASE [Payments] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [Payments] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [Payments] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [Payments] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [Payments] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [Payments] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [Payments] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [Payments] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [Payments] SET  ENABLE_BROKER 
GO
ALTER DATABASE [Payments] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [Payments] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [Payments] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [Payments] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [Payments] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [Payments] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [Payments] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [Payments] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [Payments] SET  MULTI_USER 
GO
ALTER DATABASE [Payments] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [Payments] SET DB_CHAINING OFF 
GO
ALTER DATABASE [Payments] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [Payments] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [Payments] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [Payments] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO
ALTER DATABASE [Payments] SET QUERY_STORE = OFF
GO
USE [Payments]
GO
/****** Object:  User [ISTPL\rohan.chaugule]    Script Date: 25-05-2022 00:38:51 ******/
CREATE USER [ISTPL\rohan.chaugule] FOR LOGIN [ISTPL\rohan.chaugule] WITH DEFAULT_SCHEMA=[dbo]
GO
ALTER ROLE [db_owner] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_accessadmin] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_securityadmin] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_ddladmin] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_backupoperator] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_datareader] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_datawriter] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_denydatareader] ADD MEMBER [ISTPL\rohan.chaugule]
GO
ALTER ROLE [db_denydatawriter] ADD MEMBER [ISTPL\rohan.chaugule]
GO
/****** Object:  Schema [IDS]    Script Date: 25-05-2022 00:38:51 ******/
CREATE SCHEMA [IDS]
GO
/****** Object:  Schema [LND]    Script Date: 25-05-2022 00:38:51 ******/
CREATE SCHEMA [LND]
GO
/****** Object:  Schema [RNS]    Script Date: 25-05-2022 00:38:51 ******/
CREATE SCHEMA [RNS]
GO
/****** Object:  Schema [stg]    Script Date: 25-05-2022 00:38:51 ******/
CREATE SCHEMA [stg]
GO
/****** Object:  Table [dbo].[Able_Claim_Benefit]    Script Date: 25-05-2022 00:38:51 ******/
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
/****** Object:  Table [dbo].[able_trnx_sample]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[able_trnx_sample](
	[Month_Eftv_Date] [varchar](50) NULL,
	[Claim_Expense_Id] [varchar](50) NULL,
	[Claim_Number] [varchar](50) NULL,
	[Payee] [varchar](50) NULL,
	[Policy_Number] [varchar](50) NULL,
	[Invoice_Type] [varchar](50) NULL,
	[Invoice_Number] [varchar](50) NULL,
	[Amount_inc_GST] [varchar](50) NULL,
	[GST] [varchar](50) NULL,
	[Payment_Method] [varchar](50) NULL,
	[Vendor_Id] [varchar](50) NULL,
	[Admin_Initials] [varchar](50) NULL,
	[Payment_Creation_Date] [varchar](50) NULL,
	[Payment_Reference] [varchar](50) NULL,
	[Authorised_By] [varchar](50) NULL,
	[Authorisation_Date] [varchar](50) NULL,
	[Lumpsum_IP_Indicator] [varchar](50) NULL,
	[Claim_Type] [varchar](50) NULL,
	[Class_of_Business] [varchar](50) NULL,
	[Benefit_Code] [varchar](50) NULL,
	[Product_Code] [varchar](50) NULL,
	[Target_Benefit_Type] [varchar](50) NULL,
	[GUID] [varchar](50) NULL,
	[Created_Date_Time] [varchar](50) NULL,
	[Updated_Date_Time] [varchar](50) NULL,
	[Updated_By] [varchar](50) NULL,
	[Import_Log_ID] [varchar](50) NULL,
	[Import_File_Name] [varchar](50) NULL,
	[Process_Log_ID] [varchar](50) NULL,
	[Exclude_Flag] [varchar](50) NULL,
	[Exclude_Reason] [varchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[cd_history]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cd_history](
	[Approved  ] [varchar](450) NULL,
	[Approved (child)] [varchar](50) NULL,
	[Company] [varchar](50) NULL,
	[Company_1] [varchar](50) NULL,
	[Created  ] [varchar](50) NULL,
	[Created (child)] [varchar](50) NULL,
	[Customer Item] [varchar](50) NULL,
	[Customer Item (child)] [varchar](50) NULL,
	[Customer Item_4] [varchar](450) NULL,
	[Delivered Quantity] [varchar](50) NULL,
	[Delivered Quantity (child)] [varchar](50) NULL,
	[Due 1-7 Days  ] [varchar](450) NULL,
	[Due 1-7 Days (child)] [varchar](450) NULL,
	[Due Beyond 7 Days  ] [varchar](50) NULL,
	[Due Beyond 7 Days (child)] [varchar](50) NULL,
	[Due Today  ] [varchar](50) NULL,
	[Due Today (child)] [varchar](50) NULL,
	[End Date] [varchar](50) NULL,
	[End Date (child)] [varchar](50) NULL,
	[Exceptions  ] [varchar](50) NULL,
	[Exceptions (child)] [varchar](50) NULL,
	[Internal Sales Representative] [varchar](50) NULL,
	[Internal Sales Representative (child)] [varchar](50) NULL,
	[Item] [varchar](50) NULL,
	[Item (child)] [varchar](50) NULL,
	[Item (child) (child)] [varchar](50) NULL,
	[Item (child)_3] [varchar](50) NULL,
	[Item_2] [varchar](50) NULL,
	[Last Delivery Date] [varchar](50) NULL,
	[Last Delivery Date (child)] [varchar](50) NULL,
	[Line] [varchar](50) NULL,
	[Order Date] [varchar](50) NULL,
	[Order Date (child)] [varchar](50) NULL,
	[Ordered Quantity] [varchar](50) NULL,
	[Ordered Quantity (child)] [varchar](50) NULL,
	[Past Due  ] [varchar](50) NULL,
	[Past Due (child)] [varchar](50) NULL,
	[Past Due Partially  ] [varchar](50) NULL,
	[Past Due Partially (child)] [varchar](50) NULL,
	[Plnd Rct Date] [varchar](50) NULL,
	[Plnd Rct Date (child)] [varchar](50) NULL,
	[Processed  ] [varchar](50) NULL,
	[Processed (child)] [varchar](50) NULL,
	[Received date] [varchar](50) NULL,
	[Rev ] [varchar](50) NULL,
	[Sales Office] [varchar](50) NULL,
	[Sales Office (child)] [varchar](50) NULL,
	[Schedule] [varchar](50) NULL,
	[Schedule Lines] [varchar](50) NULL,
	[Schedule Type] [varchar](50) NULL,
	[Ship-to Business Partner] [varchar](50) NULL,
	[Ship-to Business Partner (child)] [varchar](50) NULL,
	[Ship-to Business Partner_6] [varchar](50) NULL,
	[Sold-to Business Partner] [varchar](50) NULL,
	[Sold-to Business Partner (child)] [varchar](50) NULL,
	[Sold-to Business Partner_5] [varchar](50) NULL,
	[Start Date] [varchar](50) NULL,
	[Start Date (child)] [varchar](50) NULL,
	[Start Date Due] [varchar](50) NULL,
	[Status] [varchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [IDS].[Able_Claim_Benefit]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [IDS].[Able_Claim_Benefit](
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
/****** Object:  Table [IDS].[Able_Claim_Expense]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [IDS].[Able_Claim_Expense](
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
/****** Object:  Table [IDS].[Able_Claim_Expense_Trnx]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [IDS].[Able_Claim_Expense_Trnx](
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
/****** Object:  Table [IDS].[Stg_AMPL_GL_Journal_Trnx]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [IDS].[Stg_AMPL_GL_Journal_Trnx](
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
/****** Object:  Table [IDS].[STG_EXPENSE_MERCURY]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [IDS].[STG_EXPENSE_MERCURY](
	[STG_EXPENSE_MERCURY_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[Admin_System_File_Id] [numeric](10, 0) NULL,
	[Admin_System_Id] [varchar](15) NULL,
	[File_Record_Number] [numeric](10, 0) NULL,
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
	[GL_Posting_Date] [datetime] NULL,
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
	[Stat_Fund] [varchar](1) NULL,
	[Sequence_Number] [varchar](1) NULL,
	[Primary_Secondary_Ind] [varchar](1) NULL,
	[Created_Date_Time] [datetime] NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[ACCTNG_CENSUS_DATE] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[STG_EXPENSE_MERCURY_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [LND].[able_trnx_sample]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [LND].[able_trnx_sample](
	[Month_Eftv_Date] [varchar](500) NULL,
	[Claim_Expense_Id] [varchar](500) NULL,
	[Claim_Number] [varchar](500) NULL,
	[Payee] [varchar](500) NULL,
	[Policy_Number] [varchar](500) NULL,
	[Invoice_Type] [varchar](500) NULL,
	[Invoice_Number] [varchar](500) NULL,
	[Amount_inc_GST] [varchar](500) NULL,
	[GST] [varchar](500) NULL,
	[Payment_Method] [varchar](500) NULL,
	[Vendor_Id] [varchar](500) NULL,
	[Admin_Initials] [varchar](500) NULL,
	[Payment_Creation_Date] [varchar](500) NULL,
	[Payment_Reference] [varchar](500) NULL,
	[Authorised_By] [varchar](500) NULL,
	[Authorisation_Date] [varchar](500) NULL,
	[Lumpsum_IP_Indicator] [varchar](500) NULL,
	[Claim_Type] [varchar](500) NULL,
	[Class_of_Business] [varchar](500) NULL,
	[Benefit_Code] [varchar](500) NULL,
	[Product_Code] [varchar](500) NULL,
	[Target_Benefit_Type] [varchar](500) NULL,
	[GUID] [varchar](500) NULL,
	[Created_Date_Time] [varchar](500) NULL,
	[Updated_Date_Time] [varchar](500) NULL,
	[Updated_By] [varchar](500) NULL,
	[Import_Log_ID] [varchar](500) NULL,
	[Import_File_Name] [varchar](500) NULL,
	[Process_Log_ID] [varchar](500) NULL,
	[Exclude_Flag] [varchar](500) NULL,
	[Exclude_Reason] [varchar](500) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [LND].[gl_trnx_sample]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [LND].[gl_trnx_sample](
	[GL_Business_Unit_Id] [varchar](500) NULL,
	[GL_Journal_Id] [varchar](500) NULL,
	[GL_Journal_Date] [varchar](500) NULL,
	[GL_Journal_Line] [varchar](500) NULL,
	[GL_Ledger_Code] [varchar](500) NULL,
	[GL_Account_Id] [varchar](500) NULL,
	[GL_Department_Id] [varchar](500) NULL,
	[GL_Product_Id] [varchar](500) NULL,
	[GL_Project_Id] [varchar](500) NULL,
	[GL_Affiliate_Id] [varchar](500) NULL,
	[GL_Currency_Code] [varchar](500) NULL,
	[GL_Monetary_Amount] [varchar](500) NULL,
	[GL_Posting_Ref_1] [varchar](500) NULL,
	[GL_Journal_Line_Desc] [varchar](500) NULL,
	[Status_Ref_1] [varchar](500) NULL,
	[Source_System_Ref_1] [varchar](500) NULL,
	[Source_System_Ref_2] [varchar](500) NULL,
	[GL_Posting_Ref_2] [varchar](500) NULL,
	[Status_Ref_2] [varchar](500) NULL,
	[GL_Posting_Date] [varchar](500) NULL,
	[User_Id] [varchar](500) NULL,
	[Source_System_Narrative_1] [varchar](500) NULL,
	[GL_Statistic_Amount] [varchar](500) NULL,
	[GL_Foreign_Currency_Code] [varchar](500) NULL,
	[GL_Foreign_Amount] [varchar](500) NULL,
	[GL_Journal_Month] [varchar](500) NULL,
	[Pcode] [varchar](500) NULL,
	[Policy_Number] [varchar](500) NULL,
	[Paid_From_Date] [varchar](500) NULL,
	[Paid_To_Date] [varchar](500) NULL,
	[Created_Date_Time] [varchar](500) NULL,
	[Import_Log_ID] [varchar](500) NULL,
	[Import_File_Name] [varchar](500) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [LND].[merc_trnx_sample]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [LND].[merc_trnx_sample](
	[Admin_System_File_Id] [varchar](500) NULL,
	[Admin_System_Id] [varchar](500) NULL,
	[File_Record_Number] [varchar](500) NULL,
	[Client_Ref_Id_1] [varchar](500) NULL,
	[Client_Ref_Id_2] [varchar](500) NULL,
	[Client_Ref_Id_3] [varchar](500) NULL,
	[Client_Ref_Id_4] [varchar](500) NULL,
	[Trans_Ref_Key_2] [varchar](500) NULL,
	[Trans_Ref_Key_3] [varchar](500) NULL,
	[Trans_Ref_Key_4] [varchar](500) NULL,
	[Trans_Ref_Key_5] [varchar](500) NULL,
	[Trans_Ref_Key_6] [varchar](500) NULL,
	[GL_Business_Unit_Id] [varchar](500) NULL,
	[GL_Account_Id] [varchar](500) NULL,
	[GL_Department_Id] [varchar](500) NULL,
	[GL_Product_Code] [varchar](500) NULL,
	[GL_Project_Id] [varchar](500) NULL,
	[GL_Affiliate_Id] [varchar](500) NULL,
	[GL_Posting_Date] [varchar](500) NULL,
	[GL_Posting_Ref_Text] [varchar](500) NULL,
	[Ledger_Movement_Amount] [varchar](500) NULL,
	[GL_Posting_File_Id] [varchar](500) NULL,
	[Trans_Ref_Key_1] [varchar](500) NULL,
	[Client_Ref_Id_5] [varchar](500) NULL,
	[Product_Ref_Key_1] [varchar](500) NULL,
	[Product_Ref_Key_2] [varchar](500) NULL,
	[Product_Ref_Key_3] [varchar](500) NULL,
	[Bus_Line_Ref_Key_1] [varchar](500) NULL,
	[Bus_Line_Ref_Key_2] [varchar](500) NULL,
	[Bus_Line_Ref_Key_3] [varchar](500) NULL,
	[Business_Line_Location] [varchar](500) NULL,
	[Business_Line_Planner_Id] [varchar](500) NULL,
	[Business_Tax_Classn] [varchar](500) NULL,
	[Stat_Fund_Code] [varchar](500) NULL,
	[Investment_Pool_Id] [varchar](500) NULL,
	[Off_Investment_Pool_Id] [varchar](500) NULL,
	[Transaction_Amount] [varchar](500) NULL,
	[Transaction_Process_Date] [varchar](500) NULL,
	[Investment_Effective_Date] [varchar](500) NULL,
	[Movement_Effective_Date] [varchar](500) NULL,
	[GL_Currency_Code] [varchar](500) NULL,
	[Trans_Ref_Key_7] [varchar](500) NULL,
	[Trans_Ref_Key_8] [varchar](500) NULL,
	[Trans_Ref_Key_9] [varchar](500) NULL,
	[Trans_Ref_Key_10] [varchar](500) NULL,
	[Trans_Ref_Key_11] [varchar](500) NULL,
	[Business_Product_Line_Code] [varchar](500) NULL,
	[Business_Product_Owner_Code] [varchar](500) NULL,
	[Business_Product_Code] [varchar](500) NULL,
	[Business_Line_Code] [varchar](500) NULL,
	[Business_Trans_Type] [varchar](500) NULL,
	[Source_Sys_Reference_1] [varchar](500) NULL,
	[Source_Sys_Narrative_1] [varchar](500) NULL,
	[Source_Sys_Reference_2] [varchar](500) NULL,
	[Source_Sys_Narrative_2] [varchar](500) NULL,
	[Admin_File_Ref_Text] [varchar](500) NULL,
	[Stat_Fund] [varchar](500) NULL,
	[Sequence_Number] [varchar](500) NULL,
	[Primary_Secondary_Ind] [varchar](500) NULL,
	[Created_Date_Time] [varchar](500) NULL,
	[Import_Log_ID] [varchar](500) NULL,
	[Import_File_Name] [varchar](500) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [RNS].[Reins_Expense_Able]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [RNS].[Reins_Expense_Able](
	[Reins_Expense_Able_Id] [int] NULL,
	[ACCTNG_CENSUS_DATE] [datetime] NOT NULL,
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
/****** Object:  Table [RNS].[Reins_Expense_GL_AMPL]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [RNS].[Reins_Expense_GL_AMPL](
	[Reins_Expense_GL_AMPL_Id] [int] IDENTITY(1,1) NOT NULL,
	[ACCTNG_CENSUS_DATE] [datetime] NOT NULL,
	[Policy_Number] [varchar](50) NULL,
	[EDW_Product_System_Code] [varchar](3) NULL,
	[EDW_Policy_Number] [varchar](50) NULL,
	[EDW_Contract_Id] [varchar](50) NULL,
	[EDW_Contract_Key] [numeric](10, 0) NULL,
	[EDW_Product_Code] [varchar](20) NULL,
	[EDW_Product_Name] [varchar](100) NULL,
	[EDW_Class_of_Business] [varchar](50) NULL,
	[EDW_Product_Key] [numeric](10, 0) NULL,
	[Reins_Contract_ID] [varchar](3) NULL,
	[Reins_Policy_Number] [varchar](50) NULL,
	[Contract_Id] [varchar](50) NULL,
	[Reins_Contract_Start_Date] [numeric](10, 0) NULL,
	[Reins_Class_Of_Business] [varchar](20) NULL,
	[Reins_Product_Key] [varchar](100) NULL,
	[Reins_Product_Code] [varchar](50) NULL,
	[Reins_Product_Name] [numeric](10, 0) NULL,
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
	[EDW_Country_Code] [varchar](3) NULL,
PRIMARY KEY CLUSTERED 
(
	[Reins_Expense_GL_AMPL_Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [RNS].[RNS.Reins_Expense_Mercury]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [RNS].[RNS.Reins_Expense_Mercury](
	[Reins_Expense_Mercury_Id] [int] IDENTITY(1,1) NOT NULL,
	[ACCTNG_CENSUS_DATE] [datetime] NOT NULL,
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
	[Reins_Contract_ID] [numeric](10, 0) NULL,
	[Reins_Policy_Number] [varchar](50) NULL,
	[Contract_Id] [varchar](50) NULL,
	[Reins_Contract_Start_Date] [datetime] NULL,
	[Reins_Class_Of_Business] [varchar](50) NULL,
	[Reins_Product_Key] [numeric](10, 0) NULL,
	[Reins_Product_Code] [varchar](20) NULL,
	[Reins_Product_Name] [varchar](100) NULL,
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
	[EDW_Country_Code] [varchar](3) NULL,
PRIMARY KEY CLUSTERED 
(
	[Reins_Expense_Mercury_Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[STG_EXPENSE_ABLE]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[STG_EXPENSE_ABLE](
	[STG_EXPENSE_ABLE_ID] [bigint] IDENTITY(1,1) NOT NULL,
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
	[Exclude_Reason] [varchar](255) NULL,
	[ACCTNG_CENSUS_DATE] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[STG_EXPENSE_ABLE_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[STG_EXPENSE_GL_AMPL]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[STG_EXPENSE_GL_AMPL](
	[STG_EXPENSE_GL_AMPL_ID] [bigint] IDENTITY(1,1) NOT NULL,
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
	[Import_File_Name] [varchar](500) NOT NULL,
	[ACCTNG_CENSUS_DATE] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[STG_EXPENSE_GL_AMPL_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [stg].[STG_EXPENSE_MERCURY]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stg].[STG_EXPENSE_MERCURY](
	[STG_EXPENSE_MERCURY_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[Admin_System_File_Id] [numeric](10, 0) NULL,
	[Admin_System_Id] [varchar](15) NULL,
	[File_Record_Number] [numeric](10, 0) NULL,
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
	[GL_Posting_Date] [datetime] NULL,
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
	[Stat_Fund] [varchar](1) NULL,
	[Sequence_Number] [varchar](1) NULL,
	[Primary_Secondary_Ind] [varchar](1) NULL,
	[Created_Date_Time] [datetime] NULL,
	[Import_Log_ID] [int] NOT NULL,
	[Import_File_Name] [varchar](500) NOT NULL,
	[ACCTNG_CENSUS_DATE] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[STG_EXPENSE_MERCURY_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [IDS].[LND_TRANSFER_STG_ABLE]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [IDS].[LND_TRANSFER_STG_ABLE]
AS
BEGIN 
insert into [IDS].[Able_Claim_Expense]
( 	
		[Month_Eftv_Date] 
	--	,[Claim_Expense_Id] 
		,[Claim_Number] 
		,[Payee] 
		,[Policy_Number] 
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
		,[Lumpsum_IP_Indicator] 
		,[Claim_Type] 
		,[Class_of_Business] 
		,[Benefit_Code] 
		,[Product_Code] 
		,[Target_Benefit_Type] 
		,[GUID] 
		,[Created_Date_Time] 
		,[Updated_Date_Time] 
		,[Updated_By] 
		,[Import_Log_ID] 
		,[Import_File_Name] 
		,[Process_Log_ID] 
		,[Exclude_Flag] 
		,[Exclude_Reason] 

)
select 
	 CAST([Month_Eftv_Date] as datetime) as [Month_Eftv_Date]
--	,CAST([Claim_Expense_Id] as int) as [Claim_Expense_Id]
	,[Claim_Number] 
	,[Payee] 
	,[Policy_Number] 
	,[Invoice_Type] 
	,[Invoice_Number] 
	,CAST([Amount_inc_GST] as numeric) as [Amount_inc_GST]
	,CAST([GST] as numeric) as [GST]
	,[Payment_Method] 
	,[Vendor_Id] 
	,[Admin_Initials] 
	,CAST([Payment_Creation_Date] as datetime) as [Payment_Creation_Date]
	,[Payment_Reference] 
	,[Authorised_By]
	,CAST([Authorisation_Date] as datetime) as [Authorisation_Date]
	,[Lumpsum_IP_Indicator] 
	,[Claim_Type] 
	,[Class_of_Business] 
	,[Benefit_Code] 
	,[Product_Code] 
	,[Target_Benefit_Type] 
	,CAST([GUID] as NUMERIC) as [GUID]
	,[Created_Date_Time]
	,CAST([Updated_Date_Time] as datetime) as [Updated_Date_Time]
	,[Updated_By] 
	,CAST([Import_Log_ID]  as int) as [Import_Log_ID]
	,[Import_File_Name] 
	,CAST([Process_Log_ID] as int) as [Process_Log_ID]
	,[Exclude_Flag] 
	,[Exclude_Reason] 

  FROM [LND].[able_trnx_sample]
   where 1=1
END 

--Exec [IDS].[LND_TRANSFER_STG_ABLE]
GO
/****** Object:  StoredProcedure [IDS].[LND_TRANSFER_STG_GL]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [IDS].[LND_TRANSFER_STG_GL]
AS
BEGIN 
insert into [IDS].[Stg_AMPL_GL_Journal_Trnx]
( 	
	[GL_Business_Unit_Id] 
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
	,[Policy_Number] 
	,[Paid_From_Date] 
	,[Paid_To_Date] 
	--,[Created_Date_Time] 
	,[Import_Log_ID] 
	,[Import_File_Name] 
)
select 
	[GL_Business_Unit_Id] 
	,[GL_Journal_Id] 
	,CAST(right(left([GL_Journal_Date],10),4) + '-' + right(left(GL_Journal_Date,5),2) + '-' + left(GL_Journal_Date,2) as datetime) as [GL_Journal_Date]
	,CAST([GL_Journal_Line] as NUMERIC) as [GL_Journal_Line]
	,[GL_Ledger_Code] 
	,[GL_Account_Id]
	,[GL_Department_Id] 
	,[GL_Product_Id] 
	,[GL_Project_Id] 
	,[GL_Affiliate_Id]
	,[GL_Currency_Code] 
	,CAST([GL_Monetary_Amount] as NUMERIC) as [GL_Monetary_Amount]
	,[GL_Posting_Ref_1] 
	,[GL_Journal_Line_Desc] 
	,[Status_Ref_1] 
	,[Source_System_Ref_1] 
	,[Source_System_Ref_2] 
	,[GL_Posting_Ref_2] 
	,[Status_Ref_2] 
	,CAST(right(left([GL_Posting_Date],10),4) + '-' + right(left(GL_Posting_Date,5),2) + '-' + left(GL_Posting_Date,2) as datetime) as [GL_Posting_Date]
	,[User_Id] 
	,[Source_System_Narrative_1] 
	,CAST([GL_Statistic_Amount] as NUMERIC) as [GL_Statistic_Amount]
	,[GL_Foreign_Currency_Code] 
	,CAST([GL_Foreign_Amount] as NUMERIC) as [GL_Foreign_Amount]
	,[GL_Journal_Month] 
	,[Pcode] 
	,[Policy_Number]
	,CAST(right(left([Paid_From_Date],10),4) + '-' + right(left(Paid_From_Date,5),2) + '-' + left(Paid_From_Date,2) as datetime) as [Paid_From_Date]
	,CAST(right(left([Paid_To_Date],10),4) + '-' + right(left([Paid_To_Date],5),2) + '-' + left([Paid_To_Date],2) as datetime) as [Paid_To_Date]
--	,CAST(right(left([Created_Date_Time],10),4) + '-' + right(left([Created_Date_Time],5),2) + '-' + left([Created_Date_Time],2) as datetime) as [Created_Date_Time]
	,CAST([Import_Log_ID] as int) as [Import_Log_ID]
	,[Import_File_Name] 

 FROM [LND].[gl_trnx_sample]
   where 1=1
 END 
GO
/****** Object:  StoredProcedure [IDS].[LND_TRANSFER_STG_MERC]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [IDS].[LND_TRANSFER_STG_MERC]
AS
BEGIN 
insert into [IDS].[STG_EXPENSE_MERCURY]
( 	[Admin_System_File_Id] 
	,[Admin_System_Id] 
	,[File_Record_Number]
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
	,[GL_Posting_Date] 
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
	,[Stat_Fund] 
	,[Sequence_Number] 
	,[Primary_Secondary_Ind] 
	,[Created_Date_Time] 
	,[Import_Log_ID] 
	,[Import_File_Name] 	
)
select 
	Cast([Admin_System_File_Id] as NUMERIC(10,0)) as [Admin_System_File_Id]
	,[Admin_System_Id] 
	,Cast([File_Record_Number] as NUMERIC(10,0)) as [File Record Number]
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
	,Cast([GL_Posting_Date] as datetime) as [GL_Posting_Date]
	,[GL_Posting_Ref_Text] 
	,CAST([Ledger_Movement_Amount] as NUMERIC(10,0)) as [Ledger_Movement_Amount] 
	,CAST([GL_Posting_File_Id] as NUMERIC(10,0)) as [GL_Posting_File_Id]
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
	,CAST([Transaction_Process_Date] as datetime) as [Transaction_Process_Date]
	,CAST([Investment_Effective_Date] as datetime) as [Investment_Effective_Date]
	,CAST([Movement_Effective_Date] as datetime) as [Movement_Effective_Date]
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
	,[Stat_Fund] 
	,[Sequence_Number] 
	,[Primary_Secondary_Ind] 
	,CAST([Created_Date_Time] as datetime) as [Created_Date_Time] 
	,CAST([Import_Log_ID] as int) as [Import_Log_ID]
	,[Import_File_Name] 

  FROM [LND].[merc_trnx_sample]
   where 1=1
END
GO
/****** Object:  StoredProcedure [IDS].[Load_Sample_Values]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Proc [IDS].[Load_Sample_Values]
@Table_Name Varchar(200)--, @file_path nvarchar(255)
As
Begin

declare 
		@s_name sysname = N'RNS',
		--@t_name sysname,
		@delimiter nchar(1)= ',',
		 --@BulkIns_query nvarchar(max),
		 @Truncquery nvarchar(max)



Set @Truncquery = 'Truncate Table IDS.' +@Table_Name
Exec (@Truncquery)
--truncate table RNS.REINS_CONTRACT_RISK_INFORCE

Bulk Insert [IDS].[Able_Claim_Expense]								-- change Table Name
From 'C:\Users\rohan.chaugule\Pictures\Payment\IDS\ABLE_original\able_trnx_sample.csv' -- change Table Name
With (Fieldterminator = ',', rowterminator = '\n', Firstrow = 2)

END;
GO
/****** Object:  StoredProcedure [IDS].[usp_Load_Able_Claim_Expense_Trnx]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [IDS].[usp_Load_Able_Claim_Expense_Trnx]
  @ProcessLogID Int
  ,@ProcessingMonthYear NVARCHAR(100)
AS

	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
 --
 BEGIN TRY;
 BEGIN TRAN T1;
 delete from IDS.Able_Claim_Expense_Trnx						--- to avoid two sets of data we delete in case data is present
 where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear);
--
 SET @DeleteRowCount = @@ROWCOUNT;                               ---@@ global variable
 --
 ;WITH w_delta_records												--- cte (select, group by, except, select)
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
  FROM [IDS].[Able_Claim_Expense]												---Source
   where 1=1																	--- True Condition
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
   except																				--- except the data for last month
   SELECT   [Claim_Number]																---returns those records from the FIRST select that are not returned by the select statement from other query
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
  FROM [IDS].[Able_Claim_Expense]
   where 1=1
   AND month_eftv_date = EOMONTH(DATEADD(month,-1, convert(datetime,@ProcessingMonthYear)))					--- output for last month
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
 insert into IDS.Able_Claim_Expense_Trnx
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
SELECT convert(datetime,@ProcessingMonthYear) [Month_Eftv_Date]						--- there are two cte's in the 2nd one we have used an alias so that it identifies that this is the 2nd cte.
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
	;WITH w_existing_claims																--- cte 
	 AS
	 (
	 select distinct [Able_Claim_Expense_Trnx_Id],[GUID] 
	 from IDS.Able_Claim_Expense_Trnx [nolock] 
	 where month_Eftv_date < convert(datetime,@ProcessingMonthYear) 					--- eg:31st Jan < all the data before the date we pass
	 )
	 
	 
	 UPDATE IDS.Able_Claim_Expense_Trnx
	 SET IDS.Able_Claim_Expense_Trnx.[Able_Claim_Expense_Trnx_Id] = src.[Able_Claim_Expense_Trnx_Id],
	     updated_date_time = GETDATE(),
		 Updated_By = system_user
	 FROM IDS.Able_Claim_Expense_Trnx trg,w_existing_claims src
	 where trg.month_Eftv_date = convert(datetime,@ProcessingMonthYear)
	 and trg.[GUID] = src.[GUID]

	 set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
	 --
	 UPDATE IDS.Able_Claim_Expense_Trnx
	 SET [Able_Claim_Expense_Trnx_Id] = NEXT VALUE FOR [IDS].[Able_Claim_Expense_Trnx_Seq], 				--- next value for: retrieves the next value in the sequence generator 
	     updated_date_time = GETDATE(),																--- Able_Claim_Expense_Trnx_Seq
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
/****** Object:  StoredProcedure [RNS].[Load_Sample_Values]    Script Date: 25-05-2022 00:38:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE Proc [RNS].[Load_Sample_Values]
@Table_Name Varchar(200)--, @file_path nvarchar(255)
As
Begin

declare 
		@s_name sysname = N'RNS',
		--@t_name sysname,
		@delimiter nchar(1)= ',',
		 --@BulkIns_query nvarchar(max),
		 @Truncquery nvarchar(max)



Set @Truncquery = 'Truncate Table RNS.' +@Table_Name
Exec (@Truncquery)
--truncate table RNS.REINS_CONTRACT_RISK_INFORCE

Bulk Insert [IDS].[Able_Claim_Expense]								-- change Table Name
From 'C:\Users\rohan.chaugule\Pictures\Payment\IDS\ABLE_original\able_trnx_sample.csv' -- change Table Name
With (Fieldterminator = ',', rowterminator = '\n', Firstrow = 2)

END;

EXEC [RNS].[Load_Sample_Values] 'REINS_CONTRACT_RISK_INFORCE'

--set @file_path = 'C:\Users\sumit.chapalge\Desktop\Sample Values\'
--set @BulkIns_query = 'Bulk Insert [AU\RMTVAR].RNS.'+@Table_Name +' From '+@TblPath+@Table_Name+' With (Fieldterminator = '','', rowterminator = ''\n'', Firstrow = 2)'
/*
DECLARE @BulkIns_query nvarchar(MAX),
            @CRLF nchar(2) = NCHAR(13) + NCHAR(10);

    SET @BulkIns_query = N'BULK INSERT ' + QUOTENAME(@s_name) + N'.' + QUOTENAME(@Table_Name) + @CRLF +
               N'FROM N''' + REPLACE(@file_path,'''','''''') + N'''' + @CRLF + 
               N'WITH (FIELDTERMINATOR = N' + QUOTENAME(@delimiter,'''') + N',' + @CRLF + 
               N'      ROWTERMINATOR = ''\n'');'

    --PRINT @SQL;
    EXEC sys.sp_executesql @BulkIns_query;
*/
--Exec (@BulkIns_query)

/*
set @BulkIns_query = 'insert into '+ @Table_Name+ ' from RNS.Test_Bulk_Load'
Exec (@BulkIns_query)



set @BulkIns_query1 = 'Select * into ' + @Table_Name+' from RNS.Test_Bulk_Load'
Exec (@BulkIns_query1)
*/
GO
USE [master]
GO
ALTER DATABASE [Payments] SET  READ_WRITE 
GO
