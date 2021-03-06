-- making changes to the file 
-- adding 2 comments to the file

USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_Load_GL_Journal_Trnx]    Script Date: 27-05-2022 12:47:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER PROCEDURE [IDS].[usp_Load_GL_Journal_Trnx]
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
delete from [IDS].[GL_Journal_Trnx]
where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear);
--
 SET @DeleteRowCount = @DeleteRowCount + @@ROWCOUNT;
insert into [IDS].[GL_Journal_Trnx]
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
  FROM [IDS].[Stg_AMPL_GL_Journal_Trnx]
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
 SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT
	--
	

	 exec [IDS].[usp_Remove_Garbage_Characters] @ProcessLogID,'dbo','GL_Journal_Trnx'



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





