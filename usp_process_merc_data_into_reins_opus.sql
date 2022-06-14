/****** Object:  StoredProcedure [IDS].[usp_Process_Merc_Data_into_Reins_Opus]    Script Date: 6/13/2022 8:03:28 PM ******/


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [IDS].[usp_Process_Merc_Data_into_Reins_Opus] @ProcessLogID int,@ProcessingMonthYear NVARCHAR(100)
AS
BEGIN

	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	Declare @Created_By Varchar(30) = IDS.udf_Get_Username()
	BEGIN TRY;
 
	BEGIN TRAN T1;
	
	-- Insert records into Reins_opus_trnx from Mercury_trnx table.
	
	INSERT INTO [IDS].[Reins_Opus_Trnx]
           ([Month_Effective_Date]
           ,[Reins_Opus_Trnx_Id]
           ,[Product_System_Code]
           ,[Policy_Number]
           ,[EDW_Contract_Key]
           ,[EDW_Contract_Id]
           ,[EDW_Product_Key]
           ,[EDW_Product_Code]
           ,[EDW_Product_Desc]
           ,[EDW_Class_Of_Business]
           ,[Reins_Benefit_Category]
           ,[Transaction_Effective_date]
           ,[Transaction_Process_Date]
           ,[Transaction_Amount]
           ,[Transaction_Type]
           ,[Claim_Expense_Id]
           ,[EDW_Event_Status_Code]
           ,[EDW_Event_Type_Code]
           ,[EDW_Event_Type_Desc]
           ,[EDW_Event_Number]
           ,[Created_Date_Time]
           ,[Created_By]
           ,[Updated_Date_Time]
           ,[Updated_By]
           ,[Process_Log_ID]
           ,[Exclude_Flag]
           ,[Exclude_Reason]
           ,[Account_Id]
           ,[GL_Product_Code]
           ,[EDW_Policy_Number]
           ,[Client_Ref_Id_2]
           ,[Trans_Ref_Key_1]
           ,[Ledger_Movement_Amount]
           ,[GL_Posting_Date])
     select
			Month_Effective_Date,
			'MR' + cast(Mercury_trnx_id as varchar (18)),
			Product_System_Code,
			Policy_Number,
			EDW_Contract_Key,
			EDW_Contract_Id,
			EDW_Product_Key,
			EDW_Product_Code,
			EDW_Product_Name,
			EDW_Class_Of_Business,
			NULL as Reins_Benefit_Category,
			Month_Effective_Date AS [Transaction_Effective_date],
			GL_Posting_Date AS Transaction_Process_Date,
			Ledger_Movement_Amount as Transaction_Amount,
			NULL as Transaction_Type,
			NULL as Claim_Expense_Id,
			NULL as EDW_Event_Status_Code,
			NULL as EDW_Event_Type_Code,
			NULL as EDW_Event_Type_Desc,
			NULL as EDW_Event_Number,
			getdate() as Created_Date_Time,
			@Created_By,
			NULL as Updated_Date_Time,
			NULL as Updated_By,
			@ProcessLogID as Process_Log_ID,
			'N' as Exclude_Flag,
			NULL as Exclude_Reason,
			GL_Account_Id,
			GL_Product_Code,
			EDW_Policy_Number,
			Client_Ref_Id_2,
			Trans_Ref_Key_1,
			Ledger_Movement_Amount,
			GL_Posting_Date
	  from
	  IDS.Mercury_Trnx
	  where Product_System_Code = 'LS'
	  and Exclude_Flag = 'N'
	  and Month_Effective_Date = @ProcessingMonthYear

	SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT

	COMMIT TRAN T1;
	PRINT ' Inserted records into Reins_opus_trax table' + convert(varchar(50), @InsertRowCount)
	
	--Update Payment Item Type Lt 0

	Begin Transaction
	UPDATE 
		[IDS].[Reins_Opus_Trnx]
	SET
		Transaction_Type = [Trnx_Type_LT_0],
		Updated_by = @Created_By,
		Updated_date_time = getdate()
	FROM 
		IDS.[Reins_Claim_Expense_Trnx_Type] ,
		[IDS].[Reins_Opus_Trnx]
	WHERE
		IDS.[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_Opus_Trnx].ACCOUNT_ID
		AND ((([IDS].[Reins_Opus_Trnx].[Ledger_Movement_Amount])<0))
		AND [IDS].[Reins_Opus_Trnx].[Transaction_Type] IS NULL 
		and Month_Effective_Date = @ProcessingMonthYear
	
	set @UpdateRowCount = @UpdateRowCount + @@Rowcount
    COMMIT

	--Update Payment Item Type Gt 0

	Begin Transaction
	UPDATE 
		 [IDS].[Reins_Opus_Trnx]
	SET
	   Transaction_Type = [Trnx_Type_GT_0]
	FROM 
		 IDS.[Reins_Claim_Expense_Trnx_Type] ,
		[IDS].[Reins_Opus_Trnx]
	WHERE
		IDS.[Reins_Claim_Expense_Trnx_Type].GL_ACCOUNT_ID = [IDS].[Reins_Opus_Trnx].ACCOUNT_ID
	AND     ((([IDS].[Reins_Opus_Trnx].[Ledger_Movement_Amount])>=0))
	AND     [IDS].[Reins_Opus_Trnx].[Transaction_Type] IS NULL
	and Month_Effective_Date = @ProcessingMonthYear
 
	set @UpdateRowCount = @UpdateRowCount + @@Rowcount
	COMMIT

	PRINT ' Updated records for Transaction_Type in Reins_opus_trax table' + convert(varchar(50), @UpdateRowCount)

	--Update Reins benefit Cat based on GL Product Mapping
	
	Begin Transaction
	
	Update [Reins_Opus_Trnx]
	set [Reins_Benefit_Category] = B.[Reins_Benefit_Type]
	from [Reins_Opus_Trnx] A, [IDS].[Reins_GL_Product] B
	where A.GL_Product_Code = B.GL_Product_Code
	and [Reins_Benefit_Category] is null
	and Month_Effective_Date = @ProcessingMonthYear
	
	set @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT
    COMMIT

	PRINT ' Updated records for Reins_Benefit_Category in Reins_opus_trax table' + convert(varchar(50), @UpdateRowCount)


	SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount
	END TRY

	BEGIN CATCH
		IF @@TRANCOUNT > 0
		ROLLBACK TRAN T1;
		SELECT 0 AS InsertRowCount, 0 as UpdateRowCount, 0 as DeleteRowCount;
		THROW;
	END CATCH
END
