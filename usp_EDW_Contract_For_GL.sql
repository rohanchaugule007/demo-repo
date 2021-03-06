-- making changes to the file. 
-- adding 2 comments to the file

USE [ResLife_Kushal]
GO
/****** Object:  StoredProcedure [IDS].[usp_EDW_Contract_For_GL]    Script Date: 27-05-2022 12:47:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [IDS].[usp_EDW_Contract_For_GL]
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
	from [IDS].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	--and Source_System_Ref_2 NOT IN ('SPE','SF9')
	--
	DECLARE Columns_Lists_2  CURSOR LOCAL  FORWARD_ONLY  READ_ONLY
	FOR 
	select distinct RTRIM(LTRIM(UPPER(Policy_Number)))+'-' Policy_Number
	from [IDS].[GL_Journal_Trnx]
	where [Month_Eftv_Date] = convert(datetime,@ProcessingMonthYear)
	and Policy_Number is not null
	and [EDW_Contract_Key] IS NULL
	and ISNULL(source_system_Ref_2,'ooNVLoo') <> 'MER'
	and Policy_Number LIKE '%[^0-9]%'
	and charindex('-',RTRIM(LTRIM(UPPER(policy_number)))) =  0
	and right(RTRIM(LTRIM(UPPER(Policy_Number))),1) NOT LIKE '%[^0-9]%'
	UNION ALL
	select distinct left(RTRIM(LTRIM(UPPER(Policy_Number))),len(RTRIM(LTRIM(UPPER(Policy_Number))))-1)+'-'+right(RTRIM(LTRIM(UPPER(Policy_Number))),1) Policy_Number
	from [IDS].[GL_Journal_Trnx]
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
	from [IDS].[GL_Journal_Trnx]
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
	UPDATE [IDS].[GL_Journal_Trnx]
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
		from [IDS].[GL_Journal_Trnx]
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
	UPDATE [IDS].[GL_Journal_Trnx]
		SET [IDS].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [IDS].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[EDW_Product_System_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[IDS].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[IDS].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[IDS].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[IDS].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[IDS].[GL_Journal_Trnx].updated_by = system_user
	FROM [IDS].[GL_Journal_Trnx] main, w_source src
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
		/*********************************************
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

		 --   INSERT INTO #Temp_EDW_Contract
			--EXEC Utility.dbo.usp_queryedw @SQL

				*********************************************/

	--
	
	UPDATE [IDS].[GL_Journal_Trnx]
		SET [IDS].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [IDS].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[IDS].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[IDS].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[IDS].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[IDS].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[IDS].[GL_Journal_Trnx].updated_by = system_user
	FROM [IDS].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
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

	--	******************************* Update this code later************************************
		 --   INSERT INTO #Temp_EDW_Contract
			--EXEC Utility.dbo.usp_queryedw @SQL
	--	******************************* END************************************
	--
	UPDATE [IDS].[GL_Journal_Trnx]
		SET [IDS].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [IDS].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[IDS].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[IDS].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[IDS].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[IDS].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[IDS].[GL_Journal_Trnx].updated_by = system_user
	FROM [IDS].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
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

	UPDATE [IDS].[GL_Journal_Trnx]
		SET [IDS].[GL_Journal_Trnx].[EDW_Contract_Key] = src.[EDW_Contract_Key],
		    [IDS].[GL_Journal_Trnx].[EDW_Policy_Number] = src.[EDW_Policy_Number],
			[IDS].[GL_Journal_Trnx].[EDW_Product_System_Code] = src.[Product_System_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Contract_Id] = src.[EDW_Contract_Id],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Key] = src.[EDW_Product_Key],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Code] = src.[EDW_Product_Code],
			[IDS].[GL_Journal_Trnx].[EDW_Product_Name] = src.[EDW_Product_Name],
			[IDS].[GL_Journal_Trnx].[EDW_Class_Of_Business] = src.[EDW_Class_Of_Business],
			[IDS].[GL_Journal_Trnx].[EDW_Country_Code] = src.[EDW_Country_Code],
			[IDS].[GL_Journal_Trnx].Updated_Date_Time = getDATE(),
			[IDS].[GL_Journal_Trnx].updated_by = system_user
	FROM [IDS].[GL_Journal_Trnx] main, #Temp_EDW_Contract src
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





