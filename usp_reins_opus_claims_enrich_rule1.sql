/****** Object:  StoredProcedure [IDS].[usp_Reins_Opus_Claims_Enrich_Rule1]    Script Date: 6/13/2022 8:05:40 PM ******/
-- checking merge conflicts

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [IDS].[usp_Reins_Opus_Claims_Enrich_Rule1]


AS

BEGIN
	SET NOCOUNT ON
	
	DECLARE @EDW_Contract_Key Numeric(10,0)
	DECLARE @Transaction_Process_date datetime
	DECLARE @EDW_Product_Code varchar(50)
	DECLARE @NumRows int
	--DECLARE @sql varchar(4000)
	DECLARE @DeleteRowCount Int = 0
	DECLARE @InsertRowCount Int = 0
	DECLARE @UpdateRowCount Int = 0
	Declare @Created_By Varchar(30) = IDS.udf_Get_Username()

	CREATE table #Get_Policy_list (
			EDW_Contract_Key Numeric(10,0),
			Transaction_Process_date datetime,
			EDW_Product_code varchar(50)
			)
	

	declare @sqlstatement nvarchar(4000)

	set @sqlstatement = 
			'insert into #Get_Policy_list
			SELECT DISTINCT A.EDW_Contract_Key, A.Transaction_Process_date, A.EDW_Product_Code
			From IDS.Reins_Claim_Expense_Trnx A, IDS.Reins_Claim_Expense B
			WHERE A.Claim_expense_id = B.Claim_expense_id
			and A.Claim_Enrichment_rule is null and A.Transaction_type in (''Expense'', ''Expense reversal'')
			and A.product_system_code = ''LS''
			--and A.Source_trnx_Id like ''LS%''
			--and B.Claim_Expense_Status != ''IGNORE''
			'
			
	
	exec(@sqlstatement);

	DECLARE Policy_List_To_Process  CURSOR LOCAL  FORWARD_ONLY  READ_ONLY
    FOR 
	Select * from #Get_Policy_list

	OPEN Policy_List_To_Process
	
	FETCH NEXT FROM Policy_List_To_Process into 
	@EDW_Contract_Key,
	@Transaction_Process_date,
	@EDW_Product_Code

	
	WHILE @@FETCH_STATUS = 0  
	BEGIN 
		Select @NumRows = count(*) from RNS.Reins_Claim_listing LEFT JOIN RNS.REINS_PRODUCT ON RNS.Reins_Claim_listing.REINS_PRODUCT_ID = RNS.REINS_PRODUCT.REINS_PRODUCT_ID
		where EDW_Contract_Key = @EDW_Contract_Key
		and RNS.REINS_PRODUCT.PRODUCT_SYSTEM_CODE = 'LS' -------------------------Added by KP
		--and @Transaction_Process_date Between RNS.REINS_CLAIM_LISTING.First_Contact_date and ISNULL(Claim_Finalised_Date+14, cast('2899-12-31' as datetime))
		/*and Row_End_Date = '3000-12-31'*/ /*Comented by Harsh as mapping for Row_End_Date not found*/
		and EDW_Product_Code = @EDW_Product_Code
	
				

		If @NumRows = 1
		Begin

			Begin Transaction
						
			UPDATE IDS.Reins_Claim_Expense_Trnx
			SET
				 [Product_Option_Code] = RNS.Reins_Claim_listing.Product_Option_Code ,
				 [Claim_Number] = RNS.Reins_Claim_listing.[Claim_Number],
				/* [Layer_0_Reins_Percent] = RNS.Reins_Claim_listing.[Layer_0_Reins_Percent],
				 [Layer_1_Reins_Percent] = RNS.Reins_Claim_listing.[Layer_1_Reins_Percent],
				 [Layer_1_Treaty_Id] = RNS.Reins_Claim_listing.[Layer_1_Treaty_Id],           /*Comented by Harsh Layer system not in use now*/
				 [Layer_2_Treaty_Id] = RNS.Reins_Claim_listing.[Layer_2_Treaty_Id], */

				 Claim_Source_id = RNS.Reins_Claim_listing.Reins_Claim_Listing_ID ,

				 /*Layer_2_Reins_Percent = RNS.Reins_Claim_listing.Layer_2_Reins_Percent,*/  /*Comented by Harsh Layer system not in use now*/
				 [Claim_enrichment_rule] = 'CE_LS_Pol',
				 Updated_by = @Created_By,
				 Updated_date_time = getdate()

			FROM IDS.Reins_Claim_Expense_Trnx ,
				 RNS.Reins_Claim_listing, RNS.REINS_PRODUCT
			Where IDS.Reins_Claim_Expense_Trnx.EDW_Contract_Key = RNS.Reins_Claim_listing.EDW_Contract_Key
				  AND IDS.Reins_Claim_Expense_Trnx.EDW_Contract_Key = @EDW_Contract_Key
				  AND IDS.Reins_Claim_Expense_Trnx.Transaction_type in ('Expense', 'Expense reversal')
				  AND IDS.Reins_Claim_Expense_Trnx.Claim_Enrichment_rule is null
				  AND IDS.Reins_Claim_Expense_Trnx.Product_System_Code = 'LS'
				  /*AND Reins_Claim_Listing.EDW_Product_System_Code = 'LS' -------------------Commented by Harsh as mapping for EDW_Product_System_Code not found */
				  AND RNS.REINS_PRODUCT.PRODUCT_SYSTEM_CODE='LS' ---------------------------- Added by KP
				--  AND IDS.Reins_Claim_Expense_Trnx.Transaction_Process_date between RNS.Reins_Claim_listing.FIRST_CONTACT_DATE and ISNULL(RNS.Reins_Claim_listing.Claim_Finalised_Date+14, cast('2899-12-31' as date))
				 /* AND RNS.Reins_Claim_listing.Row_End_Date = '3000-12-31'*/ /*Commented by Harsh as mapping for Row_End_Date not found */
				  

				set @UpdateRowCount = @UpdateRowCount + @@Rowcount
			
			COMMIT Transaction
	
		End
		

	FETCH NEXT FROM Policy_List_To_Process into 
	@EDW_Contract_Key,
	@Transaction_Process_date,
	@EDW_Product_Code

	END
	CLOSE Policy_List_To_Process
	Deallocate Policy_List_To_Process

SELECT @InsertRowCount AS InsertRowCount, @UpdateRowCount as UpdateRowCount, @DeleteRowCount as DeleteRowCount

END

