/****** Object:  StoredProcedure [RNS].[usp_Load_Reins_Life]    Script Date: 27/04/2022 3:11:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER  Proc [RNS].[usp_Load_Reins_Life]
	@pAcctng_Census_Date DATE  -- Usp_Merge_Party
As
BEGIN

	Declare @Created_Date_Time Datetime = Getdate()
	Declare @Created_By Varchar(30) = rns.udf_Get_Username()
	Declare @Row_Eftv_Date Datetime
	Declare @Deleterowcount Int = 0
	Declare @Insertrowcount Int = 0
	Declare @Updaterowcount Int = 0

	-- Declare and initialise variables/assumptions
	Declare @dAUDIT_STATUS                VARCHAR (10)='current',
			@dREINS_PAS_ID                BIGINT      =1,
			@dACCTNG_CENSUS_END_DATE      DATE        ='12/31/3000',
			@dCURRENT_RECORD_IND_Y        CHAR (1)    ='Y',
			@dCURRENT_RECORD_IND_N        CHAR (1)    ='N',
			@dROW_UPDATED_TSTP            DATETIME    =Null,
			@dUPDATED_BY                  VARCHAR (50)=Null,
			@Last_Acctng_Census_Date	  DATE=[RNS].[udf_Get_Last_Month_End](@pAcctng_Census_Date)

--Declare @Maxroweftvdate Datetime
--Declare @Processingeftvdate Datetime=@pAcctng_Census_Date
/*
Select  @Maxroweftvdate =Max(ACCTNG_CENSUS_START_DATE) From [RNS].Reins_Life
If (@Maxroweftvdate >= @Processingeftvdate)
Begin
Raiserror('Data Of This Processing Period Already Exists, Clear Data Before Rerun',16,1);
End
*/
	Begin Try;-- Begin Try Block , This Acts Like A Wrapper Around The Code To Catch Any Exceptions In The Succeeding Catch Block
	Begin Tran T1;  -- Begin Transaction, Has To Be Named To Commit (Or) Rollback Based On The Completion (Or) Failure Of Any Transactions From Here

		Insert Into [RNS].[REINS_LIFE]
			( --Table And Columns In Which To Insert The Data
				 SOURCE_PARTY_KEY
				,FIRST_NAME
				,SECOND_NAME
				,SURNAME
				,GENDER_CODE
				,DOB
				,DECEASED_IND
				,DOD
				,POST_CODE
				,STATE_CODE
				,AUDIT_STATUS                   --Inserted Hardcoded Value
				,PRODUCT_SYSTEM_CODE			--Added Ne Columns as discussed with Gaurav 11042022
				,REINS_PAS_ID                   --Inserted Hardcoded Value
				,DIMENSIONCHECKSUM
				,ACCTNG_CENSUS_START_DATE
				,ACCTNG_CENSUS_END_DATE
				,CURRENT_RECORD_IND
				,ROW_INSERTED_TSTP
				,ROW_UPDATED_TSTP
				,INSERTED_BY
				,UPDATED_BY
			)
			-- Select The Rows/Columns To Insert That Are Output From This Merge Statement 
			-- In This Example, The Rows To Be Inserted Are The Rows That Have Changed (Update).
			Select
				 SOURCE_PARTY_KEY
				,FIRST_NAME
				,SECOND_NAME
				,SURNAME
				,GENDER_CODE
				,DOB
				,DECEASED_IND
				,DOD
				,POST_CODE
				,STATE_CODE
				,AUDIT_STATUS
				,PRODUCT_SYSTEM_CODE			--Added Ne Columns as discussed with Gaurav 11042022
				,REINS_PAS_ID                   
				,DIMENSIONCHECKSUM
				,ACCTNG_CENSUS_START_DATE
				,ACCTNG_CENSUS_END_DATE
				,CURRENT_RECORD_IND
				,ROW_INSERTED_TSTP
				,ROW_UPDATED_TSTP
				,INSERTED_BY
				,UPDATED_BY
				
			From
			(
				-- This Is The Beginning Of The Merge Statement.
				-- The Target Must Be Defined, In This Example It Is Our Slowly Changing
				-- Dimension Table
				Merge Into [RNS].[REINS_LIFE] As Target
				-- The Source Must Be Defined With The Using Clause
				Using 
				(
				-- The Source Is Made Up Of The Attribute Columns From The Staging Table.
				Select 
					 SOURCE_PARTY_KEY
					,FIRST_NAME
					,SECOND_NAME
					,SURNAME
					,GENDER_CODE
					,DOB
					,DECEASED_IND
					,DOD
					,POST_CODE
					,STATE_CODE
					,PRODUCT_SYSTEM_CODE			--Added Ne Columns as discussed with Gaurav 11042022
					,Binary_Checksum (
								 FIRST_NAME
								,SECOND_NAME
								,SURNAME
								,GENDER_CODE
								,DOB
								,DECEASED_IND
								,DOD
								,POST_CODE
								,STATE_CODE
								,PRODUCT_SYSTEM_CODE) As DIMENSIONCHECKSUM
				From [STG].[STG_PARTY]
				) As Source 
				( 
					 SOURCE_PARTY_KEY
					,FIRST_NAME
					,SECOND_NAME
					,SURNAME
					,GENDER_CODE
					,DOB
					,DECEASED_IND
					,DOD
					,POST_CODE
					,STATE_CODE
					,PRODUCT_SYSTEM_CODE
					,DIMENSIONCHECKSUM
				) On --We Are Matching On The Sourcesystemid In The Target Table And The Source Table.
				(
				Target.SOURCE_PARTY_KEY = Source.SOURCE_PARTY_KEY
				)
				-- If The Id's Match But The Checksums Are Different, Then The Record Has Changed;
				-- Therefore, Update The Existing Record In The Target, End Dating The Record 
				-- And Set The Activeflag Flag To N
				When Matched And Target.DIMENSIONCHECKSUM <> Source.DIMENSIONCHECKSUM 
												And Target.CURRENT_RECORD_IND='Y'
				Then 
				Update Set 
				CURRENT_RECORD_IND = @dCURRENT_RECORD_IND_N, -- N
				ROW_UPDATED_TSTP=@Created_Date_Time, --Getdate(), 
				UPDATED_BY=@Created_By, --Suser_Sname(),
				ACCTNG_CENSUS_END_DATE=@Last_Acctng_Census_Date -- Getdate()
				-- If The Id's Do Not Match, Then The Record Is New;
				-- Therefore, Insert The New Record Into The Target Using The Values From The Source.
				When Not Matched Then  
				Insert 
				(
				 SOURCE_PARTY_KEY
				,FIRST_NAME
				,SECOND_NAME
				,SURNAME
				,GENDER_CODE
				,DOB
				,DECEASED_IND
				,DOD
				,POST_CODE
				,STATE_CODE
				,AUDIT_STATUS
				,PRODUCT_SYSTEM_CODE
				,REINS_PAS_ID                  
				,DIMENSIONCHECKSUM
				,ACCTNG_CENSUS_START_DATE
				,ACCTNG_CENSUS_END_DATE
				,CURRENT_RECORD_IND
				,ROW_INSERTED_TSTP
				,ROW_UPDATED_TSTP
				,INSERTED_BY
				,UPDATED_BY
				)
				Values 
				(
				 Source.SOURCE_PARTY_KEY
				,Source.FIRST_NAME
				,Source.SECOND_NAME
				,Source.SURNAME
				,Source.GENDER_CODE
				,Source.DOB
				,Source.DECEASED_IND
				,Source.DOD
				,Source.POST_CODE
				,Source.STATE_CODE
				,@dAUDIT_STATUS
				,Source.PRODUCT_SYSTEM_CODE
				,@dREINS_PAS_ID
				,Source.DIMENSIONCHECKSUM
				,@pAcctng_Census_Date
				,@dACCTNG_CENSUS_END_DATE
				,@dCURRENT_RECORD_IND_Y
				,@Created_Date_Time --Getdate(),
				,@dROW_UPDATED_TSTP
				,@Created_By -- Suser_Sname(),
				,@dUPDATED_BY
				)

				Output $Action, 
				 Source.SOURCE_PARTY_KEY
				,Source.FIRST_NAME
				,Source.SECOND_NAME
				,Source.SURNAME
				,Source.GENDER_CODE
				,Source.DOB
				,Source.DECEASED_IND
				,Source.DOD
				,Source.POST_CODE
				,Source.STATE_CODE
				,@dAUDIT_STATUS
				,Source.PRODUCT_SYSTEM_CODE
				,@dREINS_PAS_ID
				,Source.DIMENSIONCHECKSUM
				,@pAcctng_Census_Date
				,@dACCTNG_CENSUS_END_DATE
				,@dCURRENT_RECORD_IND_Y
				,@Created_Date_Time --Getdate(),
				,@dROW_UPDATED_TSTP
				,@Created_By -- Suser_Sname(),
				,@dUPDATED_BY
				) -- The End Of The Merge Statement
			--The Changes Output Below Are The Records That Have Changed And Will Need
			--To Be Inserted Into The Slowly Changing Dimension.
				As Changes 
				(
					 Action
					,SOURCE_PARTY_KEY
					,FIRST_NAME
					,SECOND_NAME
					,SURNAME
					,GENDER_CODE
					,DOB
					,DECEASED_IND
					,DOD
					,POST_CODE
					,STATE_CODE
					,AUDIT_STATUS
					,PRODUCT_SYSTEM_CODE
					,REINS_PAS_ID                   
					,DIMENSIONCHECKSUM
					,ACCTNG_CENSUS_START_DATE
					,ACCTNG_CENSUS_END_DATE
					,CURRENT_RECORD_IND
					,ROW_INSERTED_TSTP
					,ROW_UPDATED_TSTP
					,INSERTED_BY
					,UPDATED_BY
				)
				Where Action='Update';  

										
	Select @Insertrowcount = @Insertrowcount + Count(*)
        From [RNS].[REINS_LIFE] Where [ROW_INSERTED_TSTP] = @Created_Date_Time;
                  
        Select @Updaterowcount = @Updaterowcount + Count(*)
        From [RNS].[REINS_LIFE] Where [ROW_UPDATED_TSTP] = @Created_Date_Time; --And [Lastupdated]<>[Startdate]

	Commit Tran T1; --Commit Transaction If No Errors
	
	Select @Insertrowcount As Insertrowcount, @Updaterowcount As Updaterowcount, @Deleterowcount As Deleterowcount --Output Rowset To The Console
					---
	End Try-- End Try Block 
                --
                Begin Catch -- Start Of Catch Block, This Catches Any Exceptions Occured In The Preceding Try Block
                    If @@Trancount > 0
                    Rollback Tran T1; --Rollback Transaction If There Are Any Errors (Or) Exceptions
                   Select 0 As Insertrowcount, 0 As Updaterowcount, 0 As Deleterowcount;
                    Throw; -- Throw The Exception
                End Catch
END