-- checking for additional comments
-- adding some lines of code

select * from student 
select * from student1 
select * from table
select * from table1 
select * from lapsed_reinstated

USE [Payments]
GO
/****** Object:  StoredProcedure [RNS].[Load_Sample_Values]    Script Date: 07-06-2022 23:30:12 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER Proc [RNS].[Load_Sample_Values]
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
