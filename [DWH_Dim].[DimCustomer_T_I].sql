/****** Object:  StoredProcedure [DWH_Dim].[DimCustomer_T_I]    Script Date: 11/12/2023 5:00:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [DWH_Dim].[DimCustomer_T_I] @FilePath [varchar](500) AS

BEGIN
	-- Drops [DWH_Dim].[DimCustomer_E] external table, if exists
    IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('DWH_Dim.DimCustomer_E'))
		DROP EXTERNAL TABLE [DWH_Dim].[DimCustomer_E];
	
	-- Creates [DWH_Dim].[DimCustomer_E] external table
	-- Data from the parquet file in the data lake is loaded to this table
	DECLARE @sql varchar(max);
	-- DECLARE @FilePath [varchar](500) = '/dimension/customer';
	SET @sql = '
		CREATE EXTERNAL TABLE [DWH_Dim].[DimCustomer_E]
		(
			[Name] [varchar](100) NULL,
			[Phone] [varchar](100) NULL,
			[AddressLine1] [varchar](100) NULL,
			[AddressLine2] [varchar](100) NULL,
			[City] [varchar](100) NULL,
			[State] [varchar](100) NULL,
			[PostalCode] [varchar](100) NULL,
			[Country] [varchar](100) NULL
		)  
		WITH (
			LOCATION = ''' + @FilePath + ''',
			DATA_SOURCE = [AzureDataLakeStore],  
			FILE_FORMAT = [FileformatParquet]
		);'
	EXEC (@sql)
	
	
	-- Drops [DWH_Dim].[DimCustomer_T] temporary table, if exists
	IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('DWH_Dim.DimCustomer_T'))
		DROP TABLE [DWH_Dim].[DimCustomer_T];

	-- Creates [DWH_Dim].[DimCustomer_T] temporary table
	-- This temporary table is used to do all the required transformations to the data, before loading data to the final table in the DWH
	CREATE TABLE [DWH_Dim].[DimCustomer_T]
	WITH (heap, DISTRIBUTION = ROUND_ROBIN) 
	AS
	SELECT
		[Name]
		,[Phone]
		,[AddressLine1]
		,NULLIF([AddressLine2], 'nan') AS [AddressLine2]
		,[City]
		,NULLIF([State], 'nan') AS [State]
		,NULLIF([PostalCode], 'nan') AS [PostalCode]
		,[Country]
	FROM [DWH_Dim].[DimCustomer_E];


	-- Update / Insert data into the final DWH table [DWH_Dim].[DimCustomer]
	-- Exception handling has been used to minimize the impact on DWH table in case of an issue with the Insert / Update query
	-- TRY Block - Insert / Update query
	-- CATCH Block - If an error occurs in the TRY block, rollbacks the entire transaction so that there would be no impact on the DWH table. And raises an error so that this can be identified from the ADF side.
	DECLARE @xact_state smallint = 0;
	BEGIN TRY

		BEGIN TRAN
			-- Unique value = [Name]. So, [Name] will be considered as the PK in Update and Insert.


			-- Insert new records to the dimension table
			INSERT INTO [DWH_Dim].[DimCustomer]
			( 
				[Name]
				,[Phone]
				,[AddressLine1]
				,[AddressLine2]
				,[City]
				,[State]
				,[PostalCode]
				,[Country]
				,[DateInserted]
			)
			SELECT 
				[DWH_Dim].[DimCustomer_T].[Name]
				,[DWH_Dim].[DimCustomer_T].[Phone]
				,[DWH_Dim].[DimCustomer_T].[AddressLine1]
				,[DWH_Dim].[DimCustomer_T].[AddressLine2]
				,[DWH_Dim].[DimCustomer_T].[City]
				,[DWH_Dim].[DimCustomer_T].[State]
				,[DWH_Dim].[DimCustomer_T].[PostalCode]
				,[DWH_Dim].[DimCustomer_T].[Country]
				,GETDATE()
			FROM [DWH_Dim].[DimCustomer_T]
			LEFT OUTER JOIN [DWH_Dim].[DimCustomer] 
			ON [DWH_Dim].[DimCustomer_T].[Name] = [DWH_Dim].[DimCustomer].[Name]
			WHERE [DWH_Dim].[DimCustomer].[Name] IS NULL
			AND [DWH_Dim].[DimCustomer_T].[Name] IS NOT NULL;


			-- Update already existing records in the dimension table
			UPDATE [DWH_Dim].[DimCustomer]
            SET [Phone]  = [DWH_Dim].[DimCustomer_T].[Phone]
				,[AddressLine1] = [DWH_Dim].[DimCustomer_T].[AddressLine1]
				,[AddressLine2] = [DWH_Dim].[DimCustomer_T].[AddressLine2]
				,[City] = [DWH_Dim].[DimCustomer_T].[City]
				,[State] = [DWH_Dim].[DimCustomer_T].[State]
				,[PostalCode] = [DWH_Dim].[DimCustomer_T].[PostalCode]
				,[Country] = [DWH_Dim].[DimCustomer_T].[Country]
            FROM [DWH_Dim].[DimCustomer_T]
            WHERE [DWH_Dim].[DimCustomer_T].[Name] = [DWH_Dim].[DimCustomer].[Name]
			AND ([DWH_Dim].[DimCustomer].[Phone]  != [DWH_Dim].[DimCustomer_T].[Phone]
				OR [DWH_Dim].[DimCustomer].[AddressLine1] != [DWH_Dim].[DimCustomer_T].[AddressLine1]
				OR [DWH_Dim].[DimCustomer].[AddressLine2] != [DWH_Dim].[DimCustomer_T].[AddressLine2]
				OR [DWH_Dim].[DimCustomer].[City] != [DWH_Dim].[DimCustomer_T].[City]
				OR [DWH_Dim].[DimCustomer].[State] != [DWH_Dim].[DimCustomer_T].[State]
				OR [DWH_Dim].[DimCustomer].[PostalCode] != [DWH_Dim].[DimCustomer_T].[PostalCode]
				OR [DWH_Dim].[DimCustomer].[Country] != [DWH_Dim].[DimCustomer_T].[Country]);

		COMMIT TRAN

	END TRY
	BEGIN CATCH

		SET  @xact_state = XACT_STATE();
		IF (@@trancount > 0)
			ROLLBACK TRAN;

		DECLARE @Message varchar(MAX) = CONCAT('Error in TRY/CATCH - ', ERROR_MESSAGE()),
				@Severity int = ERROR_SEVERITY(),
				@State smallint = ERROR_STATE();

		RAISERROR(@Message, @Severity, @State);
			
	END CATCH

END
		
GO


