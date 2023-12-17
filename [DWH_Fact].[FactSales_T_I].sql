-- Sales fact table SP which reads the Sales data parquet file from data lake, do all required transformations to the dataset, and write the cleaned dataset to the [DWH_Fact].[FactSales] warehouse table.
CREATE PROC [DWH_Fact].[FactSales_T_I] @FilePath [varchar](500) AS

BEGIN
	-- Drops [DWH_Fact].[FactSales_E] external table, if exists
    IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('DWH_Fact.FactSales_E'))
		DROP EXTERNAL TABLE [DWH_Fact].[FactSales_E];
	
	-- Creates [DWH_Fact].[FactSales_E] external table
	-- Data from the parquet file in the data lake is loaded to this table
	DECLARE @sql varchar(max);
	-- DECLARE @FilePath [varchar](500) = '/fact/sales';
	SET @sql = '
		CREATE EXTERNAL TABLE [DWH_Fact].[FactSales_E]
		(
			[SalesOrderId] [varchar](100) NULL,
			[Quantity] [varchar](100) NULL,
			[UnitPrice] [varchar](100) NULL,
			[LineItemId] [varchar](100) NULL,
			[TotalAmount] [varchar](100) NULL,
			[Date] [varchar](100) NULL,
			[Status] [varchar](100) NULL,
			[Product] [varchar](100) NULL,
			[Customer] [varchar](100) NULL
		)  
		WITH (
			LOCATION = ''' + @FilePath + ''',
			DATA_SOURCE = [AzureDataLakeStore],  
			FILE_FORMAT = [FileformatParquet]
		);'
	EXEC (@sql)
	
	
	-- Drops [DWH_Fact].[FactSales_T] temporary table, if exists
	IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('DWH_Fact.FactSales_T'))
		DROP TABLE [DWH_Fact].[FactSales_T];

	-- Creates [DWH_Fact].[FactSales_T] temporary table
	-- This temporary table is used to do all the required transformations to the data, before loading data to the final table in the DWH
	-- [SalesOrderId] + [LineItemId] combination can be used to uniquely identify a record. So, [HashValue] is generated based on these 2 fields. 
	CREATE TABLE [DWH_Fact].[FactSales_T]
	WITH (heap, DISTRIBUTION = ROUND_ROBIN) 
	AS
	SELECT [SalesOrderId]
		,CAST([Quantity] AS FLOAT) AS [Quantity]
		,CAST([UnitPrice] AS FLOAT) AS [UnitPrice]
		,[LineItemId]
		,CAST([TotalAmount] AS FLOAT) AS [TotalAmount]
		,CAST([Date] AS DATE) AS [Date]
		,[Status]
		,[Product]
		,[Customer]
		,HASHBYTES('MD5', [SalesOrderId] + [LineItemId]) as [HashValue]
	FROM [DWH_Fact].[FactSales_E];


	-- Insert data into the final DWH table [DWH_Fact].[FactSales]
	-- Exception handling has been used to minimize the impact on DWH table in case of an issue with the Delete / Insert query
	-- TRY Block - Delete / Insert query
	-- CATCH Block - If an error occurs in the TRY block, rollbacks the entire transaction so that there would be no impact on the DWH table. And raises an error so that this can be identified from the ADF side.
	DECLARE @xact_state smallint = 0;
	BEGIN TRY

		BEGIN TRAN
			-- Already existing records in [DWH_Fact].[FactSales] are deleted using [HashValue]
			DELETE
			FROM [DWH_Fact].[FactSales]
			WHERE [HashValue] in (SELECT [HashValue] FROM [DWH_Fact].[FactSales_T]);

			
			-- Insert new records to [DWH_Fact].[FactSales]
			INSERT INTO [DWH_Fact].[FactSales]
			(
				[CustomerSK]
				,[ProductSK]
				,[DateSK]
				,[SalesOrderId]
				,[LineItemId]
				,[Quantity]
				,[UnitPrice]
				,[TotalAmount]
				,[Status]
				,[DateInserted]
				,[HashValue]
			)
				
			SELECT 
				ISNULL([DWH_Dim].[DimCustomer].[CustomerSK], -1)
				,ISNULL([DWH_Dim].[DimProduct].[ProductSK], -1)
				,ISNULL([DWH_Dim].[DimDate].[DateSK], -1)
				,[DWH_Fact].[FactSales_T].[SalesOrderId]
				,[DWH_Fact].[FactSales_T].[LineItemId]
				,[DWH_Fact].[FactSales_T].[Quantity]
				,[DWH_Fact].[FactSales_T].[UnitPrice]
				,[DWH_Fact].[FactSales_T].[TotalAmount]
				,[DWH_Fact].[FactSales_T].[Status]
				,GETDATE()
				,[DWH_Fact].[FactSales_T].[HashValue]
			FROM [DWH_Fact].[FactSales_T]
			LEFT OUTER JOIN [DWH_Dim].[DimCustomer] ON [DWH_Fact].[FactSales_T].[Customer] = [DWH_Dim].[DimCustomer].[Name]
			LEFT OUTER JOIN [DWH_Dim].[DimProduct] ON [DWH_Fact].[FactSales_T].[Product] = [DWH_Dim].[DimProduct].[Code]
			LEFT OUTER JOIN [DWH_Dim].[DimDate] ON [DWH_Fact].[FactSales_T].[Date] = [DWH_Dim].[DimDate].[Date];

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


