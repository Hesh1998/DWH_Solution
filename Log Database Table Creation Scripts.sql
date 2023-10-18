-- Creates a schema for log tables
CREATE SCHEMA log;


-- Creates PipelineStatus table
CREATE TABLE [log].[PipelineStatus](
	[Id] [int] IDENTITY(1,1) NOT NULL, -- Auto inserts a unique Id for each entry
	[DataFactory] [varchar](20) NOT NULL,
	[Pipeline] [varchar](30) NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NULL,
	[Status] [varchar](10) NULL,
	[Error] [varchar](200) NULL
)
GO


-- Creates LakeLogs table
CREATE TABLE [log].[LakeLogs](
	[Id] [int] IDENTITY(1,1) NOT NULL, -- Auto inserts a unique Id for each entry
	[Key] [varchar](50) NOT NULL,
	[Value] [varchar](100) NOT NULL
)
GO