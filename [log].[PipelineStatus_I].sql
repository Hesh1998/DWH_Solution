-- This SP is used to insert a new record into the [log].[PipelineStatus] table at the start of each pipeline run
CREATE PROCEDURE [log].[PipelineStatus_I] @DataFactory varchar(100), @Pipeline varchar(100)
AS
	INSERT INTO [log].[PipelineStatus] ([DataFactory], [Pipeline], [StartTime])
	SELECT @DataFactory, @Pipeline, GETDATE();
RETURN 0
GO
