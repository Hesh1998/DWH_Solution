-- This SP is used to update the record inserted into the [log].[PipelineStatus] table at the start of each pipeline run, based on the pipeline run status
CREATE PROCEDURE [log].[PipelineStatus_U] @DataFactory varchar(100), @Pipeline varchar(100), @Status varchar(100), @Error varchar(100)
AS
	DECLARE @Id AS INT;
	SELECT @Id = MAX(Id) FROM [log].[PipelineStatus] WHERE [DataFactory] = @DataFactory AND [Pipeline] = @Pipeline;
	
	UPDATE [log].[PipelineStatus]
		SET [EndTime] = GETDATE()
		,[Status] = @Status
		,[Error] = @Error
	WHERE Id = @Id

RETURN 0
GO


