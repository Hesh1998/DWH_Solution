/****** Object:  StoredProcedure [log].[PipelineStatus_U]    Script Date: 11/12/2023 1:43:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

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


