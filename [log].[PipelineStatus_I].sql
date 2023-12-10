/****** Object:  StoredProcedure [log].[PipelineStatus_I]    Script Date: 11/12/2023 1:25:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [log].[PipelineStatus_I] @DataFactory varchar(100), @Pipeline varchar(100)
AS
	INSERT INTO [log].[PipelineStatus] ([DataFactory], [Pipeline], [StartTime])
	SELECT @DataFactory, @Pipeline, GETDATE();
RETURN 0
GO