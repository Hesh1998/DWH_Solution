/****** Object:  StoredProcedure [log].[LakeLogs_S]    Script Date: 11/12/2023 1:30:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [log].[LakeLogs_S] @Key VARCHAR(50)
AS
	SELECT [Value] FROM [log].[LakeLogs] WHERE [Key] = @Key;
RETURN 0
GO


