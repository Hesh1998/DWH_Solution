-- This SP is used to retrieve values from the log database based on the key value passed as a parameter
CREATE PROCEDURE [log].[LakeLogs_S] @Key VARCHAR(50)
AS
	SELECT [Value] FROM [log].[LakeLogs] WHERE [Key] = @Key;
RETURN 0
GO


