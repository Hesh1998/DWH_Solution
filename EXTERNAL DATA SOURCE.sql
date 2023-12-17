-- Method used -> Create external data source to reference Azure Data Lake Store Gen2 using the storage account key
-- Links - https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-external-tables?tabs=hadoop
-- Links - https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-data-source-transact-sql?view=azure-sqldw-latest&preserve-view=true&tabs=dedicated

-- Create a database master key for the DWH if one does not already exist, using your own password. This key is used to encrypt the credential secret in next step.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Sqlhesh@datawh&m123';


CREATE DATABASE SCOPED CREDENTIAL [ADLS_credential]
WITH 
-- IDENTITY = '<storage_account_name>' ,
IDENTITY = 'heshtestdwhstorage',  
-- SECRET = '<storage_account_key>'
SECRET = '1xs16+I0PEmjBgdVLsrVsMCz9JIYQrnWcFAFbPrhpDtlgfD6AY/H0NZTz4aYwBkntcQy2dIHiTrr+ASt7lk8jg==';


-- Note this example uses a Gen2 secured endpoint (abfss)
CREATE EXTERNAL DATA SOURCE AzureDataLakeStore
WITH
  -- Please note the abfss endpoint when your account has secure transfer enabled
  ( LOCATION = 'abfss://data-lake@heshtestdwhstorage.dfs.core.windows.net',
    CREDENTIAL = ADLS_credential ,
    TYPE = HADOOP
  ) ;


  