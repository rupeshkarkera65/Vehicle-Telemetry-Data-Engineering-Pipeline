
-- Connect to: Dedicated Pool created
-- Use Database: `telemetry_dedicated_pool`

-- 1: telemetry gold table
CREATE TABLE dbo.telemetry_gold 
( 
    vehicle_id       VARCHAR(50), 
    total_events       BIGINT, 
    avg_speed     FLOAT, 
    max_speed    BIGINT, 
    min_speed   BIGINT, 
    avg_temperature   FLOAT, 
    min_fuel_level BIGINT, 
    event_date       DATE
     ) 
WITH 
( 
    DISTRIBUTION = HASH(vehicle_id), 
    CLUSTERED COLUMNSTORE INDEX 
); 



-- 2: watermark table
CREATE TABLE [dbo].[WatermarkTable] 
(  
    [TableName] [nvarchar](255)  NULL, 
    [WatermarkValue] [datetime]  NULL 
) 
WITH 
( 
    DISTRIBUTION = ROUND_ROBIN, 
    CLUSTERED COLUMNSTORE INDEX 
)
GO 

-- Insert one default value
insert into dbo.WatermarkTable (WatermarkValue,TableName)
select '2026-01-17','telemetry_gold'



-- 3: Proc Stored Procedure
CREATE PROC [dbo].[sp_UpdateTelemetryWatermark] @PipelineName [VARCHAR](100),@NewWatermark [DATETIME] AS
BEGIN
If Exists (select '1' from dbo.WatermarkTable where TableName = @PipelineName)
begin
    UPDATE dbo.WatermarkTable
    SET WatermarkValue = @NewWatermark
    WHERE TableName = @PipelineName;
end
END;
GO



-- View the data
select * from telemetry_gold;
select * from WatermarkTable;


-- See the max event date
SELECT MAX(event_date) AS MaxEventDate 
FROM dbo.telemetry_gold; 
