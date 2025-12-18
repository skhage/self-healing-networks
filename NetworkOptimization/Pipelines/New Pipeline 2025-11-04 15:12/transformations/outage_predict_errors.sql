-- Step 1: Sample 30 rows and set status
CREATE OR REPLACE TEMP VIEW sampled_telemetry_issue AS
SELECT
  *,
  'telemetry issue detected' AS status,
  current_timestamp() AS event_timestamp
FROM telecommunications.self_healing_networks.device_status_st
ORDER BY RAND() LIMIT 30;

-- Step 2: Upsert the new status using Auto CDC
APPLY CHANGES INTO telecommunications.self_healing_networks.device_status_st
FROM sampled_telemetry_issue
KEYS (device_id)
SEQUENCE BY event_timestamp
COLUMNS status, event_timestamp
STORED AS SCD TYPE 1;