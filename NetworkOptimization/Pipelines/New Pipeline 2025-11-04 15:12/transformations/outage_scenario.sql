CREATE OR REFRESH STREAMING TABLE telecommunications.self_healing_networks.device_status_st AS (
  SELECT *, current_timestamp() AS event_timestamp,
         'System Online and Functioning' AS status
  FROM STREAM(telecommunications.self_healing_networks.tower_devices)
);

create or refresh streaming table telecommunications.self_healing_networks.device_status_st_pipe as (
  select * from telecommunications.self_healing_networks.device_status_st);

create temporary view simulated_outage as (
  SELECT *,
    current_timestamp() AS event_timestamp,
    'Telemetry Issue Detected' AS status
  FROM telecommunications.self_healing_networks.device_status
  TABLESAMPLE (uniform(30, 60) ROWS)
);

create flow device_updates as 
auto cdc into telecommunications.self_healing_networks.device_status_st_pipe
from stream(simulated_outage)
keys (device_id)
sequence by event_timestamp
columns * except (op, event_timestamp)
stored as scd type 1;

CREATE MATERIALIZED VIEW telecommunications.self_healing_networks.device_status_triage AS
SELECT
   *,
   ai_query('databricks-gemma-3-12b', 
                        CONCAT('what is a telemetry error for the following mobile tower device? Please be extremely concise and give only the example telemetry error, preferably 1 sentence max.', device)
               ) AS telemetry_error 
FROM stream(telecommunications.self_healing_networks.device_status_st);

CREATE MATERIALIZED VIEW telecommunications.self_healing_networks.triaged_devices AS
SELECT
    tower_id,
    device_id,
    device,
    ai_query('ka-bb76eed0-endpoint', concat('Please find an automated and remote fix for this device and error message: ', telemetry_error, '. Be as concise as possible, perferably using fewer than 20 words. If such a solution exists without requiring human intervention, start the response with the phrase AUTOMATED FIX followed by a 1-sentence overview. If human intervention is necessary, start the response with the phrase HUMAN INTERVENTION REQUIRED and state a single phrase overview for the course of action needed.')) as solution, 
    current_timestamp() as event_timestamp
FROM telecommunications.self_healing_networks.device_status_triage;

create temporary view triaged_latlon as (
 select triaged.*, 
  locs.lat as station_lat,
  locs.lon as station_lon
 from telecommunications.self_healing_networks.sf_mobilelocations as locs 
 left join telecommunications.self_healing_networks.triaged_devices as triaged
 on  triaged.tower_id = locs.tower_id
);

create temporary view most_recent_schedule  as (
SELECT * 
from telecommunications.self_healing_networks.fieldtech_route
where date = (select max(date) from telecommunications.self_healing_networks.fieldtech_route)
);

create temporary view tower_service_assignments as (
SELECT
  t.tower_id as tower_id,
  t.device_id as device_id,
  t.device as device,
  t.solution as solution,
  t.station_lat as station_lat,
  t.station_lon as station_lon,
  r.guid as guid,
  r.name as name,
  r.stop_number as stop_number,
  ST_Distance(ST_Point(r.lat, r.lon), ST_Point(t.station_lat, t.station_lon)) as distance_km
FROM most_recent_schedule as r
CROSS JOIN triaged_latlon as t
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.guid ORDER BY distance_km ASC) = 1
);

CREATE temporary VIEW nearest_tech AS
SELECT
    tower_id,
    device_id,
    device,
    guid,
    name,
    solution,
    min(distance_km) as distance_km,
    min(station_lat) as station_lat,
    min(station_lon) as station_lon
FROM tower_service_assignments
group by tower_id, device_id, device, guid, name, solution;

create temporary view triaged_devices_reshape as (  
  SELECT tower_id,
         device_id,
         device,
         CASE WHEN STARTSWITH(solution, 'AUTOMATED') THEN solution ELSE CONCAT('Deploying Field Technician ', name, ' with instruction ', solution) END AS status,
         current_timestamp() AS event_timestamp
  FROM nearest_tech
);

create flow triaged_device_updates as 
auto cdc into telecommunications.self_healing_networks.device_status_st_pipe
from stream(triaged_devices_reshape)
keys (device_id)
sequence by event_timestamp
columns * except (op, event_timestamp)
stored as scd type 1;