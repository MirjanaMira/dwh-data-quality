USE ROLE MERCHANT_GROWTH_ANALYST;

CREATE OR REPLACE TABLE analyst_merchant_growth.onlinestore_events
(
	CREATED_AT TIMESTAMPNTZ,
	FILE_ROW_NUMBER NUMBER(8),
	DATA VARIANT,
	CREATED_AT_RAW VARIANT,
	DEVICE VARCHAR,
	EVENT_ID VARCHAR,
	EVENT_TYPE VARCHAR,
	MERCHANT_CODE VARCHAR
);

--initial loading:

-- insert  INTO ANALYST_MERCHANT_GROWTH.onlinestore_events
-- SELECT MIN(CREATED_AT)            AS CREATED_AT,
--                 FILE_ROW_NUMBER,
--                 PARSE_JSON(raw: data)      as data,
--                 raw:created_at             as created_at_raw,
--                 raw:device::varchar        as device,
--                 raw:event_id::varchar      as event_id,
--                 raw:event_type::varchar    as event_type,
--                 raw:merchant_code::varchar as merchant_code
--          FROM sumup_dwh_prod.src_sumup_online_store.bi_events
--          WHERE raw: data IS NOT NULL
--          --  and created_at >= (select max(created_at) from sumup_dwh_prod.ANALYST_MERCHANT_GROWTH.onlinestore_events)
--          GROUP BY FILE_ROW_NUMBER,
--                   PARSE_JSON(raw: data),
--                   raw:created_at,
--                   raw:device::varchar,
--                   raw:event_id::varchar,
--                   raw:event_type::varchar,
--                   raw:merchant_code::varchar;


CREATE OR REPLACE TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_ALL_EVENTS
  WAREHOUSE = ANALYTICS_WH
  SCHEDULE = 'USING CRON 0 2 * * * Europe/Berlin'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS

MERGE INTO ANALYST_MERCHANT_GROWTH.onlinestore_events oe
    USING
        (SELECT MIN(CREATED_AT)            AS CREATED_AT,
                FILE_ROW_NUMBER,
                PARSE_JSON(raw: data)      as data,
                raw:created_at             as created_at_raw,
                raw:device::varchar        as device,
                raw:event_id::varchar      as event_id,
                raw:event_type::varchar    as event_type,
                raw:merchant_code::varchar as merchant_code
         FROM sumup_dwh_prod.src_sumup_online_store.bi_events
         WHERE raw: data IS NOT NULL
           and created_at >= (select max(created_at) from sumup_dwh_prod.ANALYST_MERCHANT_GROWTH.onlinestore_events)
         GROUP BY FILE_ROW_NUMBER,
                  PARSE_JSON(raw: data),
                  raw:created_at,
                  raw:device::varchar,
                  raw:event_id::varchar,
                  raw:event_type::varchar,
                  raw:merchant_code::varchar) new ON new.CREATED_AT = oe.CREATED_AT AND
                                                     new.FILE_ROW_NUMBER = oe.FILE_ROW_NUMBER AND
                                                     new.event_id = oe.EVENT_ID
    WHEN MATCHED THEN UPDATE SET
        oe.data = new.data,
        oe.CREATED_AT_RAW = new.created_at_raw,
        oe.device = new.device,
        oe.EVENT_TYPE = new.event_type,
        oe.MERCHANT_CODE = new.merchant_code
    WHEN NOT MATCHED THEN INSERT (created_at, file_row_number, data, created_at_raw, device, event_id, event_type,
                                  merchant_code)
        VALUES (created_at, file_row_number, data, created_at_raw, device, event_id, event_type, merchant_code);


ALTER TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_ALL_EVENTS RESUME;

USE SCHEMA ANALYST_MERCHANT_GROWTH;
SELECT CURRENT_TIMESTAMP(), *
  FROM TABLE(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-15,current_timestamp()),
    result_limit => 10,
   -- database_name => ‘SUMUP_DWH_PROD’,
    task_name=>'TASK_REFRESH_ONLINE_STORE_ALL_EVENTS'))
WHERE database_name = 'SUMUP_DWH_PROD'
  ORDER BY scheduled_time DESC;

--
-- select *
-- from SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.onlinestore_events;
