USE ROLE MERCHANT_GROWTH_ANALYST;

--stage table

CREATE OR REPLACE TABLE analyst_merchant_growth.onlinestore_stage_merchant_flow
(
	MERCHANT_CODE VARCHAR,
	EVENT_TYPE VARCHAR,
	SIGNUP_DATE TIMESTAMPNTZ,
	FIRST_PUBLISHED_STORE TIMESTAMPNTZ,
	LAST_PUBLISHED_STORE TIMESTAMPNTZ,
	LAST_UNPUBLISHED_STORE TIMESTAMPNTZ,
	DATE_ONBOARDING_GC TIMESTAMPNTZ,
	DATE_LAST_ITEMSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_SHIPPINGMETHODSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_STORELAYOUTSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_STORELINKSTEPCHANGED TIMESTAMPNTZ,
	UPDATED_AT TIMESTAMPLTZ
);

--presentation fact table

CREATE OR REPLACE TABLE analyst_merchant_growth.onlinestore_fact_merchant_flow
(
	MERCHANT_CODE VARCHAR,
	SIGNUP_DATE TIMESTAMPNTZ,
	FIRST_PUBLISHED_STORE TIMESTAMPNTZ,
	DATE_LAST_PUBLISH_STATUS TIMESTAMPNTZ,
	CURRENT_PUBLISH_STATUS VARCHAR,
	DATE_ONBOARDING_GC TIMESTAMPNTZ,
	DATE_LAST_ITEMSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_SHIPPINGMETHODSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_STORELAYOUTSTEPCHANGED TIMESTAMPNTZ,
	DATE_LAST_STORELINKSTEPCHANGED TIMESTAMPNTZ,
	UPDATED_AT TIMESTAMPLTZ
);


--initial loading
--
-- INSERT  INTO ANALYST_MERCHANT_GROWTH.onlinestore_stage_merchant_flow
--
-- CREATE TABLE ANALYST_MERCHANT_GROWTH.onlinestore_stage_merchant_flow_2 AS
-- SELECT merchant_code,
--        event_type,
--        CASE WHEN event_type = 'SignUp' then MIN(created_at) END                                   AS signup_date,
--        CASE
--            WHEN event_type = 'PublishedStore'
--                then MIN(data:timestampCreatedAt::timestamp) END                                   AS first_published_store,
--        CASE
--            WHEN event_type = 'PublishedStore'
--                then MAX(data:timestampCreatedAt::timestamp) END                                   AS last_published_store,
--        CASE
--            WHEN event_type = 'UnpublishedStore'
--                then MAX(data:timestampCreatedAt::timestamp) END                                   AS last_unpublished_store,
--        CASE WHEN event_type = 'OnboardingGuideCompleted' then MIN(created_at) END                 AS date_onboarding_gc,
--        CASE WHEN event_type = 'ItemStepChanged' then MAX(created_at) END                          AS date_last_ItemStepChanged,
--        CASE
--            WHEN event_type = 'ShippingMethodStepChanged'
--                then MAX(created_at) END                                                           AS date_last_ShippingMethodStepChanged,
--        CASE WHEN event_type = 'StoreLayoutStepChanged' then MAX(created_at) END                   AS date_last_StoreLayoutStepChanged,
--        CASE WHEN event_type = 'StoreLinkStepChanged' then MAX(created_at) END                     AS date_last_StoreLinkStepChanged,
--        current_timestamp                                                                          AS updated_at
-- from analyst_merchant_growth.onlinestore_events
-- WHERE event_type IN ('SignUp', 'PublishedStore', 'UnpublishedStore', 'OnboardingGuideCompleted',
--                      'ItemStepChanged', 'ShippingMethodStepChanged', 'StoreLayoutStepChanged', 'StoreLinkStepChanged'
--     )
-- GROUP BY merchant_code, event_type;
--
-- INSERT INTO analyst_merchant_growth.onlinestore_fact_merchant_flow
--
-- SELECT merchant_code,
--        MIN(signup_date)                         as signup_date,
--        MIN(first_published_store)               as first_published_store,
--        CASE
--            WHEN MAX(last_published_store) >= MAX(last_unpublished_store)
--                THEN MAX(last_published_store)
--            ELSE MAX(last_unpublished_store) END AS date_last_publish_status,
--        CASE
--            WHEN MAX(LAST_PUBLISHED_STORE) IS NOT NULL OR MAX(last_unpublished_store) IS NOT NULL
--                THEN CASE WHEN MAX(LAST_PUBLISHED_STORE) >= MAX(last_unpublished_store)
--                    THEN 'PublishedStore'
--                    ELSE 'UnpublishedStore' END
--            ELSE '' END                          AS current_publish_status,
--        MIN(date_onboarding_gc)                  AS date_onboarding_gc,
--        MAX(date_last_ItemStepChanged)           AS date_last_ItemStepChanged,
--        MAX(date_last_ShippingMethodStepChanged) AS date_last_ShippingMethodStepChanged,
--        MAX(date_last_StoreLayoutStepChanged)    AS date_last_StoreLayoutStepChanged,
--        MAX(date_last_StoreLinkStepChanged)      AS date_last_StoreLinkStepChanged,
--        max(updated_at)                        AS updated_at
-- FROM analyst_merchant_growth.onlinestore_stage_merchant_flow
-- GROUP BY merchant_code;

--TASKS;

ALTER TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_ALL_EVENTS SUSPEND;

CREATE OR REPLACE TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_STAGE_PART1
  WAREHOUSE = ANALYTICS_WH
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
  AFTER SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_ALL_EVENTS

AS
    MERGE INTO analyst_merchant_growth.onlinestore_stage_merchant_flow osmf
        USING
            (SELECT merchant_code,
                    event_type,
                    CASE WHEN event_type = 'SignUp' THEN MIN(created_at) END                                   AS signup_date,
                    CASE
                        WHEN event_type = 'PublishedStore'
                            then MIN(data:timestampCreatedAt::timestamp) END                                   AS first_published_store,
                    CASE
                        WHEN event_type = 'PublishedStore'
                            then MAX(data:timestampCreatedAt::timestamp) END                                   AS last_published_store,
                    CASE
                        WHEN event_type = 'UnpublishedStore'
                            then MAX(data:timestampCreatedAt::timestamp) END                                   AS last_unpublished_store,
                    CASE
                        WHEN event_type = 'OnboardingGuideCompleted'
                            then MIN(created_at) END                                                           AS date_onboarding_gc,
                    CASE WHEN event_type = 'ItemStepChanged' then MAX(created_at) END                          AS date_last_ItemStepChanged,
                    CASE
                        WHEN event_type = 'ShippingMethodStepChanged'
                            then MAX(created_at) END                                                           AS date_last_ShippingMethodStepChanged,
                    CASE WHEN event_type = 'StoreLayoutStepChanged' then MAX(created_at) END                   AS date_last_StoreLayoutStepChanged,
                    CASE WHEN event_type = 'StoreLinkStepChanged' then MAX(created_at) END                     AS date_last_StoreLinkStepChanged,
                    current_timestamp                                                                          AS updated_at
             FROM analyst_merchant_growth.onlinestore_events
             WHERE event_type IN ('SignUp', 'PublishedStore', 'UnpublishedStore', 'OnboardingGuideCompleted',
                                  'ItemStepChanged', 'ShippingMethodStepChanged', 'StoreLayoutStepChanged',
                                  'StoreLinkStepChanged')
            AND updated_at >= (select MAX(updated_at) from sumup_dwh_prod.analyst_merchant_growth.onlinestore_stage_merchant_flow)
             GROUP BY merchant_code, event_type) new ON new.MERCHANT_CODE = osmf.MERCHANT_CODE AND
                                                     new.EVENT_TYPE = osmf.EVENT_TYPE

        WHEN MATCHED THEN UPDATE SET
        osmf.last_published_store = new.last_published_store,
        osmf.last_unpublished_store = new.last_unpublished_store,
        osmf.date_last_ShippingMethodStepChanged = new.date_last_ShippingMethodStepChanged,
        osmf.date_last_StoreLayoutStepChanged = new.date_last_StoreLayoutStepChanged,
        osmf.date_last_StoreLinkStepChanged = new.date_last_StoreLinkStepChanged,
        osmf.updated_at = current_timestamp
    WHEN NOT MATCHED THEN INSERT (merchant_code, event_type,signup_date, first_published_store, last_published_store, last_unpublished_store, date_onboarding_gc, date_last_ItemStepChanged,
                                  date_last_ShippingMethodStepChanged,date_last_StoreLayoutStepChanged,date_last_StoreLinkStepChanged,updated_at)
        VALUES (merchant_code, event_type,signup_date, first_published_store, last_published_store, last_unpublished_store, date_onboarding_gc, date_last_ItemStepChanged,
                                  date_last_ShippingMethodStepChanged,date_last_StoreLayoutStepChanged,date_last_StoreLinkStepChanged,updated_at);


ALTER TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_STAGE_PART1 RESUME;
ALTER TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_ALL_EVENTS RESUME;

--task 2

ALTER TASK  SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_STAGE_PART1 SUSPEND;

CREATE OR REPLACE TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_FACT_PART1
    WAREHOUSE = ANALYTICS_WH
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
    AFTER SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_STAGE_PART1
    AS
        MERGE INTO analyst_merchant_growth.onlinestore_fact_merchant_flow ofmf
            USING
                (SELECT merchant_code,
                        MIN(signup_date)                         as signup_date,
                        MIN(first_published_store)               as first_published_store,
                        CASE
                            WHEN MAX(last_published_store) >= MAX(last_unpublished_store)
                                THEN MAX(last_published_store)
                            ELSE MAX(last_unpublished_store) END AS date_last_publish_status,
                        CASE
                            WHEN MAX(LAST_PUBLISHED_STORE) IS NOT NULL OR MAX(last_unpublished_store) IS NOT NULL
                                THEN CASE
                                         WHEN MAX(LAST_PUBLISHED_STORE) >= MAX(last_unpublished_store)
                                             THEN 'PublishedStore'
                                         ELSE 'UnpublishedStore' END
                            ELSE '' END                          AS current_publish_status,
                        MIN(date_onboarding_gc)                  AS date_onboarding_gc,
                        MAX(date_last_ItemStepChanged)           AS date_last_itemstepchanged,
                        MAX(date_last_ShippingMethodStepChanged) AS date_last_shippingmethodstepchanged,
                        MAX(date_last_StoreLayoutStepChanged)    AS date_last_storelayoutstepchanged,
                        MAX(date_last_StoreLinkStepChanged)      AS date_last_storelinkstepchanged,
                        CURRENT_TIMESTAMP                        AS updated_at
                 FROM analyst_merchant_growth.onlinestore_stage_merchant_flow
                 WHERE UPDATED_AT >=
                       (SELECT MAX(updated_at) FROM analyst_merchant_growth.onlinestore_fact_merchant_flow)
                 GROUP BY merchant_code) new ON new.merchant_code = ofmf.merchant_code
            WHEN MATCHED THEN UPDATE SET
                ofmf.signup_date = LEAST(IFNULL(new.signup_date, ofmf.signup_date), ofmf.signup_date),
                ofmf.first_published_store =
                        LEAST(IFNULL(new.first_published_store, ofmf.first_published_store), ofmf.first_published_store),
                ofmf.date_last_publish_status =
                        GREATEST(ifnull(new.date_last_publish_status, ofmf.date_last_publish_status),
                                 ofmf.date_last_publish_status),
                ofmf.current_publish_status = IFNULL(new.current_publish_status, ofmf.current_publish_status),
                ofmf.date_onboarding_gc =
                        LEAST(IFNULL(new.date_onboarding_gc, ofmf.date_onboarding_gc), ofmf.date_onboarding_gc),
                ofmf.date_last_itemstepchanged =
                        GREATEST(IFNULL(new.date_last_itemstepchanged, ofmf.date_last_itemstepchanged),
                                 ofmf.date_last_itemstepchanged),
                ofmf.date_last_shippingmethodstepchanged =
                        GREATEST(IFNULL(new.date_last_shippingmethodstepchanged,
                                        ofmf.date_last_shippingmethodstepchanged),
                                 ofmf.date_last_shippingmethodstepchanged),
                ofmf.date_last_storelayoutstepchanged =
                        GREATEST(IFNULL(new.date_last_shippingmethodstepchanged,
                                        ofmf.date_last_storelayoutstepchanged),
                                 ofmf.date_last_storelayoutstepchanged),
                ofmf.date_last_storelinkstepchanged =
                        GREATEST(IFNULL(new.date_last_storelinkstepchanged, ofmf.date_last_storelinkstepchanged),
                                 ofmf.date_last_storelinkstepchanged),
                ofmf.updated_at = current_timestamp
            WHEN NOT MATCHED THEN INSERT (merchant_code, signup_date, first_published_store, date_last_publish_status,
                                          current_publish_status, date_onboarding_gc, date_last_itemstepchanged,
                                          date_last_shippingmethodstepchanged, date_last_storelayoutstepchanged,
                                          date_last_storelinkstepchanged, updated_at)
                VALUES (merchant_code, signup_date, first_published_store, date_last_publish_status,
                        current_publish_status,
                        date_onboarding_gc, date_last_itemstepchanged,
                        date_last_shippingmethodstepchanged, date_last_storelayoutstepchanged,
                        date_last_storelinkstepchanged, updated_at);



ALTER TASK SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_FACT_PART1 RESUME;
ALTER TASK  SUMUP_DWH_PROD.ANALYST_MERCHANT_GROWTH.TASK_REFRESH_ONLINE_STORE_STAGE_PART1 RESUME;


USE SCHEMA ANALYST_MERCHANT_GROWTH;
SELECT CURRENT_TIMESTAMP(), *
  FROM TABLE(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-15,current_timestamp()),
    result_limit => 10,
   -- database_name => ‘SUMUP_DWH_PROD’,
    task_name=>'TASK_REFRESH_ONLINE_STORE_FACT_PART1'))
WHERE database_name = 'SUMUP_DWH_PROD'
  ORDER BY scheduled_time DESC;
