

{{ config (
    materialized="table"
)}}
            
with __dbt__cte__COVID_DATA_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "VARISU".COVID._AIRBYTE_RAW_COVID_DATA
select
    to_varchar(get_path(parse_json(_airbyte_data), '"key"')) as KEY,
    to_varchar(get_path(parse_json(_airbyte_data), '"date"')) as DATE,
    to_varchar(get_path(parse_json(_airbyte_data), '"new_tested"')) as NEW_TESTED,
    to_varchar(get_path(parse_json(_airbyte_data), '"new_deceased"')) as NEW_DECEASED,
    to_varchar(get_path(parse_json(_airbyte_data), '"total_tested"')) as TOTAL_TESTED,
    to_varchar(get_path(parse_json(_airbyte_data), '"new_confirmed"')) as NEW_CONFIRMED,
    to_varchar(get_path(parse_json(_airbyte_data), '"new_recovered"')) as NEW_RECOVERED,
    to_varchar(get_path(parse_json(_airbyte_data), '"total_deceased"')) as TOTAL_DECEASED,
    to_varchar(get_path(parse_json(_airbyte_data), '"total_confirmed"')) as TOTAL_CONFIRMED,
    to_varchar(get_path(parse_json(_airbyte_data), '"total_recovered"')) as TOTAL_RECOVERED,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "VARISU".COVID._AIRBYTE_RAW_COVID_DATA as table_alias
-- COVID_DATA
where 1 = 1

),  __dbt__cte__COVID_DATA_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__COVID_DATA_AB1
select
    cast(KEY as 
    varchar
) as KEY,
    cast(DATE as 
    varchar
) as DATE,
    cast(NEW_TESTED as 
    varchar
) as NEW_TESTED,
    cast(NEW_DECEASED as 
    varchar
) as NEW_DECEASED,
    cast(TOTAL_TESTED as 
    varchar
) as TOTAL_TESTED,
    cast(NEW_CONFIRMED as 
    varchar
) as NEW_CONFIRMED,
    cast(NEW_RECOVERED as 
    varchar
) as NEW_RECOVERED,
    cast(TOTAL_DECEASED as 
    varchar
) as TOTAL_DECEASED,
    cast(TOTAL_CONFIRMED as 
    varchar
) as TOTAL_CONFIRMED,
    cast(TOTAL_RECOVERED as 
    varchar
) as TOTAL_RECOVERED,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__COVID_DATA_AB1
-- COVID_DATA
where 1 = 1

),  __dbt__cte__COVID_DATA_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__COVID_DATA_AB2
select
    md5(cast(coalesce(cast(KEY as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '') || '-' || coalesce(cast(NEW_TESTED as 
    varchar
), '') || '-' || coalesce(cast(NEW_DECEASED as 
    varchar
), '') || '-' || coalesce(cast(TOTAL_TESTED as 
    varchar
), '') || '-' || coalesce(cast(NEW_CONFIRMED as 
    varchar
), '') || '-' || coalesce(cast(NEW_RECOVERED as 
    varchar
), '') || '-' || coalesce(cast(TOTAL_DECEASED as 
    varchar
), '') || '-' || coalesce(cast(TOTAL_CONFIRMED as 
    varchar
), '') || '-' || coalesce(cast(TOTAL_RECOVERED as 
    varchar
), '') as 
    varchar
)) as _AIRBYTE_COVID_DATA_HASHID,
    tmp.*
from __dbt__cte__COVID_DATA_AB2 tmp
-- COVID_DATA
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__COVID_DATA_AB3
select
    KEY,
    DATE,
    NEW_TESTED,
    NEW_DECEASED,
    TOTAL_TESTED,
    NEW_CONFIRMED,
    NEW_RECOVERED,
    TOTAL_DECEASED,
    TOTAL_CONFIRMED,
    TOTAL_RECOVERED,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_COVID_DATA_HASHID
from __dbt__cte__COVID_DATA_AB3
-- COVID_DATA from "VARISU".COVID._AIRBYTE_RAW_COVID_DATA
where 1 = 1

            ) order by (_AIRBYTE_EMITTED_AT)
      );
    alter table "VARISU".COVID."COVID_DATA" cluster by (_AIRBYTE_EMITTED_AT);