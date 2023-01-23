/*
This query returns every table that has not been updated for a month
or those not present in the dbt monitoring table.
Using filters:
* :project_name

Requirements on the querying role:
* **select** privilege on the schema **information_schema**
in each database it is monitoring
* **usage** on all schemas in each database it is monitoring
* **references** on all tables in each database it is monitoring
* **references** on all views in each database it is monitoring
*/

with all_infos as (
    select
        'project_name' as project_name
        , *
    from
        project_database.information_schema.tables
    union
    select
        'project_name_2' as project_name
        , *
    from
        project_database_2.information_schema.tables
)

, wanted_info as (
    select
        *
    from
        all_infos
    where
        i.project_name = :project_name
        and table_schema != 'INFORMATION_SCHEMA'
)

, calc as (
select
    info.table_catalog as db_name
    , info.table_schema as schema_name
    , info.table_name as table_name
    , info.table_type
    , max(models.run_started_at) as last_ran
    , convert_timezone('Europe/Warsaw', 'UTC', max(info.last_altered)) as last_altered
from
    wanted_info as info
left join
    monitoring.dbt_artifacts.model_executions as models
    on lower(info.table_name) = lower(models.name)
    and lower(info.table_schema) = lower(models.schema)

group by
    1, 2, 3, 4
)

select
    *
from
    calc
where
    last_altered::date < dateadd(month, -1, sysdate()::date)
    or last_ran is null
order by
    table_type
    , db_name
    , schema_name
    , last_altered
