# monitor-dbt-artifact

This repo's purpose is only to store queries used to create a Snowsight dashboard to monitor dbt projects.
The tables used are created by the dbt package [dbt-artifacts](https://github.com/brooklyn-data/dbt_artifacts/). 

* Data warehouse: Snowflake
* Database: monitoring
* Schema: dbt_artifacts

I would recommend using dbt to ship those queries as views (need to remove where clauses using Snowsight filters) and use the views directly within Snowsight or any other BI tool. 
