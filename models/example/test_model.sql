/* This is the MODEL DDL, where you specify model metadata and configuration information. */
MODEL (
  name DBT.DBT_PROJECT,
  kind FULL
);

/*
  Optional pre-statements that will run before the model's query.
  You should NOT do things that cause side effects that could error out when
  executed concurrently with other statements, such as creating physical tables.
*/ /* CACHE TABLE countries AS SELECT * FROM raw.countries; */ /*
  This is the single query that defines the model's logic.
  Although it is not required, it is considered best practice to explicitly
  specify the type for each one of the model's columns through casting.
*/
SELECT
  ID
FROM DBT_PROJECT.my_first_dbt_model
LIMIT 10