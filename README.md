# Random User Virtual Schema

## Overview
With this project a REST API is connected to Exasol using Virtual Schema. The template for this project was this [repository](https://github.com/exasol/openweather-virtual-schema).

## Getting Started
Firstly you have to create the Virtual Schema. To do so copy the contents of [user-virtual-schema.sql](https://github.com/dnsdnsdns/exasol-restapi-virtual-schema/blob/master/user-virtual-schema.sql) into your SQL editor and run the first two `CREATE OR REPLACE` statements

After the scripts are created you need to fill in the placeholders for the Virtual Schema creation.

### DISCLAIMER
API-KEY is not needed for the Random User API.

```sql
CREATE VIRTUAL SCHEMA <NAME>
USING openweather_vs_scripts.openweather_adapter
WITH API_KEY = 'your key'
     LOG_LISTENER = 'your log listener IP'
     LOG_LISTENER_PORT = 'your log listener port'
     LOG_LEVEL = 'INFO or WARNING'
/
``` 

After the Virtual Schema is creates succesfully you can run SQL queries from your database against the API. Please refer to the example SQL statements at the bottom of [user-virtual-schema.sql](https://github.com/dnsdnsdns/exasol-restapi-virtual-schema/blob/master/user-virtual-schema.sql) .

## Behind the scenes

A more detailed explanation on how this Virtual Schema works can be found in the Exasol Community. Have a look [here](https://community.exasol.com/t5/database-features/using-virtual-schema-on-a-rest-api/ta-p/2298)

## Suported Features
All expressions work in both directions:
`[city_name = 'Stuttgart'] == ['Stuttgart' = city_name]`

### Filter by gender
```sql
SELECT * FROM USER_VS.USER_TABLE
WHERE  gender = 'female';
```

## Deleting the schema

In order to delete the Virtual Schema and it's schema  run:

```sql
DROP FORCE VIRTUAL SCHEMA user_vs CASCADE;
DROP SCHEMA restapi_vs_scripts CASCADE;
```
