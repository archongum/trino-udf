# Installation
1. `mvn clean assembly:assembly`
2. Copy `trino-udf-*-jar-with-dependencies.jar` to `${TRINO_HOME}/plugin/custom-functions/` in all Trino nodes.
(create directory if not exists)
3. Restart Trino cluster

# Versions
- JDK-11
- Trino-380

# Functions
## Scalar Functions
| Function                | Return Type | Argument Types | Description                                                                          | Usage                                   |
|-------------------------|-------------|----------------|--------------------------------------------------------------------------------------|-----------------------------------------|
| first_day               | date        | date           | first day of month                                                                   | first_day(current_date)                 |
| last_day                | date        | date           | last day of month                                                                    | last_day(current_date)                  |
| yesterday               | date        |                | yesterday                                                                            | yesterday()                             |
| last_second             | timestamp   | date           | last second of the date                                                              | last_second(current_date)               |
| yesterday_last_second   | timestamp   |                | last second of yesterday                                                             | yesterday_last_second()                 |
| to_datetime             | timestamp   | date, varchar  | combine the two args                                                                 | to_datetime(current_date, '23:59:59')   |
| array_max_count_element | T           | array(T)       | Get maximum count element (null is not counting; if has multiple return one of them) | array_max_count_element(array['1','2']) |
| rand                    | double      | varchar        | Return double in [0,1]                                                               | rand(varchar)                           |

## Aggregate Functions
| Function                   | Return Type | Argument Types | Description                                                                          | Usage                   |
|----------------------------| ----------- |----------------| ------------------------------------------------------------------------------------ | ----------------------- |
| max_count_element          | VARCHAR     | VARCHAR        | Get maximum count element (null is not counting; if has multiple return one of them) | max_count_element(name) |
| array_agg_distinct         | INTEGER     | array(VARCHAR) | Count distinct array elements. input: array(VARCHAR), output: integer.               | array_agg_distinct(ids) |
| array_agg_distinct_integer | INTEGER     | array(INTEGER) | Count distinct array elements. input: array(INTEGER), output: integer.               | array_agg_distinct(ids) |
