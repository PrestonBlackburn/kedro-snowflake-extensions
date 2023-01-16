# kedro-snowflake-extensions
A few Kedro extensions for working with Snowflake.  

See the Kedro docs for more about [custom datasets](https://kedro.readthedocs.io/en/stable/extend_kedro/custom_datasets.html)  

<br />

### Custom datasets:
```
datasets
| - dummies.py
|    - TableNameDataSet
|    - SprocNameDataSet
| - snowflake.py
|    - SQLTableDataSet
|    - SQLQueryDataSet
| - snowpark.py
|    - SnowparkSessionDataSet
```

### Use cases for datasets:
- `datasets.dummies.TableNameDataSet`: A dummy dataset that stores the full table name as a string. This can be used where you just need a pointer to a table.  

- `datasets.dummies.SprocNameDataSet`: A dummy dataset that stores a stored procedure name as a string. This can be used where you just need a pointer to a stored procedure.

- `datasets.snowflake.SQLTableDataSet`: Kedro offers a `kedro.extras.datasets.pandas.SQLTableDataSet` that is similar. This dataset uses the Snowflake Connector instead of SQL Alchemy with pandas to query Snowflake and return a table as a dataframe.

- `datasets.snowflake.SQLQueryDataSet`: Kedro offers a `kedro.extras.datasets.pandas.SQLQueryDataSet` that is similar. This dataset uses the Snowflake Connector instead of SQL Alchemy with pandas to query Snowflake and return a table as a dataframe.

- `datasets.snowpark.SnowparkSessionDataSet`: This dataset is useful if you needed to call a snowpark session from inside a node. By using the `SnowparkSessionDataSet`, you can pass a session object to a node instaed of passing credentials to the node. 