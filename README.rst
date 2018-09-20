Postgres python
---------------
A simple api to read, write, delete and update in a postgres database

Installation : Linux/Mac
---------------
  
.. code-block:: bash

    $ pip install pg_python


Server Initialization 
---------------

.. code-block:: bash

    $ from pg_python.pg_python import *
    $ pgs = pg_server(db_name, username, password, host_address)

Schema Definition (Optional)(Not available)
---------------

.. code-block:: bash

    $ schema_map = Schema()
    $ pgs.add_schema(db_name, schema_map)

Read operation
---------------
Return value is a list of dictionaries.
Every dictionary is representative of the row that is fetched.
Each dictionary's key is the same as the column name fetched. 


.. code-block:: bash

    $ value_list = read(table_name, keys_to_get_dict, where_kv_dict, limit, order_by, order_type, operator_string)
    
**Parameter information :** 

- keys_to_get_dict(a list of column names to fetch)
- where_kv_dict(a dictionary to specify the 'Where' clause)

**Example usage :** 
print pg_python.read("mock_table", ['column2','column3'], {"column_1":'55'})
  Resulting query:  SELECT column2, column3   FROM mock_table WHERE column1 = %s, ['55']
  
  Sample output:  [{'column2': 'column_data_A', 'column3': 'column_data_Z'}, {'column2': 'column_data_P', 'column3': 'column_data_Q'}]
   
Write operation
---------------

.. code-block:: bash

    $ write(table_name, kv_dict)

Overwrite operation
---------------

.. code-block:: bash

    $ write(table_name, kv_dict)

Update operation
---------------

.. code-block:: bash


    $ update(table, update_kv_map, where_kv_map)

Delete operation
---------------

.. code-block:: bash


    $ delete(table, where_kv_map, single_row)
