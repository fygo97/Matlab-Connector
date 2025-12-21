---
layout: page
title: "MATLAB Connector (Users)"
toc: true
docs_area: "users"
doc_type: doc
tags: Matlab Connector, Matlab Toolbox, Matlab, Connector
search: true
lang: en
---

# MATLAB Connector (User Documentation)

## Setting up your connector
The Polypheny-Matlab connector is supplied using a PolyphenyConnector.mltbx file that will automatically install the necessary package in your Matlab environment. To install it:
- open Matlab and wait until it is ready to run
- double-click the supplied PolyphenyConnector.mltbx and wait a few seconds. Matlab should inform you the package was successfully installed.

> **Note**
>
> MATLAB provides a graphical UI to manage installed Add-Ons. In practice, this UI can be unreliable.
> If uninstalling the Polypheny Connector via the UI fails (e.g. when installing an updated version),
> the toolbox can be force-removed manually.

### Manual Uninstall (Force Removal)

#### 1. Locate the installed toolbox

Run the following in the MATLAB console:

    which polypheny.Polypheny -all

This returns the folder where the toolbox is installed, typically of the form:

    <path>\MathWorks\MATLAB Add-Ons\Toolboxes\PolyphenyConnector

#### 2. Delete the toolbox folder

Delete the entire `PolyphenyConnector` directory from disk.
If files are locked on Windows, close MATLAB first and then delete the folder.

#### 3. Remove the toolbox from the MATLAB search path

    rmpath('<path-to-PolyphenyConnector-parent>')

#### 4. Update MATLAB’s internal toolbox cache

    rehash toolboxcache

#### 5. Persist the updated search path

    savepath

#### Fallback (only if the path is badly polluted)

    restoredefaultpath
    rehash toolboxcache
    savepath

This procedure bypasses MATLAB’s Add-On Manager and ensures the toolbox is fully removed
from disk, the search path, and MATLAB’s internal cache. Note the PolyphenyConnector might not show up in Matlabs Add-on UI anymore, even if the installation was successful.


## MATLAB Connector – Usage Examples
This section demonstrates how to use the Polypheny MATLAB connector.  
All examples assume a local Polypheny instance running on port `20590`.

---

### Opening and Closing a Connection
Create a connection by specifying host, port, username, and password and close the connection afterwards
```matlab
conn = polypheny.Polypheny( 'host', 'port', 'username', 'password' );
% your code goes here
conn.close();
```
To test this with your local machine as host we could do
```matlab
conn = polypheny.Polypheny( 'localhost', int32(20590), 'username', 'pa' );
% your code goes here
conn.close();
```
### Executing queries
Queries are executed using
```matlab
result = conn.query( 'language', 'namespace', queryString );
```
where `language` is an element of `{'sql', 'mongo', 'cypher'}`, `'namespace'` is the name of the namespace the query targets in the database and `queryString` is the string passed to the database.

> **Note:**
> For Mongo queries the namespace argument is necessary for the creation and deletion of data structures in the backend. For SQL queries the namespace argument has no consequence and thus does not need to be set. In the following examples we will therefore use `""` as namespace argument.

### Executing SQL-queries
Let us look at some practical examples
```matlab
conn.query( "sql", "", "DROP TABLE IF EXISTS test" );
conn.query( "sql", "", "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR)" );
conn.query( "sql", "", "INSERT INTO test VALUES (1,'Alice'),(2,'Bob')" );
```

#### Scalar results
If a query returns a single value, the result is returned as a MATLAB scalar. 
```matlab
x = conn.query( "sql", "", "SELECT COUNT(*) FROM test" )
```
will produce the output
```matlab
x = 2
```

#### Tabular results
Queries returning multiple rows and columns are returned as a matlab `table`
```matlab
T = conn.query( "sql", "", "SELECT * FROM test ORDER BY id" );
```
It is possible to access the columns directly by doing
```matlab
T.id
T.name
```
which in our example yields the output
```matlab
T.id   = [1; 2]
T.name = {'Alice'; 'Bob'}
```

#### Empty results
Empty results
```matlab
T = conn.query( "sql", "~", "SELECT * FROM test WHERE id = 999" );
```
will be returned as empty MATLAB array:
```matlab
T = []
```


### SQL Batch Queries
Multiple non-SELECT statements can be executed using `queryBatch`    
```matlab
conn.query( "sql", "", "DROP TABLE IF EXISTS test" );
conn.query( "sql", "", "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR)" );

queries = {
    "INSERT INTO test VALUES (1,'Alice')"
    "INSERT INTO test VALUES (2,'Bob')"
};

result = conn.queryBatch( "sql", "", queries );
```
where for `queries` containing `n` single queries, the result will be a `n x 1` vector with the i-th entry representing how many rows in the table the i-th query affected. Since each query inserts exactly one entry into one row, our example this yields the output
```matlab
    RowsAffected
    ____________

         1
         1
```
Should single queries of a batch fail a rollback will be triggered (all or nothing principle).