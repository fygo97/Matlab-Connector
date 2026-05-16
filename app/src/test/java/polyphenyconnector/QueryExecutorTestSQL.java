package polyphenyconnector;

import java.util.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.*;
import org.polypheny.jdbc.types.TypedValue;

import java.sql.Connection;

public class QueryExecutorTestSQL {

    private static PolyphenyConnection myconnection;
    private static QueryExecutor myexecutor;


    @BeforeAll
    static void setUpNamespaceAndTable() throws Exception {

        // Wait for Polypheny to be available and connect to localhost. We do this because we run all the JUnit tests on our local machine. 
        PolyphenyConnectionTestHelper.waitForPolypheny();
        Thread.sleep( 4000 );
        myconnection = new PolyphenyConnection( "localhost", 20590, "pa", "" );
        myexecutor = new QueryExecutor( myconnection );

        // 1. Setup tables for .execute()
        // Delete any TABLE called <unittest_table> and any NAMESPACE <unittest_namespace> if it exists. This is important so we can insert it
        // cleanly, in case tests break mid run the cleanup "@Afterall" might not have been executed properly.
        try {
            myexecutor.executeSql( "DROP TABLE IF EXISTS unittest_namespace.unittest_table" );
            myexecutor.executeSql( "DROP NAMESPACE IF EXISTS unittest_namespace" );
            myexecutor.executeSql( "DROP NAMESPACE IF EXISTS shop" );
        } catch ( Exception ignored ) {
        }

        // Creates the NAMESPACE <unittest_namespace> and TABLE <unittest_table>.

        myexecutor.executeSql( "CREATE DOCUMENT NAMESPACE IF NOT EXISTS shop" );
        myexecutor.executeSql( "CREATE SCHEMA unittest_namespace" );
        myexecutor.executeSql( "CREATE TABLE unittest_namespace.unittest_table (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY(id))" );

        // 2. Setup tables for executeBatch()
        // Delete any tables that might still exist as described before.
        try {
            myexecutor.executeSql( "DROP TABLE IF EXISTS unittest_namespace.batch_table" );
        } catch ( Exception ignored ) {
        }
        myexecutor.executeSql( "CREATE TABLE unittest_namespace.batch_table (" + "emp_id INT NOT NULL, " + "name VARCHAR(100), " + "gender VARCHAR(10), " + "birthday DATE, " + "employee_id INT, " + "PRIMARY KEY(emp_id))" );

    }


    @AfterAll
    static void tearDownNamespaceAndTable() {
        // Cleans up the TABLE and NAMESPACE we created so we leave no trace after the tests.
        myexecutor.executeSql( "DROP TABLE IF EXISTS unittest_namespace.unittest_table" );
        myexecutor.executeSql( "DROP TABLE IF EXISTS unittest_namespace.batch_table" );
        myexecutor.executeSql( "DROP NAMESPACE IF EXISTS unittest_namespace" );
        myexecutor.executeSql( "DROP NAMESPACE IF EXISTS shop" );
        myconnection.close();
    }


    @BeforeEach
    void clearTable() {
        myexecutor.executeSql( "DELETE FROM unittest_namespace.unittest_table" );
        myexecutor.executeSql( "DELETE FROM unittest_namespace.batch_table" );
    }


    @AfterEach
    void clearTableAfter() {
        myexecutor.executeSql( "DELETE FROM unittest_namespace.unittest_table" );
        myexecutor.executeSql( "DELETE FROM unittest_namespace.batch_table" );
    }

    // ─────────────────────────────
    // Isolated branch tests (no table needed)
    // ─────────────────────────────


    @Test
    void testScalarCase() {
        Object result = myexecutor.executeSql( "SELECT 42 AS answer" );
        assertTrue( result instanceof Object[], "Expected tabular result" );

        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        String[] colNames = (String[]) arr[0]; // first entry is column names
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data (columns).
        double[] firstRow = (double[]) columns[0];

        assertArrayEquals( new String[]{ "answer" }, colNames );
        assertArrayEquals( colTypes, new String[]{ "double" } );
        assertArrayEquals( new double[]{ 42 }, firstRow );

    }


    @Test
    void testEmptyLiteral() {
        Object result = myexecutor.executeSql( "SELECT * FROM (SELECT 1) t WHERE 1=0" );
        assertNull( result, "Query with no rows should return null" );
    }


    @Test
    void testTableLiteral() {
        // create [a,b; 1,2; 3,4; 5,6 ]
        Object result = myexecutor.executeSql( "SELECT 1 AS odd, 2 AS even UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6" );
        assertTrue( result instanceof Object[], "Expected tabular result" );

        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        String[] colNames = (String[]) arr[0]; // first entry is column names
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data (columns).
        double[] firstCol = (double[]) columns[0];
        double[] secondCol = (double[]) columns[1];

        assertArrayEquals( new String[]{ "odd", "even" }, colNames, "Column names must match" );
        assertArrayEquals( colTypes, new String[]{ "double", "double" } );

        assertArrayEquals( firstCol, new double[]{ 1.0, 3.0, 5.0 } );
        assertArrayEquals( secondCol, new double[]{ 2.0, 4.0, 6.0 } );
    }


    @Test
    void testAllPrimitiveMappings() {
        // We generate a row with: 
        // 1. A double (DECIMAL/INTEGER)
        // 2. A String (VARCHAR)
        // 3. A byte[] (BINARY)
        // 4. A double[] (INTERVAL - returns 2 values)
        String sql = "SELECT 1.1 as d, 'Alice' as s, x'0102' as b, INTERVAL '1-2' YEAR TO MONTH as i";

        Object result = myexecutor.executeSql( sql );

        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        //String[] colNames = (String[]) arr[0]; // first entry is column names -> not really necessary here
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data.

        // --- Type 1: Double -> double[] ---
        assertEquals( "double", colTypes[0] );
        assertTrue( columns[0] instanceof double[], "Column 0 should be primitive double[]" );
        assertEquals( 1.1, ((double[]) columns[0])[0] );

        // --- Type 2: String -> String[] ---
        assertEquals( "String", colTypes[1] );
        assertTrue( columns[1] instanceof String[], "Column 1 should be String[]" );
        assertEquals( "Alice", ((String[]) columns[1])[0] );

        // --- Type 3: byte[] -> byte[][] ---
        assertEquals( "byte[]", colTypes[2] );
        assertTrue( columns[2] instanceof byte[][], "Column 2 should be byte[][] (array of BLOBs)" );
        assertArrayEquals( new byte[]{ 1, 2 }, ((byte[][]) columns[2])[0] );

        // --- Type 4: double[] -> double[][] ---
        assertEquals( "double[]", colTypes[3] );
        assertTrue( columns[3] instanceof double[][], "Column 3 should be double[][] (array of intervals)" );
        // Interval '1-2' is 14 months. Index 0 is months, index 1 is millis.
        assertEquals( 14.0, ((double[][]) columns[3])[0][0], 0.1 );
    }

    // ─────────────────────────────
    // Realistic integration tests (use unittest_namespace.unittest_table)
    // ─────────────────────────────


    @Test
    void testInsertSQLAndSelect() {
        // Insert id = 1 and name = Alice into the table.
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        Object result = myexecutor.executeSql( "SELECT id, name FROM unittest_namespace.unittest_table" );
        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        //String[] colNames = (String[]) arr[0]; // first entry is column names -> not really necessary here
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data.

        // test column names match
        String[] colNames = (String[]) arr[0];
        assertArrayEquals( new String[]{ "id", "name" }, colNames, "Column names must match" );

        // test the types match
        assertArrayEquals( new String[]{ "double", "String" }, colTypes );
        assertTrue( columns[0] instanceof double[] );
        assertTrue( columns[1] instanceof String[] );

        // test the columns contents match
        assertEquals( 1.0, ((double[]) columns[0])[0] );
        assertEquals( "Alice", ((String[]) columns[1])[0] );

        // rows number should be 1
        assertEquals( 1, ((double[]) columns[0]).length, "Should have one row in id column" );
        assertEquals( 1, ((String[]) columns[1]).length, "Should have one row in Name column" );
    }

    /*
    use shop;db.orders.drop();db.createCollection("orders");
    db.orders.insert({ customername: "Alice", product: "laptop", price: 19.99 });
    db.orders.insert({ customername: "Alice", product: "phone", price: 19.99 });
    db.orders.insert({ customername: "Bob", product: "mobile", price: 12.99 });
    */


    @Test
    void testCrossModelQuery_SQL_MQL() {
        myexecutor.executeMongo( "mongo", "shop", "db.createCollection(\"orders\")" );
        myexecutor.executeMongo( "mongo", "shop", "db.orders.insert({customername: \"Alice\",product: \"laptop\", price: 799.99})" );
        myexecutor.executeMongo( "mongo", "shop", "db.orders.insert({customername: \"Alice\",product: \"phone\", price: 299.99})" );
        myexecutor.executeMongo( "mongo", "shop", "db.orders.insert({ customername: \"Bob\", product: \"mobile\", price: 299.99 });" );
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        Object result = myexecutor.executeSql( "SELECT t.name, t.id, o.d FROM unittest_namespace.unittest_table t JOIN shop.orders o ON t.name = JSON_VALUE(o.d, 'lax $.customername') WHERE t.name = 'Alice'" );
        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        String[] colNames = (String[]) arr[0]; // first entry is column names -> not really necessary here
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data.

        // arr[2] is an Object[] containing: [ [Alice], [1], [{product: "laptop", ...}] ]

        System.out.println( "--- Query Results (First Row) ---" );

        String[] firstCol = (String[]) columns[0];
        double[] secondCol = (double[]) columns[1];
        String[] thirdCol = (String[]) columns[2];

        System.out.println( Arrays.toString( colNames ) );
        System.out.println( Arrays.toString( colTypes ) );
        System.out.println( Arrays.toString( firstCol ) );
        System.out.println( Arrays.toString( secondCol ) );
        System.out.println( Arrays.toString( thirdCol ) );

    }


    @Test
    void testInsertYieldsUpdateCount() {
        Object result = myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        Object[] arr = (Object[]) result; //gets the Object[]{ colNames, instantiatedColumnTypes, resultColumns }; created by handleResultSet
        String[] colNames = (String[]) arr[0]; // first entry is column names -> not really necessary here
        String[] colTypes = (String[]) arr[1]; // second entry is column types
        Object[] columns = (Object[]) arr[2]; // third entry is the actual data.

        assertEquals( "numberOfRowsAffected", colNames[0] );
        assertEquals( "double", colTypes[0] );
        assertArrayEquals( new double[]{ 1 }, (double[]) columns[0] );
    }


    @Test
    void testInsertAndSelectMultipleRows() {
        // Insert id = 1,2 and name = Alice, Bob into the table.
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Query the result from the table.
        Object result = myexecutor.executeSql( "SELECT id, name FROM unittest_namespace.unittest_table ORDER BY id" );

        // Check the contents of the query are correct.
        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        String[] colTypes = (String[]) arr[1];
        Object[] columns = (Object[]) arr[2];

        // Test the column names match.
        assertArrayEquals( new String[]{ "id", "name" }, colNames );

        // Test the array has indeed 2 rows for Alice and Bob
        assertEquals( 2, ((double[]) columns[0]).length );

        // Test that column 1 indeed holds doubles and column 2 indeed holds Strings
        assertTrue( columns[0] instanceof double[] );
        assertTrue( colTypes[0] == "double" );
        assertTrue( columns[1] instanceof String[] );
        assertTrue( colTypes[1] == "String" );

        // Test the contents of each row are correct.
        assertArrayEquals( new double[]{ 1, 2 }, (double[]) columns[0] );
        assertArrayEquals( new String[]{ "Alice", "Bob" }, (String[]) columns[1] );
    }


    @Test
    void testQueryWithSpaces() {
        // Insert Bob into table.
        // Insert id = 1,2 and name = Alice, Bob into the table.
        myexecutor.executeSql( "  INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        myexecutor.executeSql( " INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Query the result from the table.
        Object result = myexecutor.executeSql( "SELECT id, name FROM unittest_namespace.unittest_table ORDER BY id" );

        // Check the contents of the query are correct.
        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        // String[] colTypes = (String[]) arr[1]; not needed here.
        Object[] columns = (Object[]) arr[2];

        // Test the column names match.
        assertArrayEquals( new String[]{ "id", "name" }, colNames );

        // Test the array has indeed 2 rows for Alice and Bob
        assertEquals( 2, ((double[]) columns[0]).length );

        // Test the contents of each row are correct.
        assertArrayEquals( new double[]{ 1, 2 }, (double[]) columns[0] );
        assertArrayEquals( new String[]{ "Alice", "Bob" }, (String[]) columns[1] );
    }


    @Test
    void testDeleteFromTable() {

        // Insert Bob into table.
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Delete Bob from table.
        myexecutor.executeSql( "DELETE FROM unittest_namespace.unittest_table" );

        // Test that the query comes back null.
        Object result = myexecutor.executeSql( "SELECT * FROM unittest_namespace.unittest_table WHERE name = 'Bob'" );
        assertNull( result, "After DELETE the table should be empty" );
    }


    @Test
    void testBatchInsertEmployees() {

        // Insert the List of queries
        List<String> queries = Arrays.asList(
                "INSERT INTO unittest_namespace.batch_table VALUES (1, 'Alice', 'F', DATE '1990-01-15', 1001)",
                "INSERT INTO unittest_namespace.batch_table VALUES (2, 'Bob', 'M', DATE '1989-05-12', 1002)",
                "INSERT INTO unittest_namespace.batch_table VALUES (3, 'Jane', 'F', DATE '1992-07-23', 1003)",
                "INSERT INTO unittest_namespace.batch_table VALUES (4, 'Tim', 'M', DATE '1991-03-03', 1004)",
                "INSERT INTO unittest_namespace.batch_table VALUES (5, 'Alex', 'M', DATE '1994-11-11', 1005)",
                "INSERT INTO unittest_namespace.batch_table VALUES (6, 'Mason', 'M', DATE '1988-04-22', 1006)",
                "INSERT INTO unittest_namespace.batch_table VALUES (7, 'Rena', 'F', DATE '1995-06-17', 1007)",
                "INSERT INTO unittest_namespace.batch_table VALUES (8, 'Christopher', 'M', DATE '1987-08-09', 1008)",
                "INSERT INTO unittest_namespace.batch_table VALUES (9, 'Lexi', 'F', DATE '1996-09-30', 1009)",
                "INSERT INTO unittest_namespace.batch_table VALUES (10, 'Baen', 'M', DATE '1990-10-05', 1010)",
                "INSERT INTO unittest_namespace.batch_table VALUES (11, 'Ricardo', 'M', DATE '1986-12-12', 1011)",
                "INSERT INTO unittest_namespace.batch_table VALUES (12, 'Tim', 'M', DATE '1993-02-02', 1012)",
                "INSERT INTO unittest_namespace.batch_table VALUES (13, 'Beya', 'F', DATE '1994-05-25', 1013)"
        );

        // Do the batch execution using executeBatch(...)
        int[] counts = myexecutor.executeBatchSql( queries );

        // Test that the length of the counts vector is 13 (for 13 queries in the queries list).
        assertEquals( 13, counts.length, "Batch should return 13 results" );

        // Test the i-th entry in the counts vector is actually 1 (because the i-th query changed exactly 1 row)
        for ( Object c : counts ) {
            assertEquals( 1, c, "Each INSERT should affect exactly 1 row" );
        }

        // Test the result has the correct type
        Object result = myexecutor.executeSql( "SELECT COUNT(*) FROM unittest_namespace.batch_table" );
        Object[] arr = (Object[]) result;
        Object[] columns = (Object[]) arr[2];
        assertTrue( columns[0] instanceof double[] );

        // Test the rowcount is correct.
        double rowCount = ((double[]) columns[0])[0];
        assertEquals( 13.0, rowCount, "Table should contain 13 rows after batch insert" );
    }


    @Test
    void testBatchRollbackOnFailure() {

        // Prepare one correct and one ill posed SQL statement to query as batch later.
        List<String> queries = Arrays.asList(
                "INSERT INTO unittest_namespace.batch_table VALUES (1, 'Alice')",
                "Purposefully messed up query message to produce a failure" // PK violation → id missing
        );

        // Run the ill posed batch query and test an exception is thrown.
        assertThrows( RuntimeException.class, () -> {
            myexecutor.executeBatchSql( queries );
        } );

        // Query the whole table to make sure it is really empty.
        Object result = myexecutor.executeSql( "SELECt * FROM unittest_namespace.batch_table" );

        // Test the query comes back as null i.e. the executeBatch has indeed been rolled back and the table is unchanged
        assertNull( result );
    }


    @Test
    void testConnectionFailure() {
        assertThrows( RuntimeException.class, () -> {
            PolyphenyConnection badConn = new PolyphenyConnection( "localhost", 9999, "pa", "" );
            QueryExecutor badExec = new QueryExecutor( badConn );
            badExec.executeSql( "SELECT 1" );  // should fail to connect
        } );
    }


    @Test
    void testSyntaxError() {
        RuntimeException runtimeException = assertThrows( RuntimeException.class, () -> {
            myexecutor.executeSql( "SELEC WRONG FROM nowhere" );  // typo: SELEC
        } );
        assertTrue( runtimeException.getMessage().contains( "Syntax error" ) ||
                runtimeException.getMessage().contains( "execution failed" ),
                "Exception message should indicate syntax error" );
    }


    @Test
    void testCommitFailureRollback() {
        List<String> queries = Arrays.asList(
                "INSERT INTO unittest_namespace.batch_table VALUES (1, 'Alice', 'F', DATE '1990-01-15', 1001)",
                "Intentional nonsense to produce a failure" // PK violation → id missing
        );

        assertThrows( RuntimeException.class, () -> {
            myexecutor.executeBatchSql( queries );
        } );

        Object result = myexecutor.executeSql( "SELECT * FROM unittest_namespace.batch_table" );
        assertNull( result, "Batch should have rolled back and left the table empty" );
    }


    @Test
    // This test asserts that the column names aren't stored in the first row of the table for relational results.
    // Thought that this might maybe be how it's implemented for relational results in execute(...)
    void testRelationalResultFirstRowDirectly() throws Exception {
        // Insert a row we can recognize
        myexecutor.executeSql( "INSERT INTO unittest_namespace.unittest_table (id, name) VALUES (1, 'Alice')" );

        // Unwrap to PolyConnection and PolyStatement
        Connection jdbcConn = myconnection.getConnection();
        PolyConnection polyConn = jdbcConn.unwrap( PolyConnection.class );
        PolyStatement polyStmt = polyConn.createPolyStatement();

        // Run query directly through multimodel API
        Result result = polyStmt.execute( "unittest_namespace", "sql", "SELECT * FROM unittest_namespace.unittest_table" );

        assertEquals( Result.ResultType.RELATIONAL, result.getResultType() );

        RelationalResult rr = result.unwrap( RelationalResult.class );
        Iterator<PolyRow> it = rr.iterator();
        assertTrue( it.hasNext(), "Expected at least one row" );

        PolyRow firstRow = it.next();
        assertEquals( 2, firstRow.getColumnCount(), "Expected 2 columns (id, name)" );

        TypedValue idVal = firstRow.getValue( 0 );
        TypedValue nameVal = firstRow.getValue( 1 );

        assertEquals( 1, idVal.asInt() );
        assertEquals( "Alice", nameVal.asString() );
    }

}
