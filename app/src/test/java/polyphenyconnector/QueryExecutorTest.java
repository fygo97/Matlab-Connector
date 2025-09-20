package polyphenyconnector;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;

public class QueryExecutorTest {

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
            myexecutor.execute( "sql", "DROP TABLE IF EXISTS unittest_namespace.unittest_table" );
            myexecutor.execute( "sql", "DROP NAMESPACE IF EXISTS unittest_namespace" );
        } catch ( Exception ignored ) {
        }

        // Creates the NAMESPACE <unittest_namespace> and TABLE <unittest_table>.
        myexecutor.execute( "sql", "CREATE NAMESPACE unittest_namespace" );
        myexecutor.execute( "sql", "CREATE TABLE unittest_namespace.unittest_table (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY(id))" );

        // 2. Setup tables for executeBatch()
        // Delete any tables that might still exist as described before.
        try {
            myexecutor.execute( "sql", "DROP TABLE IF EXISTS unittest_namespace.batch_table" );
        } catch ( Exception ignored ) {
        }
        myexecutor.execute( "sql", "CREATE TABLE unittest_namespace.batch_table (" + "emp_id INT NOT NULL, " + "name VARCHAR(100), " + "gender VARCHAR(10), " + "birthday DATE, " + "employee_id INT, " + "PRIMARY KEY(emp_id))" );

    }


    @AfterAll
    static void tearDownNamespaceAndTable() {
        // Cleans up the TABLE and NAMESPACE we created so we leave no trace after the tests.
        myexecutor.execute( "sql", "DROP TABLE IF EXISTS unittest_namespace.unittest_table" );
        myexecutor.execute( "sql", "DROP TABLE IF EXISTS unittest_namespace.batch_table" );
        myexecutor.execute( "sql", "DROP NAMESPACE IF EXISTS unittest_namespace" );
        myconnection.close();
    }


    @BeforeEach
    void clearTable() {
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.unittest_table" );
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.batch_table" );
    }


    @AfterEach
    void clearTableAfter() {
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.unittest_table" );
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.batch_table" );
    }

    // ─────────────────────────────
    // Isolated branch tests (no table needed)
    // ─────────────────────────────


    @Test
    void testScalarLiteral() {
        Object result = myexecutor.execute( "sql", "SELECT 42 AS answer" );
        assertTrue( result instanceof Integer, "Expected an integer scalar" );
        assertEquals( 42, result );
    }


    @Test
    void testEmptyLiteral() {
        Object result = myexecutor.execute( "sql", "SELECT * FROM (SELECT 1) t WHERE 1=0" );
        assertNull( result, "Query with no rows should return null" );
    }


    @Test
    void testTableLiteral() {
        Object result = myexecutor.execute( "sql", "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4" );
        assertTrue( result instanceof Object[], "Expected tabular result" );

        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        Object[][] data = (Object[][]) arr[1];

        assertArrayEquals( new String[]{ "a", "b" }, colNames, "Column names must match" );
        assertEquals( 2, data.length, "Should have 2 rows" );
        assertArrayEquals( new Object[]{ 1, 2 }, data[0] );
        assertArrayEquals( new Object[]{ 3, 4 }, data[1] );
    }

    // ─────────────────────────────
    // Realistic integration tests (use unittest_namespace.unittest_table)
    // ─────────────────────────────


    @Test
    void testInsert() {
        // Insert id = 1 and name = Alice into the table.
        Object result = myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        assertTrue( result instanceof Integer, "Expected an integer." );
        assertEquals( result, 1, "result should equal 1." );
    }


    @Test
    void testInsertAndSelect() {
        // Insert id = 1 and name = Alice into the table.
        myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );

        // Query the result from the table.
        Object result = myexecutor.execute( "sql", "SELECT id, name FROM unittest_namespace.unittest_table" );

        // Test that the result comes back as array.
        System.out.println( "Result is: " + result );
        assertTrue( result instanceof Object[], "Expected tabular result" );

        // Test that the contents of the query are correct.
        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        Object[][] data = (Object[][]) arr[1];
        assertArrayEquals( new String[]{ "id", "name" }, colNames, "Column names must match" );
        assertEquals( 1, data.length, "Should have one row" );
        assertArrayEquals( new Object[]{ 1, "Alice" }, data[0], "Row must match inserted values" );

    }


    @Test
    void testScalarFromTable() {
        myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Carol')" );
        Object result = myexecutor.execute( "sql", "SELECT id FROM unittest_namespace.unittest_table WHERE name = 'Carol'" );
        assertTrue( result instanceof Integer, "Expected scalar integer result" );
        assertEquals( 2, result );
    }


    @Test
    void testInsertAndSelectMultipleRows() {
        // Insert id = 1,2 and name = Alice, Bob into the table.
        myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Query the result from the table.
        Object result = myexecutor.execute( "sql", "SELECT id, name FROM unittest_namespace.unittest_table ORDER BY id" );

        // Check the contents of the query are correct.
        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        Object[][] data = (Object[][]) arr[1];

        // Test the column names match.
        assertArrayEquals( new String[]{ "id", "name" }, colNames );

        // Test the array has indeed length 2 (2 rows for Alice and Bob)
        assertEquals( 2, data.length );

        // Test the contents of each row are correct.
        assertArrayEquals( new Object[]{ 1, "Alice" }, data[0] );
        assertArrayEquals( new Object[]{ 2, "Bob" }, data[1] );
    }


    @Test
    void testQueryWithSpaces() {
        // Insert Bob into table.
        // Insert id = 1,2 and name = Alice, Bob into the table.
        myexecutor.execute( "sql", "  INSERT INTO unittest_namespace.unittest_table VALUES (1, 'Alice')" );
        myexecutor.execute( "sql", " INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Query the result from the table.
        Object result = myexecutor.execute( "sql", "SELECT id, name FROM unittest_namespace.unittest_table ORDER BY id" );

        // Check the contents of the query are correct.
        Object[] arr = (Object[]) result;
        String[] colNames = (String[]) arr[0];
        Object[][] data = (Object[][]) arr[1];

        // Test the column names match.
        assertArrayEquals( new String[]{ "id", "name" }, colNames );

        // Test the array has indeed length 2 (2 rows for Alice and Bob)
        assertEquals( 2, data.length );

        // Test the contents of each row are correct.
        assertArrayEquals( new Object[]{ 1, "Alice" }, data[0] );
        assertArrayEquals( new Object[]{ 2, "Bob" }, data[1] );
    }


    @Test
    void testDeleteFromTable() {

        // Insert Bob into table.
        myexecutor.execute( "sql", "INSERT INTO unittest_namespace.unittest_table VALUES (2, 'Bob')" );

        // Delete Bob from table.
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.unittest_table" );

        // Test that the query comes back null.
        Object result = myexecutor.execute( "sql", "SELECT * FROM unittest_namespace.unittest_table WHERE name = 'Bob'" );
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
        int[] counts = myexecutor.executeBatch( "sql", queries );

        // Test that the lenghth of the counts vector is 13 (for 13 queries in the queries liest).
        assertEquals( 13, counts.length, "Batch should return 13 results" );

        // Test the i-th entry in the counts vector is actually 1 (because the i-th query changed exactly 1 row)
        for ( int c : counts ) {
            assertEquals( 1, c, "Each INSERT should affect exactly 1 row" );
        }

        // Test the result has the correct type
        Object result = myexecutor.execute( "sql", "SELECT COUNT(*) FROM unittest_namespace.batch_table" );
        assertTrue( result instanceof Long || result instanceof Integer );

        // Test the rowcount is correct.
        int rowCount = ((Number) result).intValue();
        assertEquals( 13, rowCount, "Table should contain 13 rows after batch insert" );
    }


    @Test
    void testBatchRollbackOnFailure() {

        // Prepare one correct and one ill posed SQL statement to query as batch later.
        List<String> queries = Arrays.asList(
                "INSERT INTO unittest_namespace.batch_table VALUES (1, 'Alice')",
                "Purposefully messed up query message to produce a failure" // duplicate PK
        );

        // Run the ill posed batch query and test an exception is thrown.
        assertThrows( RuntimeException.class, () -> {
            myexecutor.executeBatch( "sql", queries );
        } );

        // Query the whole table to make sure it is really empty.
        Object result = myexecutor.execute( "sql", "SELECt * FROM unittest_namespace.batch_table" );

        // Test the query comes back as null i.e. the executeBatch has indeed been rolled back and the table is unchanged
        assertNull( result );
    }


    @Test
    void testConnectionFailure() {
        assertThrows( RuntimeException.class, () -> {
            PolyphenyConnection badConn = new PolyphenyConnection( "localhost", 9999, "pa", "" );
            QueryExecutor badExec = new QueryExecutor( badConn );
            badExec.execute( "sql", "SELECT 1" );  // should fail to connect
        } );
    }


    @Test
    void testSyntaxError() {
        RuntimeException ex = assertThrows( RuntimeException.class, () -> {
            myexecutor.execute( "sql", "SELEC WRONG FROM nowhere" );  // typo: SELEC
        } );
        assertTrue( ex.getMessage().contains( "Syntax error" ) ||
                ex.getMessage().contains( "execution failed" ),
                "Exception message should indicate syntax error" );
    }


    @Test
    void testCommitFailureRollback() {
        List<String> queries = Arrays.asList(
                "INSERT INTO unittest_namespace.batch_table VALUES (1, 'Alice', 'F', DATE '1990-01-15', 1001)",
                "Intentional nonsense to produce a failure" // PK violation
        );

        assertThrows( RuntimeException.class, () -> {
            myexecutor.executeBatch( "sql", queries );
        } );

        Object result = myexecutor.execute( "sql", "SELECT * FROM unittest_namespace.batch_table" );
        assertNull( result, "Batch should have rolled back and left the table empty" );
    }

}
