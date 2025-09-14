package polyphenyconnector;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class QueryExecutorTest {

    private static PolyphenyConnection myconnection;
    private static QueryExecutor myexecutor;


    @BeforeAll
    static void setUpNamespaceAndTable() throws Exception {

        // Wait for Polypheny to be available and connect to localhost. We do this because we run all the JUnit tests on our local machine. 
        PolyphenyConnectionTestHelper.waitForPolypheny();
        myconnection = new PolyphenyConnection( "localhost", 20590, "pa", "" );
        myexecutor = new QueryExecutor( myconnection );

        // Delete any TABLE called <unittest_table> and any NAMESPACE <unittest_namespace> if it exists. This is important so we can insert it
        // cleanly, in case tests break mid run the cleanup "@Afterall" might not have been executed properly.
        try {
            myexecutor.execute( "sql", "DROP TABLE unittest_namespace.unittest_table" );
        } catch ( Exception ignored ) {
        }
        try {
            myexecutor.execute( "sql", "DROP NAMESPACE unittest_namespace" );
        } catch ( Exception ignored ) {
        }
        // Creates the NAMESPACE <unittest_namespace> and TABLE <unittest_table>.
        myexecutor.execute( "sql", "CREATE NAMESPACE unittest_namespace" );
        myexecutor.execute( "sql", "CREATE TABLE unittest_namespace.unittest_table (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY(id))" );
    }


    @AfterAll
    static void tearDownNamespaceAndTable() {
        // Cleans up the TABLE and NAMESPACE we created so we leave no trace after the tests.
        myexecutor.execute( "sql", "DROP TABLE IF EXISTS unittest_namespace.unittest_table" );
        myexecutor.execute( "sql", "DROP NAMESPACE IF EXISTS unittest_namespace" );
        myconnection.close();
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


    @BeforeEach
    void clearTable() {
        myexecutor.execute( "sql", "DELETE FROM unittest_namespace.unittest_table" );
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

}
