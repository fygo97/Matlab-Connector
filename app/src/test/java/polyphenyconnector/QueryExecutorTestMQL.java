package polyphenyconnector;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

public class QueryExecutorTestMQL {

    private static PolyphenyConnection myconnection;
    private static QueryExecutor myexecutor;


    @BeforeAll
    static void setUpNamespaceAndCollection() throws Exception {
        PolyphenyConnectionTestHelper.waitForPolypheny();
        Thread.sleep( 4000 );
        myconnection = new PolyphenyConnection( "localhost", 20590, "pa", "" );
        myexecutor = new QueryExecutor( myconnection );

        try {
            myexecutor.execute( "mongo", "mongotest", "db.createCollection(\"unittest_collection\")" );
        } catch ( Exception ignoredException ) {
        }
    }


    @AfterAll
    static void tearDownNamespaceAndCollection() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.drop()" );
        } catch ( Exception ignored ) {
        }
        myconnection.close();
    }


    @BeforeEach
    void clearCollection() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.deleteMany({})" );
        } catch ( Exception ignored ) {
        }
    }


    @AfterEach
    void clearCollectionAfter() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.deleteMany({})" );
        } catch ( Exception ignored ) {
        }
    }


    @Test
    void testInsertandDrop() {
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertTrue( result instanceof List, "Expected a List<String>" );
        List<String> docs = (List<String>) result;
        assertEquals( 0, docs.size() );

        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":14})" );
        result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\":14})" );
        docs = (List<String>) result;
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"age\":14" ) );

        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.deleteMany({})" );
        result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\": {\"$eq\": 14}})" );
        docs = (List<String>) result;
        assertEquals( 0, docs.size() );
    }


    @Test
    void testInsertManyAndNestedDocument() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":14})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":20})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":24})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":30, \"adress\": {\"Country\": \"Switzerland\", \"Code\": 4051}})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\": {$gt:29}})" );

        assertTrue( result instanceof List, "expected a List<String> for DocumentResult" );
        List<String> docs = (List<String>) result;
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"age\":30" ) );
    }


    @Test
    void testBooleanField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"flag\":true})" );
        List<String> docs = (List<String>) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertTrue( docs.get( 0 ).contains( "\"flag\":true" ) );
    }


    @Test
    void testIntegerAgeField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":42})" );
        List<String> docs = (List<String>) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"age\":42" ) );
    }


    @Test
    void testStringField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"name\":\"Alice\"})" );
        List<String> docs = (List<String>) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"name\":\"Alice\"" ) );
    }


    @Test
    void testLongField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"big\":1111111111111111111})" );
        Object r = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        List<String> docs = (List<String>) r;
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"big\":1111111111111111111" ) );
    }


    @Test
    void testDoubleField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"pi\":3.14159})" );
        Object r = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        List<String> docs = (List<String>) r;
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"pi\":3.14159" ) );
    }


    @Test
    void testCountDocumentsReturnsStringArray() {
        myexecutor.execute( "mongo", "mongotest",
                "db.unittest_collection.insertOne({\"name\":\"Bob\"})" );

        Object result = myexecutor.execute( "mongo", "mongotest",
                "db.unittest_collection.countDocuments({})" );

        assertTrue( result instanceof List, "result should be a List<String>" );
        List<String> docs = (List<String>) result;
        assertEquals( 1, docs.size() );
        assertEquals( "{\"count\":1}", docs.get( 0 ) );
    }


    @Test
    void testListElementClasses() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"mixed\":[{\"bar\":2},1,\"foo\"]})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        List<String> docs = (List<String>) result;
        System.out.println( docs.get( 0 ) );
    }


    @Test
    void testArrayField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"scores\":[1,2,3]})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        List<String> docs = (List<String>) result;
        assertEquals( 1, docs.size() );
        assertTrue( docs.get( 0 ).contains( "\"scores\":[1,2,3]" ) );
    }


    @Test
    void testFindOnEmptyCollectionReturnsEmptyArray() {
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertTrue( result instanceof List, "expected List<String> even for empty cursor" );
        List<String> docs = (List<String>) result;
        assertEquals( 0, docs.size() );
    }


    @Test
    void testInsertManyAndFindMultiple() {
        myexecutor.execute( "mongo", "mongotest",
                "db.unittest_collection.insertOne({\"id\":10,\"name\":\"A\"})" );
        myexecutor.execute( "mongo", "mongotest",
                "db.unittest_collection.insertOne({\"id\":11,\"name\":\"B\"})" );

        Object out = myexecutor.execute( "mongo", "mongotest",
                "db.unittest_collection.find({})" );

        assertTrue( out instanceof List );
        List<String> docs = (List<String>) out;
        assertEquals( 2, docs.size() );
    }


    @Test
    void testBatchInsertAndFind() {
        List<String> queries = new ArrayList<>();
        queries.add( "db.unittest_collection.insertOne({\"name\":\"Alice\",\"age\":25})" );
        queries.add( "db.unittest_collection.insertOne({\"name\":\"Bob\",\"age\":30})" );

        Object out = myexecutor.executeBatchMongo( "mongotest", queries );

        assertTrue( out instanceof List, "Expected a List of results" );
        @SuppressWarnings("unchecked") List<List<String>> results = (List<List<String>>) out;

        assertEquals( 2, results.size(), "Expected two results (one per insert)" );
        assertTrue( results.get( 0 ) instanceof List, "First insert result should be a List<String>" );
        assertTrue( results.get( 1 ) instanceof List, "Second insert result should be a List<String>" );
        assertEquals( 1, results.get( 0 ).size(), "Each insert should yield a singleton list" );
        assertEquals( 1, results.get( 1 ).size(), "Each insert should yield a singleton list" );
    }


    @Test
    void testBatchMixedOps() {
        List<String> queries = new ArrayList<>();
        queries.add( "db.unittest_collection.insertOne({\"name\":\"Charlie\",\"active\":true})" );
        queries.add( "db.unittest_collection.countDocuments({})" );

        Object out = myexecutor.executeBatchMongo( "mongotest", queries );

        assertTrue( out instanceof List, "Expected a List of results" );
        @SuppressWarnings("unchecked") List<List<String>> results = (List<List<String>>) out;

        assertEquals( 2, results.size(), "Expected two results" );
        assertTrue( results.get( 0 ) instanceof List, "First result should be a List<String>" );
        assertTrue( results.get( 1 ) instanceof List, "Second result should be a List<String>" );

        // Insert → singleton list with ack/JSON
        assertEquals( 1, results.get( 0 ).size(), "Insert should yield a singleton list" );

        // Count → singleton list with a number string
        assertEquals( 1, results.get( 1 ).size(), "Count should yield a singleton list" );
        assertTrue( results.get( 1 ).get( 0 ).contains( "1" ), "Count result should include '1'" );
    }

}
