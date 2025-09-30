package polyphenyconnector;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class QueryExecutorTestMQL {

    private static PolyphenyConnection myconnection;
    private static QueryExecutor myexecutor;


    @BeforeAll
    static void setUpNamespaceAndCollection() throws Exception {
        PolyphenyConnectionTestHelper.waitForPolypheny();
        Thread.sleep( 4000 );
        myconnection = new PolyphenyConnection( "localhost", 20590, "pa", "" );
        myexecutor = new QueryExecutor( myconnection );

        // Trigger implicit creation by inserting a dummy doc
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
        // Query to make sure the unittest_collection is actually empty
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        String[] docs = (String[]) result;
        assertEquals( 0, docs.length );

        // Insert entry into database
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":14})" );
        result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\":14})" );
        docs = (String[]) result;
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"age\":14" ) );

        // Drop entry
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.deleteMany({})" );
        result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\": {\"$eq\": 14}})" );
        docs = (String[]) result;
        assertEquals( 0, docs.length );
    }


    @Test
    void testInsertManyAndNestedDocument() {
        // insert a document (namespace passed separately; query contains only collection.operation)

        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":14})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":20})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":24})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":30, \"adress\": {\"Country\": \"Switzerland\", \"Code\": 4051}})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\": {$gt:29}})" );

        assertTrue( result instanceof String[], "expected a String[] for DocumentResult" );
        String[] docs = (String[]) result; // you can also do this quicker by String[] docs = (String[]) myexecuter.execute(...)
        assertEquals( 1, docs.length );
        //System.out.println( docs[0] );
        assertTrue( docs[0].contains( "\"age\":30" ) );
    }


    @Test
    void testBooleanField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"flag\":true})" );
        String[] docs = (String[]) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertTrue( docs[0].contains( "\"flag\":true" ) );
    }


    @Test
    void testIntegerAgeField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":42})" );
        String[] docs = (String[]) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"age\":42" ) );
    }


    @Test
    void testStringField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"name\":\"Alice\"})" );
        String[] docs = (String[]) myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"name\":\"Alice\"" ) );
    }


    @Test
    void testLongField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"big\":1111111111111111111})" );
        Object r = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        String[] docs = (String[]) r;
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"big\":1111111111111111111" ) );
    }


    @Test
    void testDoubleField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"pi\":3.14159})" );
        Object r = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        String[] docs = (String[]) r;
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"pi\":3.14159" ) );
    }


    @Disabled
    @Test
    void testCountDocumentsReturnsScalar() {
        // ensure empty, insert one then count
        myexecutor.execute( "mongo", "unittest_collection",
                "unittest_namespace.unittest_coll.insertOne({\"name\":\"Bob\"})" );

        Object result = myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.countDocuments({})" );

        assertTrue( result instanceof Number, "Expected numeric scalar from countDocuments" );
        assertEquals( 1, ((Number) result).intValue() );
    }


    @Test
    void testListElementClasses() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"mixed\":[{\"bar\":2},1,\"foo\"]})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        String[] docs = (String[]) result;
        System.out.println( docs[0] );
    }


    @Disabled
    @Test
    void testArrayField() {
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"scores\":[1,2,3]})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({})" );
        String[] docs = (String[]) result;
        assertEquals( 1, docs.length );
        assertTrue( docs[0].contains( "\"scores\":[1,2,3]" ) );
    }


    @Disabled
    @Test
    void testFindOnEmptyCollectionReturnsEmptyArray() {
        // ensure empty
        myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.deleteMany({})" );

        Object out = myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.find({})" );

        assertTrue( out instanceof String[], "expected String[] even for empty cursor" );
        String[] docs = (String[]) out;
        assertEquals( 0, docs.length );
    }


    @Disabled
    @Test
    void testInsertManyAndFindMultiple() {
        myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.insertOne({\"_id\":10,\"name\":\"A\"})" );
        myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.insertOne({\"_id\":11,\"name\":\"B\"})" );

        Object out = myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.find({})" );

        assertTrue( out instanceof String[] );
        String[] docs = (String[]) out;
        assertEquals( 2, docs.length );
    }

}
