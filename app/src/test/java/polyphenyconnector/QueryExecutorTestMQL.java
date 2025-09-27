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
        } catch ( Exception ignoredException ) {

        }
    }

    /* 
    @AfterAll
    static void tearDownNamespaceAndCollection() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.drop()" );
        } catch ( Exception ignored ) {
        }
        myconnection.close();
    }
    */


    @BeforeEach
    void clearCollection() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.drop()" );
            myexecutor.execute( "mongo", "mongotest", "db.createCollection(\"unittest_collection\")" );
        } catch ( Exception ignored ) {
        }
    }

    /* 
    @AfterEach
    void clearCollectionAfter() {
        try {
            myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.drop()" );
        } catch ( Exception ignored ) {
        }
    }
    */


    @Test
    void testInsertAndFindReturnsDocumentArray() {
        // insert a document (namespace passed separately; query contains only collection.operation)

        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":14})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":20})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":24})" );
        myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.insertOne({\"age\":30, \"adress\": {\"Country\": \"Switzerland\", \"Code\": 4051}})" );
        Object result = myexecutor.execute( "mongo", "mongotest", "db.unittest_collection.find({\"age\": {$gt:29}})" );

        assertTrue( result instanceof String[], "expected a String[] for DocumentResult" );
        String[] docs = (String[]) result;
        assertEquals( 1, docs.length );
        //System.out.println( docs[0] );
        assertTrue( docs[0].contains( "\"age\":14" ) );
    }


    @Disabled
    @Test
    void testCountDocumentsReturnsScalar() {
        // ensure empty, insert one then count
        myexecutor.execute( "mongo", "unittest_collection",
                "unittest_namespace.unittest_coll.insertOne({\"_id\":2,\"name\":\"Bob\"})" );

        Object out = myexecutor.execute( "mongo", "unittest_namespace",
                "unittest_namespace.unittest_coll.countDocuments({})" );

        assertTrue( out instanceof Number, "Expected numeric scalar from countDocuments" );
        assertEquals( 1, ((Number) out).intValue() );
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
