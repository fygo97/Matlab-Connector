package polyphenyconnector;

import java.sql.*;
// Add this
import java.util.*;

import org.polypheny.jdbc.types.*;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PolyphenyResultSet;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.dependency.prism.ProtoValue;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.dependency.prism.ProtoDocument;
import org.polypheny.jdbc.multimodel.Result;

import java.lang.reflect.Field;       // For accessing private variables
import java.lang.reflect.Method;      // (Optional) if you need to call private methods

/*
 * Internal Driver Logic:
 * 1. A ResultSet contains a Frame.
 * 2. A Frame contains a List<List<ProtoValue>> (Rows and Columns).
 * 3. When you call resultSet.getObject(i), the driver:
 * - Grabs the raw ProtoValue for that cell.
 * - Wraps it in a new instance of TypedValue.
 * - Hands that TypedValue to us.
 */

public class QueryExecutor {

    private PolyphenyConnection polyconnection;

    // LOCKPICKING The Field is the address or the coordinate of where the data of the "ProtoValue serialized" existing in the TypedValue class actually lives. This address is fixed forever as long as the JAR doesn't change. We need this variable to store the different ProtoValues from the TypedValue class later.
    private static final Field PROTO_FIELD;
    // LOCKPICKING Similarly we also need to bypass the driver to access PolyphenyResultSet.accessValue() which returns the TypedValue form of a certain column of a result set. 
    private static final Method ACCESS_VALUE_METHOD;

    // LOCKPICKING This runs ONCE to find the coordinate of the PROTO_FIELD and reflect the function.
    static {
        try {
            // Field Heist 
            PROTO_FIELD = TypedValue.class.getDeclaredField( "serialized" );
            PROTO_FIELD.setAccessible( true );

            // LOCKINGPICKING The function accessValue(...) from the PolyphenyResultSet class is private, so we must using reflection to get around the Driver to access the TypedValue
            ACCESS_VALUE_METHOD = PolyphenyResultSet.class.getDeclaredMethod( "accessValue", int.class );
            ACCESS_VALUE_METHOD.setAccessible( true );
        } catch ( Exception e ) {
            throw new RuntimeException( "Lockpick failed. Check driver version.", e );
        }
    }


    /**
     * LOCKPICKING This function is used for getting the TypedValue directly out of the PolyphenyResultSet. Calling the private accessValue(i)
     * via reflection on the PolyphenyResultSet. That way we avoid the massive overhead for large nested documents. The driver naturally wraps
     * such a nested document into TypedValue which will then resolve to PolyDocument which again will resolve to many (Key, TypedValue) pairs.
     * Since neither TypedValue nor PolyDocument offer any functionality to cast this Object to a String we circumvent this because both of
     * those objects are essentially dead weight and parse the original ProtoValue to avoid the overhead of having to unwrap all those
     * TypedValue objects into whatever the Driver casts them to.
     */
    private TypedValue getTypedValue( PolyphenyResultSet polyphenyResultSet, int columnIndex ) {
        try {
            return (TypedValue) ACCESS_VALUE_METHOD.invoke( polyphenyResultSet, columnIndex );
        } catch ( Exception e ) {
            // If the lockpick fails, we return null so the loop can handle the error
            return null;
        }
    }


    /**
     * LOCKPICKING This function is used to get the raw ProtoValue from a TypedValue object. This runs for EVERY row to save the object in the
     * the coordinate PROTO_FIELD and thus we aim to bypass the driver which forbids us to see ProtoValue on a specific instance
     * 
     * @param tv
     * @return
     */
    private ProtoValue getRawProto( TypedValue tv ) {
        try {
            return (ProtoValue) PROTO_FIELD.get( tv );
        } catch ( Exception e ) {
            return null;
        }
    }


    /**
     * @Description
     * - Constructor
     * 
     * @param polyconnection: PolyphenyConnection object that holds the connection
     * details to the Database. It's used to execute queries
     **/
    public QueryExecutor( PolyphenyConnection polyconnection ) {
        this.polyconnection = polyconnection;
    }


    // TODO: switch from createStatement to preparedStatement
    // TODO: instead of going through the PolyResultSet it might be sensible to go through the 
    // Frame -> RelationalFrame (=List<Row>) ->  Row (=List<ProtoValue>) -> ProtoValue? here is where the Frame comes from:
    // The Frame is the raw response from the gRPC interface. In your decompiled code for DocumentResult, you can see exactly where it comes 
    // from: 
    //          The fetchMore() method calls this.getPrismInterfaceClient().fetchResult(id, timeout, fetchSize)
    //          That fetchResult (or execute) call returns the Frame object directly.
    /**
     * @Description
     * Executes queries
     * @param <T> The Return type is needed for the generics of the function
     * @param query The query String. Must be SQL for this function even if we query Mongo or Cypher with it
     * @return The result of the query. Can be an updateCount or a ResultSet (if we did a SELECT query for example).
     * @throws Exception
     */
    public <T> Object executeSql( String query ) {
        try {
            polyconnection.openIfNeeded();
            Connection connection = polyconnection.getConnection();
            if ( connection == null || connection.isClosed() ) {
                throw new RuntimeException( "Database connection is closed or null." );
            }

            try ( Statement statement = connection.createStatement() ) {

                // returns true if result of query is a ResultSet and false if its an update count or no ResultSet (see execute() function doc):
                // https://docs.polypheny.com/en/latest/drivers/jdbc/relational/statement#execute
                boolean isResultSet = statement.execute( query );

                if ( isResultSet ) {
                    // --- CASE 1: SELECt etc. (Returns rows in table format) ---
                    try ( ResultSet resultSet = statement.getResultSet() ) {
                        return handleResultSet( (PolyphenyResultSet) resultSet ); // this handles the ResultSet casting to Java types 

                    }
                } else {
                    //TODO: switch to a long here or convert it into String maybe.
                    // --- CASE 2: INSERT / UPDATE / DELETE (No rows returned) ---
                    // We return a Long so MATLAB can easily handle it as a number
                    double count = statement.getLargeUpdateCount();
                    String[] colNames = new String[]{ "numberOfRowsAffected" };
                    String[] instantiatedColumnTypes = new String[]{ "double" };

                    // We wrap the single count in an array to match the "column-based" format
                    Object[] resultColumns = new Object[]{ new double[]{ count } };
                    // Return the exact same 3-element package
                    return new Object[]{ colNames, instantiatedColumnTypes, resultColumns };

                } /*else {
                    throw new RuntimeException( "Internal error: Driver returned something that is neither ResultSet nor UpdateCount." );
                  }
                  */
            }
        } catch ( SQLException e ) {
            throw translateException( e );
        } catch ( Exception e ) {
            throw new RuntimeException( "Internal Connector Error: " + e.getMessage(), e );
        }
    }


    //TODO: parametrize those statements
    public Object executeMongo( String language, String namespace, String query ) {
        try {
            polyconnection.openIfNeeded();

            // 1. Get the raw client directly from the unwrapped PolyConnection
            PolyConnection polyConnection = polyconnection.getConnection().unwrap( PolyConnection.class );
            PrismInterfaceClient client = polyConnection.getPrismInterfaceClient();

            // 2. Prepare the callback queue just like PolyStatement does internally
            // Note: CallbackQueue and Response are part of the org.polypheny.jdbc packages
            org.polypheny.jdbc.utils.CallbackQueue<org.polypheny.jdbc.dependency.prism.StatementResponse> callback = new org.polypheny.jdbc.utils.CallbackQueue<>( org.polypheny.jdbc.dependency.prism.Response::getStatementResponse );

            int timeout = polyConnection.getNetworkTimeout();

            // 3. Execute the statement - this sends the gRPC request
            client.executeUnparameterizedStatement( namespace, language, query, callback, timeout );

            // 4. Retrieve the response directly from the queue
            org.polypheny.jdbc.dependency.prism.StatementResponse response;
            do {
                response = (org.polypheny.jdbc.dependency.prism.StatementResponse) callback.takeNext();
                // We ignore the statementId management here for simplicity as we aren't using prepared statements
            } while ( !response.hasResult() );

            // 5. Await completion to ensure the network stream is finished
            callback.awaitCompletion();

            org.polypheny.jdbc.dependency.prism.StatementResult result = response.getResult();

            // 6. Check for the raw Protobuf data: the Frame
            if ( result.hasFrame() ) {
                org.polypheny.jdbc.dependency.prism.Frame rawFrame = result.getFrame();

                if ( rawFrame.getResultCase() == org.polypheny.jdbc.dependency.prism.Frame.ResultCase.DOCUMENT_FRAME ) {
                    // Advantage: No DocumentResult or PolyDocument objects were ever created.
                    List<ProtoDocument> rawDocs = rawFrame.getDocumentFrame().getDocumentsList();
                    return protoDocumentListToJava( rawDocs );
                }

                // Handle Relational Frame if necessary (optional fallback)
                throw new UnsupportedOperationException( "Relational results in Mongo execution not supported by this bypass." );
            } else {
                //TODO: switch to a long here or convert it into String maybe.
                // CASE: Scalar Result (e.g., an update count)
                return (double) result.getScalar();
            }

        } catch ( SQLException e ) {
            throw translateException( e );
        } catch ( Exception e ) {
            throw new RuntimeException( "Direct Mongo execution failed: " + e.getMessage(), e );
        }
    }
    /*
    // check that this function works.
    public Object executeMongo( String language, String namespace, String query ) {
        try {
            polyconnection.openIfNeeded();
            Connection connection = polyconnection.getConnection();
            PolyConnection polyConnection = connection.unwrap( PolyConnection.class );
            PolyStatement polyStatement = polyConnection.createPolyStatement();
    
            // 1. Execute and get the Result base object
            Result result = polyStatement.execute( namespace, language, query );
    
            switch ( result.getResultType() ) {
                case DOCUMENT:
                    // --- THE HEIST STARTS HERE ---
                    // 2. Lockpick the private 'frame' field from the Result base class to iterate the frames directly instead of walking the slow driver stuff.
                    Field frameField = org.polypheny.jdbc.multimodel.Result.class.getDeclaredField( "frame" );
                    frameField.setAccessible( true );
                    org.polypheny.jdbc.dependency.prism.Frame rawFrame = (org.polypheny.jdbc.dependency.prism.Frame) frameField.get( result );
    
                    // 3. Verify it's actually a document frame
                    if ( rawFrame.getResultCase() == org.polypheny.jdbc.dependency.prism.Frame.ResultCase.DOCUMENT_FRAME ) {
                        // 4. Get the raw List<ProtoDocument> - NO PolyDocuments or HashMaps created yet!
                        List<ProtoDocument> rawDocs = rawFrame.getDocumentFrame().getDocumentsList();
    
                        // 5. Walk the raw Protobufs directly to JSON for MATLAB
                        return RawDocumentsToMatlab( rawDocs );
                    }
    
                case SCALAR:
                    ScalarResult scalarResult = result.unwrap( ScalarResult.class );
                    return (double) scalarResult.getScalar();
    
                default:
                    throw new UnsupportedOperationException( "Unhandled result type: " + result.getResultType() );
            }
        } catch ( SQLException e ) {
            throw translateException( e );
        } catch ( Exception e ) {
            throw new RuntimeException( "Mongo execution failed: " + e.getMessage(), e );
        }
    }
    */


    //TODO check that this function works
    private List<String> protoDocumentListToJava( List<ProtoDocument> rawDocs ) throws Exception {
        List<String> jsonDocuments = new ArrayList<>( rawDocs.size() );
        StringBuilder sb = new StringBuilder( 1024 * 64 ); // Reuse buffer

        for ( ProtoDocument doc : rawDocs ) {
            sb.setLength( 0 );
            // Call your existing recursive walker directly on the raw ProtoDocument
            protoDocumentToJson( doc, sb );
            jsonDocuments.add( sb.toString() );
        }
        return jsonDocuments;
    }


    /**
     * Helper function that handles the ResultSet casting to Java types that align well with what we need to cast types to Matlab in a
     * performant way. Had we left this function in the .execute(query) as is, the indentation would have made it hard to read.
     * 
     * @param <T>
     * @param resultSet The resultSet returned by the driver.
     * @return The finished tabled result with all Java types as entries. It is now ready to be cast to Matlab
     * @throws Exception Will be treated by execute(query)
     */
    @SuppressWarnings("unchecked")
    public <T> Object handleResultSet( PolyphenyResultSet resultSet ) throws Exception {
        ResultSetMetaData meta = resultSet.getMetaData();
        int colCount = meta.getColumnCount();

        // ─────────────────────────────
        // Case 1: Empty Result
        // ─────────────────────────────
        if ( !resultSet.next() ) {
            //System.out.println( "Empty result set." );
            return null;
        }

        // ─────────────────────────────
        // Case 2: Tabular Result (≥1 column, ≥1 row)
        // ─────────────────────────────
        String[] colNames = new String[colCount]; // saves the column names to name the columns later
        String[] colTypeNames = new String[colCount]; // saves the columnTypeNames from Protobuf (e.g. DOCUMENT, STRING, INT,...)
        String[] instantiatedColumnTypes = new String[colCount]; //saves what type T createJavaTypeArrayList used for the i-th column represented by ArrayList<T>
        List<ArrayList<T>> columnList = new ArrayList<>( colCount ); // this is the java table that stores every Column in an ArrayList<?>

        for ( int i = 1; i <= colCount; i++ ) {

            //Saves the columnName (e.g. "Student_ID or "Birthdate") of the i-th column of the ResultSet
            colNames[i - 1] = meta.getColumnName( i );

            //Saves the columnTypeName (e.g. BINARY, INTEGER) of the i-th column of the ResultSet.
            colTypeNames[i - 1] = meta.getColumnTypeName( i );

            //This creates the proper frame to cast our data into. We changed to a column-based framework to increase performance and reduce
            //object instantiation. This way we can avoid Object[][] and instead work with Object[]
            columnList.add( createJavaTypeArrayList( colTypeNames[i - 1], instantiatedColumnTypes, i - 1 ) );
        }
        PolyphenyResultSet polyphenyResultSet = (PolyphenyResultSet) resultSet;

        // We pass the function a StringBuilder as the case LIST and DOCUMENT can be massively 
        // nested. Would we have a DOCUMENT with 10 000 entries that have a nesting level of 3 and each contain 10 000 items themselves we 
        // would instantiate 10 000^3 objects otherwise. This way we only instantiate 1 and if we don't need it we only wasted 1MB.
        StringBuilder sb = new StringBuilder( 1024 * 1024 );
        do {
            for ( int i = 1; i <= colCount; i++ ) {
                try {
                    //0. Making sure that the StringBuilder is empty; otherwise it will just append everything which would be wrong. 
                    sb.setLength( 0 );

                    //1. Retrieve the TypedValue using our LOCKPICK function.
                    TypedValue tv = getTypedValue( polyphenyResultSet, i );

                    //2. Convert it using our DriverToJava logic.
                    Object convertedValue = DriverToJava( tv, sb, instantiatedColumnTypes, i );

                    // 3. Shove it into the bucket (List<ArrayList<T>> handles the Object)
                    columnList.get( i - 1 ).add( (T) convertedValue );

                } catch ( Exception e ) {
                    // Fallback: If the heist fails, use the slow, safe JDBC way
                    throw new RuntimeException( "Conversion logic has failed in QueryExecutor.execute(...)." + e );
                }
            }
        } while ( resultSet.next() );

        // Ensure that the colNames and rows have the same number of columns
        if ( colNames.length != columnList.size() ) {
            throw new RuntimeException( "Mismatch of colNames and rowData in Queryexecutor.execute(...)" );
        }

        Object[] resultColumns = new Object[colCount];
        for ( int i = 0; i < colCount; i++ ) {
            resultColumns[i] = mapArrayListToPrimitive( columnList.get( i ), instantiatedColumnTypes, i );
        }
        // Return the names and the column data separately
        return new Object[]{ colNames, instantiatedColumnTypes, resultColumns };
    }


    /**
     * 
     * This function maps all the entries to the correct output type.
     * 
     * @param sb We pass the function a StringBuilder as the case LIST and DOCUMENT can be massively nested. Would we have a DOCUMENT with 10 000 entries that
     * have a nesting level of 3 and each contain 10 000 items themselves we would instantiate >10 000^3 objects otherwise. This way we only
     * instantiate 1 and if we don't need it we only wasted 1MB.
     * @param tv The TypedValue object we received from one column entry in the current row
     * @param instantiatedColumnTypes Saves what type T createJavaTypeArrayList used for the i-th column represented by ArrayList<T>
     * @param colIndex the colIndex we are looking at currently.
     * @return A java type that is mapped so that it fits into the ArrayList<T> that holds all the column entries
     * @throws SQLException
     */
    public Object DriverToJava( TypedValue tv, StringBuilder sb, String[] instantiatedColumnTypes, int colIndex ) throws Exception {

        // Use the found ValueCase for the ProtoValue class to decide the shortcut
        String typeName = tv.getValueCase().name();
        ProtoValue raw = getRawProto( tv );
        /*    BOOLEAN(1),
              INTEGER(2),
              LONG(3),
              BIG_DECIMAL(12),
              FLOAT(7),
              DOUBLE(6),
              DATE(5),
              TIME(9),
              TIMESTAMP(10),
              INTERVAL(13),
              STRING(8),
              BINARY(4),
              NULL(11),
              LIST(16),
              DOCUMENT(18),
              FILE(19),
              VALUE_NOT_SET(0); */

        // case 1
        if ( typeName.equals( "BOOLEAN" ) ) {
            boolean result = raw.getBoolean().getBoolean();
            return result ? 1 : 0;
        }
        // case 2
        if ( typeName.equals( "INTEGER" ) ) {
            double result = (double) raw.getInteger().getInteger();
            return result;
        }

        // case 3
        if ( typeName.equals( "LONG" ) ) {
            double result = (double) raw.getLong().getLong();
            return result;
        }

        // TODO: casting it to DOUBLE might lead to imprecision issues.
        // case 12
        if ( typeName.equals( "BIG_DECIMAL" ) ) {

            // 1. Get the Scale (power of 10)
            int scale = raw.getBigDecimal().getScale();

            // 2. Get the bits of the unscaled integer
            byte[] unscaledBytes = raw.getBigDecimal().getUnscaledValue().toByteArray();

            // 3. Reconstruct the number
            java.math.BigInteger unscaledInt = new java.math.BigInteger( unscaledBytes );
            java.math.BigDecimal bigDecimal = new java.math.BigDecimal( unscaledInt, scale );
            double doubleResult = bigDecimal.doubleValue();

            // 4. Return as double for MATLAB
            return doubleResult;
        }

        //case 7
        if ( typeName.equals( "FLOAT" ) ) {
            double result = (double) raw.getFloat().getFloat();
            return result;
        }
        //case 6
        if ( typeName.equals( "DOUBLE" ) ) {
            double result = (double) raw.getDouble().getDouble();
            return result;
        }

        //case 5 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "DATE" ) ) {
            String result = tv.asString();
            return result;
        }

        //case 9 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "TIME" ) ) {
            String result = tv.asString();
            return result;
        }
        // case 10 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "TIMESTAMP" ) ) {
            String result = tv.asString();
            return result;
        }
        // case 13
        if ( typeName.equals( "INTERVAL" ) ) {
            if ( raw == null )
                return null;

            // We return a 2-element array to keep BOTH components perfectly intact.
            // Index 0: Months (Year-Month interval)
            // Index 1: Milliseconds (Day-Time interval)
            return new double[]{
                    (double) raw.getInterval().getMonths(),
                    (double) raw.getInterval().getMilliseconds()
            };
        }
        // case 8
        if ( typeName.equals( "STRING" ) ) {
            String result = raw.getString().getString();
            return result;
        }

        // case 4
        if ( typeName.equals( "BINARY" ) ) {
            byte[] result = raw.getBinary().getBinary().toByteArray();
            return result;
        }

        // case 11 
        if ( typeName.equals( "NULL" ) ) {
            if ( instantiatedColumnTypes[colIndex] == "String" ) {
                return null;
            }
            if ( instantiatedColumnTypes[colIndex] == "byte[]" ) {
                return null;
            }
            if ( instantiatedColumnTypes[colIndex] == "Double" ) {
                return Double.NaN;
            }
            if ( instantiatedColumnTypes[colIndex] == "double[]" ) {
                return new double[]{ Double.NaN, Double.NaN };
            }

        }
        // case 16
        if ( typeName.equals( "LIST" ) ) {
            // Use the binary walker to build the JSON string for the list
            protoListToJson( raw.getList(), sb );
            return raw != null ? sb.toString() : "[]";
        }
        // case 18
        if ( typeName.equals( "DOCUMENT" ) ) {
            protoDocumentToJson( raw.getDocument(), sb );
            return raw != null ? sb.toString() : "{}";
        }

        // case 19
        if ( typeName.equals( "FILE" ) ) {
            // Use the binary walker to build the JSON string for the list
            byte[] file = raw.getFile().getBinary().toByteArray();
            return file;
        }
        // case 0
        if ( typeName.equals( "VALUE_NOT_SET" ) ) {
            throw new RuntimeException( "Internal Error: DriverToJava failed. Reason: VALUE_NOT_SET case was hit." );
        }
        throw new SQLException( "Internal Error: DriverToJava failed. Reason: Unhandled or unexpected type." + typeName );
    }


    /**
     * @Description
     * Converts any ProtoValue or TypedValue to Json String format. There is a recursive call between protoValueToJson and protoDocumentToJson because protoValues can be protoDocuments which, in turn, contain ProtoValues.
     * 
     * @param sb We pass the function a StringBuilder as the case LIST and DOCUMENT can be massively nested. Would we have a DOCUMENT with 10 000 entries that
     * have a nesting level of 3 and each contain 10 000 items themselves we would instantiate >10 000^3 objects otherwise. This way we only
     * instantiate 1 and if we don't need it we only wasted 1MB.
     * @param v The ProtoValue that we want to convert to Json
     */
    private void protoValueToJson( ProtoValue v, StringBuilder sb ) throws Exception {
        // TODO: switch to the same cases as in ProtoValueToJson
        /*    BOOLEAN(1),
              INTEGER(2),
              LONG(3),
              BIG_DECIMAL(12),
              FLOAT(7),
              DOUBLE(6),
              DATE(5),
              TIME(9),
              TIMESTAMP(10),
              INTERVAL(13),
              STRING(8),
              BINARY(4),
              NULL(11),
              LIST(16),
              DOCUMENT(18),
              FILE(19),
              VALUE_NOT_SET(0); */
        String typeName = v.getValueCase().name();
        // case 1
        if ( typeName.equals( "BOOLEAN" ) ) {
            boolean result = v.getBoolean().getBoolean();
            sb.append( result );
            return;
        }
        // case 2
        if ( typeName.equals( "INTEGER" ) ) {
            double result = (double) v.getInteger().getInteger();
            sb.append( result );
            return;
        }

        // case 3
        if ( typeName.equals( "LONG" ) ) {
            double result = (double) v.getLong().getLong();
            sb.append( result );
            return;
        }

        // TODO: fix the case when the unscaled bytes might exceed what double can represent
        // case 12
        if ( typeName.equals( "BIG_DECIMAL" ) ) {

            // 1. Get the Scale (power of 10)
            int scale = v.getBigDecimal().getScale();

            // 2. Get the bits of the unscaled integer
            byte[] unscaledBytes = v.getBigDecimal().getUnscaledValue().toByteArray();

            // 3. Reconstruct the number
            java.math.BigInteger unscaledInt = new java.math.BigInteger( unscaledBytes );
            java.math.BigDecimal bigDecimal = new java.math.BigDecimal( unscaledInt, scale );
            double doubleResult = bigDecimal.doubleValue();

            // 4. Return as double for MATLAB
            if ( Double.isInfinite( doubleResult ) || Double.isNaN( doubleResult ) ) {
                // If it's out of bounds, maybe log it or return a specific error value
                doubleResult = Double.NaN;
            }
            sb.append( doubleResult );
            return;
        }

        //case 7
        if ( typeName.equals( "FLOAT" ) ) {
            double result = (double) v.getFloat().getFloat();
            sb.append( result );
            return;
        }
        //case 6
        if ( typeName.equals( "DOUBLE" ) ) {
            double result = (double) v.getDouble().getDouble();
            sb.append( result );
            return;
        }

        //case 5 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "DATE" ) ) {
            TypedValue tv = new TypedValue( v );
            sb.append( "\"" ).append( tv.asString() ).append( "\"" );
            return;
        }

        //case 9 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "TIME" ) ) {
            TypedValue tv = new TypedValue( v );
            sb.append( "\"" ).append( tv.asString() ).append( "\"" );
            return;
        }

        // case 10 - For this case use the asString() function of TypedValue because the Driver actually implements functionality for timezones
        if ( typeName.equals( "TIMESTAMP" ) ) {
            TypedValue tv = new TypedValue( v );
            sb.append( "\"" ).append( tv.asString() ).append( "\"" );
            return;
        }
        // case 13
        if ( typeName.equals( "INTERVAL" ) ) {
            // We return a 2-element array to keep BOTH components perfectly intact.
            // Index 0: Months (Year-Month interval)
            // Index 1: Milliseconds (Day-Time interval)
            double[] result = new double[]{
                    (double) v.getInterval().getMonths(),
                    (double) v.getInterval().getMilliseconds()
            };
            sb.append( "[" ).append( result[0] ).append( "," ).append( result[1] ).append( "]" );
            return;
        }

        // case 8
        if ( typeName.equals( "STRING" ) ) {
            String result = v.getString().getString();
            sb.append( "\"" ).append( escapeJson( result ) ).append( "\"" );
            return;
        }

        // case 4
        if ( typeName.equals( "BINARY" ) ) {
            byte[] bits = v.getBinary().getBinary().toByteArray();
            String encoded = Base64.getEncoder().encodeToString( bits );
            sb.append( "\"" ).append( encoded ).append( "\"" );
            return;
        }

        // case 11 
        if ( typeName.equals( "NULL" ) ) {
            sb.append( "null" );
            return;
        }
        // case 16
        if ( typeName.equals( "LIST" ) ) {
            // Use the binary walker to build the JSON string for the list
            protoListToJson( v.getList(), sb );
            return;
        }
        // case 18
        if ( typeName.equals( "DOCUMENT" ) ) {
            protoDocumentToJson( v.getDocument(), sb );
            return;
        }

        // case 19
        if ( typeName.equals( "FILE" ) ) {
            // Use the binary walker to build the JSON string for the list
            byte[] bits = v.getFile().getBinary().toByteArray(); // the file is save in a byte array like the binary case
            String encoded = Base64.getEncoder().encodeToString( bits );
            sb.append( "\"" ).append( encoded ).append( "\"" );
            return;
        }
        // case 0
        if ( typeName.equals( "VALUE_NOT_SET" ) ) {
            throw new RuntimeException( "Internal Error: DriverToJava failed. Reason: VALUE_NOT_SET case was hit." );
        }
        throw new SQLException( "Internal Error: DriverToJava failed. Reason: Unhandled or unexpected type." + typeName );
    }


    /**
     * This function converts an object of type ProtoList (which is what is used to represent arrays by Polypheny/the Driver) to a Json
     * 
     * @param list The ProtoList that we want to convert to Json (fetched in ProtoValueToJson)
     * @param sb We pass the function a StringBuilder as the case LIST and DOCUMENT can be massively nested. Would we have a DOCUMENT with 10 000 entries that
     * have a nesting level of 3 and each contain 10 000 items themselves we would instantiate >10 000^3 objects otherwise. This way we only
     * instantiate 1 and if we don't need it we only wasted 1MB.
     */
    private void protoListToJson( org.polypheny.jdbc.dependency.prism.ProtoList list, StringBuilder sb ) throws Exception {
        if ( list == null || list.getValuesCount() == 0 ) {
            sb.append( "[]" );
            return;
        }
        sb.append( "[" );
        int size = list.getValuesCount();
        for ( int i = 0; i < size; i++ ) {
            protoValueToJson( list.getValues( i ), sb ); // RECURSE with the same SB

            if ( i < size - 1 )
                sb.append( "," );
        }
        sb.append( "]" );
    }


    /**
     * @Description
     * This function maps a ProtoDocument (which is a subtype of ProtoValue) to JSON which matlab can later jsondecode. There is a recursive call between protoValueToJson and protoDocumentToJson because protoValues can be
     * protoDocuments which, in turn, contain ProtoValues.
     * @param doc The ProtoDocument we want to convert to Json
     * @param sb We pass the function a StringBuilder as the case LIST and DOCUMENT can be massively nested. Would we have a DOCUMENT with 10 000 entries that
     * have a nesting level of 3 and each contain 10 000 items themselves we would instantiate >10 000^3 objects otherwise. This way we only
     * instantiate 1 and if we don't need it we only wasted 1MB.
     */
    private void protoDocumentToJson( ProtoDocument doc, StringBuilder sb ) throws Exception {
        if ( doc == null || doc.getEntriesCount() == 0 ) {
            sb.append( "{}" );
            return;
        }

        else {

            sb.append( "{" );
            // Use getEntriesList() to get the collection, then iterate
            int count = doc.getEntriesCount();
            for ( int i = 0; i < count; i++ ) {
                // Most likely the entry type is now an inner class or named simply "Entry"
                org.polypheny.jdbc.dependency.prism.ProtoEntry entry = doc.getEntries( i );

                //sb.append( "\"" ).append( entry.getKey() ).append( "\":" );
                protoValueToJson( entry.getKey(), sb );
                sb.append( ":" );
                protoValueToJson( entry.getValue(), sb );

                if ( i < count - 1 ) {
                    sb.append( "," );
                }
            }
            sb.append( "}" );
        }

    }


    /**
     * @Description
     * This function returns the ArrayList<Type> that the column should have
     * 
     * @param <T> the generic type that the ArrayList should be containing
     * @param polyphenyTypeName the TypeName that the ProtoPolyType class returns
     * @param typeDecision Is an array of Strings where the i-th entry represents the type T of the i-th column=ArrayList<T>. i.e. if the i-th
     * column is saved in a ArrayList<Boolean> then typeDecision[i] = "Boolean"
     * @return ArrayList<Type> with the correct type to fill the column of the SQL query with later on
     * REMARK: It is necessary to cast every return to (ArrayList<T>) because otherwise
     */
    @SuppressWarnings("unchecked")
    private <T> ArrayList<T> createJavaTypeArrayList( String polyphenyTypeName, String[] typeDecision, int colIndex ) {

        switch ( polyphenyTypeName ) {
            case "UNSPECIFIED": // case 0
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "BOOLEAN": // case 1
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "TINYINT": // case 2
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "SMALLINT": // case 3
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "INTEGER": // case 4
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "BIGINT": // case 5
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "DECIMAL": // case 6
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "REAL": // case 7
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "FLOAT": // case 8
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "DOUBLE": // case 9
                typeDecision[colIndex] = "Double";
                return (ArrayList<T>) new ArrayList<Double>();
            case "DATE": // case 10
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "TIME": // case 11
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();

            // case 12, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 
            // These fall through to default in the JAR
            default:
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();

            case "TIMESTAMP": // case 13
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "INTERVAL": // case 15
                typeDecision[colIndex] = "double[]";
                return (ArrayList<T>) new ArrayList<double[]>();
            case "CHAR": // case 28
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "VARCHAR": // case 29
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "BINARY": // case 30
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "VARBINARY": // case 31
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "NULL": // case 32
                typeDecision[colIndex] = "NULL";
                return (ArrayList<T>) new ArrayList<String>();
            case "ARRAY": // case 33
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "MAP": // case 34
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "DOCUMENT": // case 35
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "GRAPH": // case 36
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "NODE": // case 37
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "EDGE": // case 38
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "PATH": // case 39
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "IMAGE": // case 40
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "VIDEO": // case 41
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "AUDIO": // case 42
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "FILE": // case 43
                typeDecision[colIndex] = "byte[]";
                return (ArrayList<T>) new ArrayList<byte[]>();
            case "DISTINCT": // case 44
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "STRUCTURED": // case 45
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "ROW": // case 46
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "OTHER": // case 47
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "CURSOR": // case 48
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "COLUMN_LIST": // case 49
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "DYNAMIC_STAR": // case 50
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "GEOMETRY": // case 51
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "SYMBOL": // case 52
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "JSON": // case 53
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "MULTISET": // case 54
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "ANY": // case 55
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "USER_DEFINED_TYPE": // case 56
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "ROW_ID": // case 57
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
            case "TEXT": // case 58
                typeDecision[colIndex] = "String";
                return (ArrayList<T>) new ArrayList<String>();
        }
    }


    private Object mapArrayListToPrimitive( ArrayList<?> list, String[] instantiatedColumnTypes, int colIndex ) {

        // this check is technically redundant because all the null cases are caught in DriverToJava and other function before mapArrayListToPrimitive is ever called.
        if ( list == null || list.isEmpty() ) {
            // Return an empty array of the correct type so MATLAB doesn't complain
            if ( instantiatedColumnTypes[colIndex].equals( "Double" ) )
                return new double[0];
            return new String[0];
        }

        switch ( instantiatedColumnTypes[colIndex] ) {
            case "Double":
                // ArrayList<Double> -> double[]
                // This is the "Primitive Mapping" you asked about earlier!
                // It handles NaN perfectly for MATLAB.
                instantiatedColumnTypes[colIndex] = "double";
                return list.stream().mapToDouble( obj -> (Double) obj ).toArray();

            case "String":
                // ArrayList<String> -> String[]
                instantiatedColumnTypes[colIndex] = "String";
                // Use the Java 8 compatible method
                return list.toArray( new String[0] );

            case "byte[]":
                // ArrayList<byte[]> -> byte[][]
                instantiatedColumnTypes[colIndex] = "byte[]";
                // MATLAB sees this as a cell array of uint8 arrays
                return list.toArray( new byte[0][] );

            case "double[]":
                // This handles the INTERVAL case (which returns [months, millis])
                instantiatedColumnTypes[colIndex] = "double[]";
                // ArrayList<double[]> -> double[][]
                return list.toArray( new double[0][] );

            case "NULL":
                instantiatedColumnTypes[colIndex] = "NULL";
                return new Object[list.size()];

            default:
                instantiatedColumnTypes[colIndex] = "default case in mapArrayListToPrimitive";
                return list.toArray();
        }
    }


    /**
     * @Description
     * This function takes makes sure that the escapes of queries are handled correctly in Strings when appending.
     * e.g. " C\mypath " must be converted into " C\\mypath " because "\" is an operator sign like in Latex
     * @param string
     * @return string: The string with the proper escape sequences
     */
    private static String escapeJson( String string ) {
        return string.replace( "\\", "\\\\" ).replace( "\"", "\\\"" );
    }


    // TODO: switch to preparedStatement
    /**
     * @Description
     * This function is capable of executing a List of non-SELECT SQL statements in one single Matlab-Java crossing.
     * All SQL statements except SELECT are supported. For further information consult the Polypheny JDBC Driver documentation
     * → https://docs.polypheny.com/en/latest/drivers/jdbc/relational/statement
     * 
     * @param queries The list of SQL query strings to be executed.
     * @return List<Integer> result A list of integers, where the i-th entry will denote for the i-th query how many rows were touched, e.g.
     * n: n rows were updated, 0: no rows were touched.
     */
    public int[] executeBatchSql( List<String> queries ) {
        polyconnection.openIfNeeded();
        try {
            polyconnection.beginTransaction();
            try ( Statement stmt = polyconnection.getConnection().createStatement() ) {
                for ( String query : queries ) {
                    String first = query.trim().toUpperCase();
                    if ( first.startsWith( "SELECT" ) ) {
                        throw new UnsupportedOperationException( "Batch execution does not support SELECT statements." );
                    }
                    stmt.addBatch( query );
                }
                int[] result = stmt.executeBatch();
                polyconnection.commitTransaction();
                return result;   // return directly
            } catch ( SQLException e ) {
                try {
                    polyconnection.rollbackTransaction();
                } catch ( Exception rollbackException ) {
                    // Propagate both the batch failure AND the rollback failure → User must be made
                    throw new RuntimeException( "SQL batch failed AND rollback failed: " + rollbackException.getMessage(), e );
                }
                throw translateException( e );
            } catch ( Exception e ) {
                try {
                    polyconnection.rollbackTransaction();
                } catch ( Exception rollbackEx ) {
                    // Propagate both the batch failure AND the rollback failure → User must be made
                    throw new RuntimeException( "SQL batch failed AND rollback failed: " + rollbackEx.getMessage(), e );
                }
                throw new RuntimeException( "SQL batch execution failed. Transaction was rolled back: " + e.getMessage(), e );
            }

        } catch ( SQLException e ) {
            throw new RuntimeException( "Failed to manage transaction", e );
        }
    }


    /**
     * @Description
     * This function is capable of executing a List of Mongo statements in one single Matlab-Java crossing.
     * Each query is executed individually via the execute(...) method. The result of each query will be a List<String>
     * containing the JSON-encoded documents or scalars (as JSON strings). All individual query results are then grouped
     * into an outer List, which represents the batch result.
     * 
     * @param namespace The Mongo namespace (e.g. database / collection context).
     * @param queries The list of Mongo query strings to be executed.
     * @return List<List<String>> result An outer list with one entry per query. Each entry is a List<String> containing
     * the documents or scalar results (as JSON strings) returned by the respective query.
     */
    public List<List<String>> executeBatchMongo( String namespace, List<String> queries ) {
        polyconnection.openIfNeeded();
        List<List<String>> results = new ArrayList<>();
        try {
            polyconnection.beginTransaction();

            for ( String query : queries ) {
                @SuppressWarnings("unchecked") List<String> result = (List<String>) executeMongo( "mongo", namespace, query );
                results.add( result );
            }

            polyconnection.commitTransaction();  // commit if all succeeded
            return results;

        } catch ( Exception e ) {
            try {
                polyconnection.rollbackTransaction();  // rollback if anything failed
            } catch ( Exception rollbackEx ) {
                throw new RuntimeException( "Rollback failed after batch error", rollbackEx );
            }
            throw new RuntimeException( "Batch execution failed", e );
        }
    }


    /**
     * @Description
     * This method ensures that exceptions thrown by Polypheny (and propagated through the JDBC driver) due to user fault when calling
     * execute or executeBatch are translated in a user-interpretable error to be propagated to Matlab, instead of just failing
     * with an obscure error/exception.
     * 
     * @param e The exception caught. 08 denotes an error with the Polypheny connection, 42 denotes a
     * @return
     */
    private RuntimeException translateException( SQLException e ) {
        String state = e.getSQLState();
        if ( state != null ) {
            if ( state.startsWith( "08" ) ) {
                return new RuntimeException( "Connection error: " + e.getMessage(), e );
            }
            if ( state.startsWith( "42" ) ) {
                return new RuntimeException( "Syntax error in query: " + e.getMessage(), e );
            }
        }
        return new RuntimeException( "Query execution failed: " + e.getMessage(), e );
    }

    /*
     * This function might be used in the future to automatically detect the namespace in MQL queries.
    
    private static void checkNamespace( String language, String namespace ) {
        if ( namespace == null || namespace.isEmpty() ) {
            if ( language.equalsIgnoreCase( "sql" ) ) {
                // fine: default namespace is used implicitly
            } else {
                throw new IllegalArgumentException(
                        "For " + language + " queries a namespace must be specified"
                );
            }
        }
    }
    */

    /**
     * @Description
     * This function determines the operation (e.g. "find" or "insertOne") of a Mongo query. This is important to distinguish
     * whether to use executeUpdate or executeQuery. Functionality was moved to a function (instead of handling it like for SQL)
     * because in Mongo queries the operation isn't as easy to determine.
     * MQL queries are always of the form <db>.<namespace>.<operation>()
     * 
     * @param q The query text of type String
     * @return
     **/
    /*
    private static String extractMongoOperation( String q ) {
        String query = q.trim();
        int paren = query.indexOf( '(' );                    // get the position of the first "(" in the query. 
        if ( paren < 0 ) {
            return "";                                          // return an empty String if no ( was found
        }
        int lastDot = query.lastIndexOf( '.', paren );       // get the position of the last "." before the "(".
        if ( lastDot < 0 ) {
            return "";                                          // return an empty string if no dot was found
        }
        String operation = query.substring( lastDot + 1, paren ).trim();
        return operation;                                       // return the <operation>
    }
    */

}
