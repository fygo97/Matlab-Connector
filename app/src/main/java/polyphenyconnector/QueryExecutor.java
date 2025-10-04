package polyphenyconnector;

import java.sql.*;
import java.util.*;

import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.*;
import org.polypheny.jdbc.types.*;

public class QueryExecutor {

    private PolyphenyConnection polyconnection;


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


    /**
     * @Description
     * - Executes the query depending on the language given by the user
     * 
     * @param language: The database language that is used (e.g. SQL, Mongo,Cypher)
     * @param namespace: The namespace in the query string. For SQL this argument as no effect as we use JDBC's
     * executeQuery(...)/executeUpdate(...). For MQL this argument will be passed to JDBC's execute(...) function. For further
     * information consult Polpyheny's web documentation.
     * @param query: The query-text to be executed (e.g. FROM emps SELECT *)
     * 
     * @return: ResultToMatlab(rs) which is a Matlab compatible object that is cast to the Matlab user.
     **/
    public Object execute( String language, String namespace, String query ) {
        polyconnection.openIfNeeded();

        switch ( language.toLowerCase() ) {
            default:
                throw new UnsupportedOperationException( "Unsupported language: " + language );

            case "sql":
                try ( Statement stmt = polyconnection.getConnection().createStatement() ) {

                    String first = query.trim().toUpperCase();

                    // SELECT statements
                    if ( first.startsWith( "SELECT" ) ) {
                        try ( ResultSet rs = stmt.executeQuery( query ) ) {
                            return SQLResultToMatlab( rs );
                        }

                        // INSERT, UPDATE, DELETE, CREATE, DROP, ... statements    
                    } else {
                        int rs = stmt.executeUpdate( query );
                        return rs;

                    }

                } catch ( SQLException e ) {
                    throw translateException( e );
                } catch ( Exception e ) {
                    throw new RuntimeException( "SQL execution failed: " + e.getMessage(), e );
                }

            case "mongo":
                try {

                    // Get the connection variable from the PolyphenyConnection.java class using the getter
                    Connection connection = polyconnection.getConnection();

                    // Unwrap Connection connection to the JDBC Driver-supplied PolyConnection polyConnection
                    PolyConnection polyConnection = connection.unwrap( PolyConnection.class );

                    // Create a PolyStatement object to call .execute(...) method of the JDBC-Driver on
                    PolyStatement polyStatement = polyConnection.createPolyStatement();

                    // Call the execute(...) function on the polyStatement
                    Result result = polyStatement.execute( namespace, language, query );

                    switch ( result.getResultType() ) {

                        case DOCUMENT:
                            // Unwrapping according to → https://docs.polypheny.com/en/latest/drivers/jdbc/extensions/result
                            DocumentResult documentResult = result.unwrap( DocumentResult.class );
                            return DocumentToMatlab( documentResult );

                        // This case was never used in any of the JUnit Tests, as every Mongo Query currently seems to be wrapped as 
                        // PolyDocument by the JDBC Driver. The case was still left in as security based on the official documentation:
                        // → https://docs.polypheny.com/en/latest/drivers/jdbc/extensions/result 
                        case SCALAR:
                            ScalarResult scalarResult = result.unwrap( ScalarResult.class );
                            long scalar = scalarResult.getScalar();
                            return scalar;

                        default:
                            throw new UnsupportedOperationException( "Unhandled result type: " + result.getResultType() );
                    }
                } catch ( SQLException e ) {
                    throw translateException( e );
                } catch ( Exception e ) {
                    throw new RuntimeException( "Mongo execution failed: " + e.getMessage(), e );
                }
            case "cypher":
                throw new UnsupportedOperationException( "Cypher execution not yet implemented." );
        }

    }


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
                @SuppressWarnings("unchecked") List<String> result = (List<String>) execute( "mongo", namespace, query );
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
     * - Casts the result of SQL queries to MatlabObjects
     * 
     * @param rs: The result object of the query of type ResultSet
     * 
     * @return: Result from the query which is either null/scalar/table
     **/
    public Object SQLResultToMatlab( ResultSet rs ) throws Exception {

        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        Object[][] ResultArray;

        // ─────────────────────────────
        // Case 1: Empty Result
        // ─────────────────────────────
        if ( !rs.next() ) {
            //System.out.println( "Empty result set." );
            return null;
        }

        // ─────────────────────────────
        // Case 2: Scalar Result
        // ─────────────────────────────
        if ( colCount == 1 && rs.isLast() ) {
            //System.out.println( "Scalar result set." );
            Object scalar = rs.getObject( 1 );
            return scalar;
        }

        // ─────────────────────────────
        // Case 3: Tabular Result (≥1 column, ≥1 row)
        // ─────────────────────────────
        String[] colNames = new String[colCount]; // get the column names to name the columns later

        for ( int i = 1; i <= colCount; i++ ) {
            colNames[i - 1] = meta.getColumnName( i ); // assign the column names to the array
        }

        List<Object[]> rows = new ArrayList<>(); // List of arrays to store the rows returned by the query
        do {
            Object[] row = new Object[colCount]; // Creates new array that will store the queries entries
            for ( int i = 0; i < colCount; i++ ) {
                row[i] = rs.getObject( i + 1 ); // Saves each entry
            }
            rows.add( row ); // Append row to the List
        } while ( rs.next() ); // First row already fetched above with rs.next() so we use do while

        // Ensure that the colNames and rows have the same number of columns
        if ( colNames.length != rows.get( 0 ).length ) {
            throw new RuntimeException( "Mismatch: colNames and rowData column count don't match" );
        }

        ResultArray = rows.toArray( new Object[rows.size()][] );

        return new Object[]{ colNames, ResultArray };
    }


    private List<String> DocumentToMatlab( DocumentResult documentResult ) {
        List<String> docs = new ArrayList<>();
        Iterator<PolyDocument> documentIterator = documentResult.iterator();
        while ( documentIterator.hasNext() ) {
            PolyDocument document = documentIterator.next();
            docs.add( NestedPolyDocumentToString( document ) ); // at the most outer layer everything must be wrapped as PolyDocument
        }
        return docs;
    }


    private String NestedPolyDocumentToString( PolyDocument document ) {
        StringBuilder sb = new StringBuilder();
        sb.append( "{" );
        Iterator<Map.Entry<String, TypedValue>> it = document.entrySet().iterator();
        while ( it.hasNext() ) {
            Map.Entry<String, TypedValue> entry = it.next();
            sb.append( "\"" ).append( entry.getKey() ).append( "\":" );
            sb.append( anyToJson( entry.getValue() ) );
            if ( it.hasNext() )
                sb.append( "," );
        }
        sb.append( "}" );
        return sb.toString();
    }


    private String NestedArrayToString( Array array ) {
        StringBuilder sb = new StringBuilder();
        sb.append( "[" );
        try {
            Object[] elems = (Object[]) array.getArray();
            for ( int i = 0; i < elems.length; i++ ) {
                sb.append( anyToJson( elems[i] ) );
                if ( i < elems.length - 1 )
                    sb.append( "," );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( "List serialization error: " + e.getMessage(), e );
        }
        sb.append( "]" );
        return sb.toString();
    }


    private String anyToJson( Object result ) {
        if ( result == null ) {
            return "null";
        }
        try {
            if ( result instanceof TypedValue ) {
                TypedValue value = (TypedValue) result;
                switch ( value.getValueCase() ) {
                    case DOCUMENT:
                        return NestedPolyDocumentToString( value.asDocument() );
                    case LIST:
                        return NestedArrayToString( value.asArray() );
                    case BOOLEAN:
                        return String.valueOf( value.asBoolean() );
                    case INTEGER:
                        return String.valueOf( value.asInt() );
                    case LONG:
                        return String.valueOf( value.asLong() );
                    case DOUBLE:
                        return String.valueOf( value.asDouble() );
                    case FLOAT:
                        return String.valueOf( value.asFloat() );
                    case BIG_DECIMAL:
                        return value.asBigDecimal().toPlainString();
                    case STRING:
                        return "\"" + escapeJson( value.asString() ) + "\"";
                    case DATE:
                        return "\"" + value.asDate().toString() + "\"";
                    case TIME:
                        return "\"" + value.asTime().toString() + "\"";
                    case TIMESTAMP:
                        return "\"" + value.asTimestamp().toString() + "\"";
                    case INTERVAL:
                        return "\"" + value.asInterval().toString() + "\"";
                    case BINARY:
                        return "\"" + Base64.getEncoder().encodeToString( value.asBytes() ) + "\"";
                    case FILE:
                        return "\"" + escapeJson( String.valueOf( value.asBlob() ) ) + "\"";
                    case NULL:
                        return "null";
                    default:
                        return "\"" + escapeJson( value.toString() ) + "\"";
                }
            } else if ( result instanceof PolyDocument ) {
                return NestedPolyDocumentToString( (PolyDocument) result );
            } else if ( result instanceof Array ) {
                return NestedArrayToString( (Array) result );
            } else if ( result instanceof String ) {
                return "\"" + escapeJson( (String) result ) + "\"";
            } else if ( result instanceof java.sql.Date
                    || result instanceof java.sql.Time
                    || result instanceof java.sql.Timestamp ) {
                return "\"" + result.toString() + "\"";
            } else if ( result instanceof byte[] ) {
                return "\"" + Base64.getEncoder().encodeToString( (byte[]) result ) + "\"";
            } else {
                // numbers, booleans, anything else
                return String.valueOf( result );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( "Serialization error: " + e.getMessage(), e );
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
