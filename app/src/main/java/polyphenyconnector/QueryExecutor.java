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
     * @return: Object of the query (SQL: empty, scalar or table; MongoQL: TODO; Cypher: TODO)
     **/
    public QueryExecutor( PolyphenyConnection polyconnection ) {
        this.polyconnection = polyconnection;
    }


    /**
     * @Description
     * - Executes the query depending on the language given by the user
     * 
     * @param language: The database language that is used (e.g. SQL, MongoQL,Cypher)
     * @param namespace: The namespace in the query string. For SQL this argument as no effect as we use JDBC's
     * executeQuery(...)/executeUpdate(...). For MQL this argument will be passed to JDBC's execute(...) function. For further
     * information consult Polpyheny's web documentation.
     * @param query: The query-text to be executed (e.g. FROM emps SELECT *)
     * 
     * @return: ResultToMatlab(rs) which is a Matlab compatible object that is cast to the Matlab user.
     **/

    /**
     * @Description
     * - Executes the query depending on the language given by the user
     * 
     * @param language: The database language that is used (e.g. SQL, MongoQL,Cypher)
     * @param namespace: The namespace in the query string. For SQL this argument as no effect as we use JDBC's
     * executeQuery(...)/executeUpdate(...). For MQL this argument will be passed to JDBC's execute(...) function. For further
     * information consult Polpyheny's web documentation.
     * @param query: The query-text to be executed (e.g. FROM emps SELECT *)
     * 
     * @return: ResultToMatlab(rs) which is a Matlab compatible object that is cast to the Matlab user.
     **/
    public Object execute( String language, String namespace, String query ) {
        polyconnection.openIfNeeded();
        try {
            Connection conn = polyconnection.getConnection();
            conn.setAutoCommit( true );
        } catch ( SQLException e ) {
            throw translateSQLException( e );
        }

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
                    throw translateSQLException( e );
                } catch ( Exception e ) {
                    throw new RuntimeException( "SQL execution failed: " + e.getMessage(), e );
                }

            case "mongo":
                try {

                    // Get the connection variable from the PolyphenyConnection.java class using the getter
                    Connection connection = polyconnection.getConnection();

                    // Unwrap Connection connection to the JDBC Driver-supplied PolyConnection polyConnection
                    PolyConnection polyConnection = connection.unwrap( PolyConnection.class );

                    // Create a PolyStatement object to call .execute(...) method of the JDBC- Driver on
                    PolyStatement polyStatement = polyConnection.createPolyStatement();

                    // Call the execute(...) function on the polyStatement
                    Result result = polyStatement.execute( namespace, language, query );

                    switch ( result.getResultType() ) {
                        case DOCUMENT:
                            DocumentResult documentResult = result.unwrap( DocumentResult.class );
                            //return DocumentToMatlab( documentResult );
                        case SCALAR:
                            ScalarResult scalarResult = result.unwrap( ScalarResult.class );
                            return scalarResult.getScalar();

                        default:
                            throw new UnsupportedOperationException( "Unhandled result type: " + result.getResultType() );
                    }
                } catch ( SQLException e ) {
                    throw translateSQLException( e );
                } catch ( Exception e ) {
                    throw new RuntimeException( "Mongo execution failed: " + e.getMessage(), e );
                }
            case "cypher":
                throw new UnsupportedOperationException( "Cypher execution not yet implemented." );
        }

    }


    /**
     * @Description
     * This function is capable of executing a List of non-SELECT SQL statements in one single Matlab-Java crossing. All SQL statements
     * except SELECT are supported. For further information consult the Polyphenys JDBC Driver documentation.
     * 
     * @param language The database language that is used (e.g. SQL, MongoQL,Cypher)
     * @param namespace: The namespace in the query string. For SQL this argument as no effect as we use JDBC's
     * executeQuery(...)/executeUpdate(...). For MQL this argument will be passed to JDBC's execute(...) function. For further
     * information consult Polpyheny's web documentation.
     * @param query_list The list of query strings to be executed.
     * @return int[] result An array of integers, where the i-th entry will denote for the i-th query how many rows were touched, e.g.
     * n: n rows were updated, 0: no rows were touched.
     */
    public int[] executeBatch( String language, String namespace, List<String> query_list ) {
        polyconnection.openIfNeeded();

        switch ( language.toLowerCase() ) {
            default:
                throw new UnsupportedOperationException( "Unsupported language: " + language );

            case "sql":
                try {
                    polyconnection.beginTransaction();
                    try ( Statement stmt = polyconnection.getConnection().createStatement() ) {
                        for ( String query : query_list ) {
                            String first = query.trim().toUpperCase();
                            if ( first.startsWith( "SELECT" ) ) {
                                throw new UnsupportedOperationException( "Batch execution does not support SELECT statements." );
                            }
                            stmt.addBatch( query );
                        }
                        int[] result = stmt.executeBatch();
                        polyconnection.commitTransaction();
                        return result;

                    } catch ( SQLException e ) {
                        throw translateSQLException( e );
                    } catch ( Exception e ) {
                        polyconnection.rollbackTransaction();
                        throw new RuntimeException( "SQL batch execution failed. Transaction was rolled back: " + e.getMessage(), e );
                    }

                } catch ( SQLException e ) {
                    throw new RuntimeException( "Failed to manage transaction", e );
                }

            case "mongoql":
                throw new UnsupportedOperationException( "MongoQL batch execution not yet implemented." );

            case "cypher":
                throw new UnsupportedOperationException( "Cypher batch execution not yet implemented." );
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
            System.out.println( "Empty result set." );
            return null;
        }

        // ─────────────────────────────
        // Case 2: Scalar Result
        // ─────────────────────────────
        if ( colCount == 1 && rs.isLast() ) {
            System.out.println( "Scalar result set." );
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

    /* 
    private String[] DocumentToMatlab( DocumentResult documentResult ) {
        List<String> docs = new ArrayList<>();
        Iterator<PolyDocument> documentIterator = documentResult.iterator(); // Call iterator for PolyDocumentResult
        while ( documentIterator.hasNext() ) {
            PolyDocument document = documentIterator.next();
            docs.add( NestedPolyDocumentToString( document ) );
        }
        return docs.toArray( new String[0] );
    }
    */


    /**
     * @Description
     * This method ensures that exceptions thrown by Polypheny (and propagated through the JDBC driver) due to user fault when calling
     * execute or executeBatch are translated in a user-interpretable error to be propagated to Matlab, instead of just failing
     * with an obscure error/exception.
     * 
     * @param e The exception caught. 08 denotes an error with the Polypheny connection, 42 denotes a
     * @return
     */
    private RuntimeException translateSQLException( SQLException e ) {
        String state = e.getSQLState();
        if ( state != null ) {
            if ( state.startsWith( "08" ) ) {
                return new RuntimeException( "Connection error: " + e.getMessage(), e );
            }
            if ( state.startsWith( "42" ) ) {
                return new RuntimeException( "Syntax error in query: " + e.getMessage(), e );
            }
        }
        return new RuntimeException( "SQL execution failed: " + e.getMessage(), e );
    }

    /* 
    private String NestedPolyDocumentToString( PolyDocument doc ) {
        StringBuilder stringbuilder = new StringBuilder();
        stringbuilder.append( "{" );
    
        Iterator<Map.Entry<String, TypedValue>> it = doc.entrySet().iterator();
        while ( it.hasNext() ) {
            Map.Entry<String, TypedValue> entry = it.next();
            String key = entry.getKey().toString();
            TypedValue value = entry.getValue();
    
            stringbuilder.append( "\"" ).append( key ).append( "\":" );
    
            if ( value instanceof PolyDocument ) {
                // recurse for nested docs
                stringbuilder.append( NestedPolyDocumentToString( (PolyDocument) value ) );
            } else if ( value != null ) {
                stringbuilder.append( "\"" ).append( value.toString() ).append( "\"" );
            } else {
                stringbuilder.append( "null" );
            }
    
            if ( it.hasNext() ) {
                stringbuilder.append( "," );
            }
        }
    
        stringbuilder.append( "}" );
        return stringbuilder.toString();
    }
    */


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


    /**
     * @Description
     * This function determines the operation (e.g. "find" or "insertOne") of a MongoQL query. This is important to distinguish
     * whether to use executeUpdate or executeQuery. Functionality was moved to a function (instead of handling it like for SQL)
     * because in MongoQL queries the operation isn't as easy to determine.
     * MQL queries are always of the form <db>.<namespace>.<operation>()
     * 
     * @param q The query text of type String
     * @return
     */
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

}
