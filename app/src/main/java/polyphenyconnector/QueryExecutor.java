package polyphenyconnector;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor {

    private PolyphenyConnection polyconnection;


    /**
     * @Description
     * - Constructor
     * 
     * @param polyconnection: PolyphenyConnection object that holds the connection
     * details to the Database. It's used to execute queries
     * @return: Object of the query (SQL: empty, scalar or table; MongoQL: TODO;
     * Cypher: TODO)
     **/
    public QueryExecutor( PolyphenyConnection polyconnection ) {
        this.polyconnection = polyconnection;
    }


    /**
     * @Description
     * - Executes the query depending on the language given by the user
     * 
     * @param language: The database language that is used (e.g. SQL, MongoQL,
     * Cypher)
     * @param query: The query-text to be executed (e.g. FROM emps SELECT *)
     * 
     * @return: ResultToMatlab(rs) which is a Matlab compatible object that is cast
     * to the Matlab user.
     **/
    public Object execute( String language, String query ) {

        polyconnection.openIfNeeded();
        switch ( language.toLowerCase() ) {
            default:
                System.err.println( "Unsupported language: " + language );
                return null;

            case "sql":
                try ( Statement stmt = polyconnection.getConnection().createStatement() ) {
                    String first = query.trim().toUpperCase();

                    if ( first.startsWith( "SELECT" ) ) {
                        try ( ResultSet rs = stmt.executeQuery( query ) ) {
                            return ResultToMatlab( rs );
                        }
                    } else {
                        stmt.executeUpdate( query );  // INSERT, UPDATE, DELETE, CREATE, DROP, ...
                        return null;
                    }
                } catch ( Exception e ) {
                    throw new RuntimeException( "SQL execution failed: " + e.getMessage(), e );
                }

            case "mongoql":
                throw new UnsupportedOperationException( "MongoQL execution not yet implemented." );

            case "cypher":
                throw new UnsupportedOperationException( "Cypher execution not yet implemented." );
        }
    }


    /**
     * @Description
     * - Casts the result of the queries to MatlabObjects, depending on
     * the Database language (SQL, MongoQL, Cypher)
     * 
     * @param rs: The result object of the query of type ResultSet
     * 
     * @return: Result from the query which is either null/scalar/table (SQL), TODO document (MongoQL)
     * or (Cypher)
     **/
    public Object ResultToMatlab( ResultSet rs ) throws Exception {

        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        Object Result;
        Object[][] ResultArray;

        // ─────────────────────────────
        // Case 1: Empty Result
        // ─────────────────────────────
        if ( !rs.next() ) {
            System.out.println( "Empty result set." );
            Result = null;
            return Result;
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
            System.err.println( "Fetched row: " + java.util.Arrays.toString( row ) );
        } while ( rs.next() ); // First row already fetched above with rs.next() so we use do while

        System.err.println( "Rows: " + java.util.Arrays.deepToString( rows.toArray() ) );
        // Ensure that the colNames and rows have the same number of columns
        if ( colNames.length != rows.get( 0 ).length ) {
            throw new RuntimeException( "Mismatch: colNames and rowData column count don't match" );
        }

        ResultArray = rows.toArray( new Object[rows.size()][] );

        return new Object[]{ colNames, ResultArray };
    }

}
