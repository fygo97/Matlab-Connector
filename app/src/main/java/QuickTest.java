import java.util.Arrays;

import polyphenyconnector.PolyphenyConnection;
import polyphenyconnector.QueryExecutor;

public class QuickTest {

    public static void main( String[] args ) throws Exception {
        PolyphenyConnection conn = new PolyphenyConnection( "localhost", 20590, "pa", "" );
        try {
            QueryExecutor exec = new QueryExecutor( conn );

            // 1) Scalar smoke test
            Object r1 = exec.execute( "sql", "SELECT 1 AS x" );
            System.out.println( "Scalar result: " + r1 );

            // 2) Table smoke test
            Object r2 = exec.execute( "sql", "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4" );
            printTable( r2 );

            // 3) First row from emps (1-row table)
            Object r3 = exec.execute( "sql", "SELECT * FROM emps LIMIT 1" );
            System.out.println( "First row from emps:" );
            printTable( r3 );

            // 4) Scalar from emps
            Object r4 = exec.execute( "sql", "SELECT empid FROM emps LIMIT 1" );
            System.out.println( "First empid (scalar): " + r4 );

        } finally {
            conn.close();
            System.exit( 0 );
        }
    }


    static void printTable( Object r ) {
        if ( r instanceof Object[] ) {
            Object[] t = (Object[]) r; // { colNames, data }
            String[] cols = (String[]) t[0];
            Object[][] data = (Object[][]) t[1];
            System.out.println( "Cols: " + Arrays.toString( cols ) );
            for ( Object[] row : data ) {
                System.out.println( Arrays.toString( row ) );
            }
        } else {
            System.out.println( String.valueOf( r ) );
        }
    }

}
