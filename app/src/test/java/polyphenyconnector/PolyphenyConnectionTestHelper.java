package polyphenyconnector;

import java.sql.DriverManager;
import java.sql.SQLException;

public class PolyphenyConnectionTestHelper {

    static void waitForPolypheny() throws Exception {
        String url = "jdbc:polypheny://localhost:20590";
        String user = "pa";
        String pass = "";

        long deadline = System.currentTimeMillis() + 7000; // 7s timeout
        int attempt = 1;
        boolean ready = false;

        while ( System.currentTimeMillis() < deadline ) {
            try ( java.sql.Connection conn = DriverManager.getConnection( url, user, pass ) ) {
                if ( conn != null && !conn.isClosed() ) {
                    ready = true;
                    break;
                }
            } catch ( Exception e ) {
                System.out.println( "Polypheny not ready (attempt " + attempt + ")" );
            }
            attempt++;
            Thread.sleep( 1000 ); // wait 1s before retry
        }

        if ( !ready ) {
            throw new RuntimeException( "Polypheny did not become available within 7 seconds." );
        }
    }


    public static void ensurePostgresAdapter( PolyphenyConnection conn ) throws SQLException {
        QueryExecutor exec = new QueryExecutor( conn );
        exec.execute( "sql", "CREATE ADAPTER IF NOT EXISTS postgresql1 USING postgresql ..." );
    }

}
