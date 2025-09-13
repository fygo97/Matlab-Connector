import polyphenyconnector.PolyphenyConnection;
import polyphenyconnector.QueryExecutor;

public class Main {

    public static void main( String[] args ) {
        try {
            String host = "localhost";
            int port = 205090;
            String user = "pa";
            String pass = "";

            PolyphenyConnection conn = new PolyphenyConnection( host, port, user, pass );
            QueryExecutor executor = new QueryExecutor( conn );
            executor.execute( "sql", "SELECT * FROM emps;" );
            conn.close();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }

}
