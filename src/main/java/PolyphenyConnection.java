import java.sql.*;
import java.net.Socket;
import com.mathworks.engine.MatlabEngine;

public class PolyphenyConnection {
    private Connection connection;
    private MatlabEngine matlabEngine; 
    private final String url, username, password;

    /*
    @Description
    - Constructor supporting lazy-open: Stores logins; connects on first use to protect server resources

    @params:
    - url: the url of the database
    - username: username to access the database with
    - password: password to the corresponding username

    @return
    - Creates a PolyphenyConnection object that can be passed to the ExecuteQuery class and used to run queries
    */
    public PolyphenyConnection(String url, String username, String password) throws Exception {
        this.url = url; 
        this.username = username; 
        this.password = password; 
        this.connection = null;   //The connection is established later on when needed. Lazy open prevents accidental resource leaks induced by user
        this.matlabEngine = null; //The Matlab engine is needed for the typecasting in ExecuteQuery later, but started here to avoid overhead

        StartLocalPolypheny();    //Starts Polypheny locally on the machine if it's not already running
        StartMatlabEngine();      //Starts MatlabEngine to handle the query results. Every connection will each create their own MatlabEngine
    }


    /*  
    @Description
    - Checks if Polypheny is running locally and starts it if not

    @param -
    @return
    - Boolean true or Exception: because Polypheny is either running in the end or was started.
    */

    private boolean isPolyphenyRunning() {
        try (Socket socket = new Socket("localhost", 20590)) {
            return true; // Able to connect -> running, Otherwise an Exception will be thrown later
        } catch (Exception e) {
            return false;
        }
    }


    /*
    @REQUIREMENTS
    - polypheny.jar in lib folder

    @Description
    - Starts the Polypheny application on the local machine

    @param -
    @return -
     */
    public void StartLocalPolypheny() {
        try {
            if (!isPolyphenyRunning()) {
                System.out.println("Polypheny not running. Attempting to start...");
                ProcessBuilder pb = new ProcessBuilder("java", "-jar", "lib/polypheny.jar");
                pb.inheritIO();
                pb.start();

                // Wait and retry a few times
                int retries = 5;
                int delay = 2000; // 2 seconds
                while (retries-- > 0 && !isPolyphenyRunning()) {
                    System.out.println("Waiting for Polypheny to start...");
                    Thread.sleep(delay);
                }

                if (!isPolyphenyRunning()) {
                    System.err.println("Polypheny did not start in time.");
                }
            } else {
                System.out.println("Polypheny is already running.");
            }
        } catch (Exception e) {
            System.err.println("Could not start Polypheny: " + e.getMessage());
        }
    }


    /*
    @Description
    - Starts the matlabEngine of the PolyphenyConnection class

    @param -
    @return -
     */
    public void StartMatlabEngine() throws Exception {
        if (matlabEngine == null) {
            matlabEngine = MatlabEngine.startMatlab();
            System.out.println("Shared MATLAB engine started.");
        }
    }

    /*
    @Description
    - Stops the matlabEngine of the PolyphenyConnection class

    @param -
    @return -
     */
    public void StopMatlabEngine() throws Exception {
        if (matlabEngine != null) {
            matlabEngine.close();
            matlabEngine = null;
            System.out.println("Shared MATLAB engine stopped.");
        }
    }

    /*
    @Description
    - Opens the server connection to Polypheny if needed (reuse otherwise). Checking the if-clause in java is a lot faster, than iterative
      opening and closing of the connection after every use for large numbers of queries, as it eliminates the ~10ms matlab-java crossover
      that opening and closing a connection from matlab would create. For 1M queries that avoids 1M * 10ms = ~10 000 sec = 2.8 hrs of overhead.

    @param -
    @return -
     */
    public void openIfNeeded() {
        if (connection == null) {
            try {
                Class.forName("org.polypheny.jdbc.PolyphenyDriver"); // load driver
                connection = DriverManager.getConnection(url, username, password); //establish connection
            } catch (Exception e) {
                throw new RuntimeException("Failed to open connection", e);
            }
        }
    }

    /*
    @Description
    - Closes connection if open

    @param -
    @return -
    */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("Failed to close connection: " + e.getMessage());
        }

        try {
            if (matlabEngine!=null) matlabEngine.close();
        } catch(Exception e){
            System.err.println("Failed to close MATLAB: " + e.getMessage());
        }
    }

    /*
    @Description
    - Getter function for the connection variable of PolyphenyConnection 
    
    @param -
    @return Connection connection variable of the PolyphenyConnection class
    */
    public Connection get_connection() {
        return this.connection;
    }

    /* 
    @Description
    - Setter function for the connection variable

    @param input_connection: The connection we want to set your PolyphenyConnection object to.
    @return -
     */
    public void set_connection(Connection input_connection){
        this.connection = input_connection;
    }

    /* 
    @Description
    - Getter function for the MatlabEngine of the PolyphenyConnection object

    @param -
    @return MatlabEngine matlabEngine variable of the PolyphenyConnection class
    */
    public MatlabEngine get_MatlabEngine() {
        return this.matlabEngine;
    }




    public static void main(String[] args) {
        try {
            String url = "jdbc:polypheny://localhost/public";
            String user = "pa";
            String pass = "";

            PolyphenyConnection conn = new PolyphenyConnection(url, user, pass);
            QueryExecutor executor = new QueryExecutor(conn);
            executor.execute("sql", "public", "SELECT * FROM emps;");

            conn.get_MatlabEngine().eval("disp(head(T,5));");
            conn.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
}


}
