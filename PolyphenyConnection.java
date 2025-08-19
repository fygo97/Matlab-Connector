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


    // Checks if Polypheny is running locally and starts it if not
    private boolean isPolyphenyRunning() throws Exception{
        try (Socket socket = new Socket("localhost", 20590)) {
            return true; // Able to connect -> running, Otherwise an Exception will be thrown later
        }
    }


    /*
    @REQUIREMENTS:
    - polypheny.jar in lib folder

    Starts the Polypheny application on the local machine
     */
    public void StartLocalPolypheny() throws Exception{
            // If the connection isn't open locally start the Polypheny application locally.
            if (!isPolyphenyRunning()) {
                ProcessBuilder pb = new ProcessBuilder("java", "-jar", "lib/polypheny.jar");
                pb.inheritIO(); // show output
                pb.start();
                Thread.sleep(4000); // wait for Polypheny to boot
                } 
            // If the connection is open do nothing
            else {System.err.println("Polypheny already running on local system");
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
    Open the server connection to Polypheny if needed (reuse otherwise). Checking the if-clause in java is a lot faster for large numbers of 
    queries as it eliminates the ~10ms matlab-java crossover that opening and closing a connection from matlab would create. For 1M queries
    that avoids 1M * 10ms = ~10 000 sec = 2.8 hrs of overhead.

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
    Closes connection if open

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
    Getter for the connection variable of PolyphenyConnection 
    
    @param -
    @return Connection connection of the PolyphenyConnection class
    */
    public Connection get_connection() {
        return this.connection;
    }

    // Setter function for the connection variable
    public void set_connection(Connection input_connection){
        this.connection = input_connection;
    }

    // Getter function for the connection variable
    public MatlabEngine get_MatlabEngine() {
        return this.matlabEngine;
    }


}

/*
public static void main(String[] args) {
    try {
        String url = "jdbc:polypheny://localhost/public";
        String user = "pa";
        String pass = "";

        PolyphenyConnection conn = new PolyphenyConnection(url, user, pass);
        Object result = conn.execute("sql", "public", "SELECT * FROM emps;");

        conn.matlab.eval("disp(head(T,5));");
        conn.close();
        
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
 */
