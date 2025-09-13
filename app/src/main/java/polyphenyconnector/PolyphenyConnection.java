package polyphenyconnector;

import java.sql.*;

public class PolyphenyConnection {

    private Connection connection;
    private final String host, url, username, password;
    private final int port;


    /**
     * @Description
     * - Constructor supporting lazy-open: Stores logins; connects on first use to protect server resources.
     * 
     * @param host: the host that should be used for the connection
     * @param port: the port that should be used for the connection
     * @param username: username to access the database with
     * @param password: password to the corresponding username
     * 
     **/
    public PolyphenyConnection( String host, int port, String username, String password ) {
        this.host = host;
        this.port = port;
        this.url = "jdbc:polypheny://" + host + ":" + port;
        this.username = username;
        this.password = password;
        this.connection = null; // The connection is established later on when needed. Lazy open
                               //  prevents accidental resource leaks induced by user
    }


    /**
     * @Description
     * - Opens the server connection to Polypheny if needed (reuse otherwise). Checking the
     * if-clause in java is a lot faster, than iterative opening and closing of the connection after every
     * use for large numbers of queries, as it eliminates the ~10ms matlab-java crossover that opening and
     * closing a connection from matlab would create. For 1M queries that avoids 1M*10ms = ~10 000sec=2.8 hrs
     * of overhead.
     * 
     **/
    public void openIfNeeded() {
        if ( connection == null ) {
            try {
                connection = DriverManager.getConnection( url, username, password );  // runs startLocalPolypheny if connection isn't responsive
                connection.setAutoCommit( true );  // make sure standard mode defaults to AutoCommit                       
            } catch ( SQLException e ) {
                throw new RuntimeException( "Failed to open connection", e );
            }
        }
    }


    /**
     * @Description
     * - Getter function for the host
     * @return host The host passed to the PolyphenyConnection object.
     */
    public String getHost() {
        return host;
    }


    /**
     * @Description
     * - Getter function for the port
     * @return port The port passed to the PolyphenyConnection object.
     */
    public int getPort() {
        return port;
    }


    /**
     * @Description
     * - Closes connection if open
     * 
     **/
    public void close() {
        try {
            if ( connection != null && !connection.isClosed() ) {
                connection.close();
            }
        } catch ( SQLException e ) {
            System.err.println( "Failed to close connection: " + e.getMessage() );
        } finally {
            connection = null;
        }
    }


    /**
     * @Description
     * - Getter function for the connection variable of PolyphenyConnection
     * 
     * @return
     * - Connection connection variable of the PolyphenyConnection class
     **/
    public Connection getConnection() {
        return this.connection;
    }


    /**
     * @Description
     * - Begins Database transaction. This is necessary to expose here because we need it to control flow in
     * Batch queries handled in the QueryExecutor class later.
     * 
     * @throws SQLException
     */
    public void beginTransaction() throws SQLException {
        openIfNeeded();
        connection.setAutoCommit( false );
    }


    /**
     * @Description
     * - Commits Database transaction. This is necessary to expose here because we need it to control flow in
     * Batch queries handled in the QueryExecutor class later.
     * 
     * @throws SQLException
     */
    public void commitTransaction() throws SQLException {
        connection.commit();
        connection.setAutoCommit( true );

    }


    /**
     * @Description
     * - Rolls back Database transaction. This is necessary to expose here because we need it to control flow in
     * Batch queries handled in the QueryExecutor class later.
     * 
     * @throws SQLException
     */
    public void rollbackTransaction() throws SQLException {
        connection.rollback();
        connection.setAutoCommit( true );
    }

}
