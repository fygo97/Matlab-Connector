package polyphenyconnector;

import java.sql.SQLException;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class PolyphenyConnectionTest {

    private PolyphenyConnection conn;
    private String host, username, password;
    private int port;


    @BeforeAll
    static void waitForPolypheny() throws Exception {
        PolyphenyConnectionTestHelper.waitForPolypheny();
    }


    @BeforeEach
    void setUp() {
        host = "localhost";
        port = 20590;
        username = "pa";
        password = "";
        conn = new PolyphenyConnection( host, port, username, password );
    }


    @AfterEach
    void tearDown() {
        conn.close();
    }


    @Test
    void getHostPort() {
        assertEquals( port, conn.getPort(), "Port must be 20590." );
        assertEquals( host, conn.getHost(), "Host must be localhost." );

    }


    @Test
    void testLazyOpen() {
        assertNull( conn.getConnection(), "Connection should start as null" );
        conn.openIfNeeded();
        assertNotNull( conn.getConnection(), "Connection should be established after openIfNeeded()" );
    }


    @Test
    void testBeginCommitRollback() throws SQLException {
        conn.openIfNeeded();

        // Test AutoCommit is enabled in the standard 1-Batch query case. This is important so myQueryExecutor.execute(...) can AutoCommit
        assertTrue( conn.getConnection().getAutoCommit(), "AutoCommit should be true in the standard 1-Batch query case." );

        // Test beginTransaction() actually disables the AutoCommit setting in openIfNeeded(). Important for its use in N-Batching.
        conn.beginTransaction();
        assertFalse( conn.getConnection().getAutoCommit(), "AutoCommit should be false in transaction" );

        // Test that AutoCommit is set to true again after commitTransaction() executes, so the standard 1-Batch query case can AutoCommit.
        conn.commitTransaction();
        assertTrue( conn.getConnection().getAutoCommit(), "AutoCommit should be true after commit" );

        // Test that after the rollbackTransaction() AutoCommit is true again, so the standard 1-Batch query case can AutoCommit.
        conn.beginTransaction();
        conn.rollbackTransaction();
        assertTrue( conn.getConnection().getAutoCommit(), "AutoCommit should be true after rollback" );
    }


    @Test
    void testOpen() {

        // Test that openIfNeeded() twice doesn't change the existing connection.
        conn.openIfNeeded();
        java.sql.Connection firstConnection = conn.getConnection();
        conn.close();
        conn.openIfNeeded();
        java.sql.Connection secondConnection = conn.getConnection();
        assertNotSame( firstConnection, secondConnection, "A new connection should be created after close()" );

        // Test executing openIfNeeded() twice doesn't throw an Exception
        assertDoesNotThrow( () -> conn.openIfNeeded(), "Opening twice should not throw an exception" );

    }


    @Test
    void testClose() {
        // Test that opening twice doesn't throw an exception

        conn.close();
        assertDoesNotThrow( () -> conn.close(), "Closing twice should not throw an exception" );
        assertNull( conn.getConnection(), "Connection should be null after close" );
    }


    @Test
    void testOpenWithInvalidCredentials() {

        // Tests that a RuntimeException is thrown. Makes sure false Username and Password are treated with an Exception
        PolyphenyConnection badConn = new PolyphenyConnection( host, port, "wronguser", "wrongpass" );
        assertThrows( RuntimeException.class, badConn::openIfNeeded, "Opening with bad credentials should fail" );
    }

}
