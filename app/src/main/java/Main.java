public class Main {
        public static void main(String[] args) {
        try {
            String url = "jdbc:polypheny://localhost/public";
            String user = "pa";
            String pass = "";

            PolyphenyConnection conn = new PolyphenyConnection(url, user, pass);
            QueryExecutor executor = new QueryExecutor(conn);
            executor.execute("sql", "SELECT * FROM emps;");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
