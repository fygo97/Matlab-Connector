public class Main {
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
