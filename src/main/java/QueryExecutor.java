import java.sql.*;
import com.mathworks.engine.MatlabEngine;

public class QueryExecutor{
    private PolyphenyConnection polyconnection;
    
    /*
    @Description
    - Constructor

    @param
    - polyconnection: PolyphenyConnection object that holds the connection details to the Database. It's used to execute queries
    @return
    - Object of the query (SQL: empty, scalar or table; MongoQL: TODO; Cypher: TODO)
    */
    public QueryExecutor(PolyphenyConnection polyconnection) {
        this.polyconnection = polyconnection;
    }


    /* 
    @Description
    - Executes the query depending on the language given by the user

    @param
    - language: The database language that is used (e.g. SQL, MongoQL, Cypher)
    - namespace: Name of the database namespace (e.g. emps, students)
    - query: The query-text to be executed (e.g. FROM emps SELECT *)
    @return
    - ResultToMatlab(rs) which is a Matlab compatible object that is cast to the Matlab user.
    */
    public Object execute(String language, String namespace, String query) {

        polyconnection.openIfNeeded();
        switch (language.toLowerCase()) {
            default:
                System.err.println("Unsupported language: " + language);
                return null;

            case "sql":
                try (Statement stmt = polyconnection.get_connection().createStatement();
                    ResultSet rs = stmt.executeQuery(query)) 
                    {
                        return ResultToMatlab(rs);
                    } catch (Exception e) {
                    System.err.println("SQL execution failed: " + e.getMessage());
                    return null;
                }

            case "mongoql":
                throw new UnsupportedOperationException("MongoQL execution not yet implemented.");

            case "cypher":
                throw new UnsupportedOperationException("Cypher execution not yet implemented.");
        }
    }

    /*
    @Description
    Casts the result of the queries to MatlabObjects, depending on the Databse language (SQL, MongoQL, Cypher)

    @param ResultSet rs: The result object of the query 
    @return engine.getVariable("T") which is either null/scalar/table (SQL), document (MongoQL) or TODO (Cypher)
    */
    public Object ResultToMatlab(ResultSet rs) throws Exception {

        MatlabEngine engine = polyconnection.get_MatlabEngine();
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // ─────────────────────────────
        // Case 1: Empty Result
        // ─────────────────────────────
        if (!rs.next()) {
            System.out.println("Empty result set.");
            engine.eval("T = table();");
            return engine.getVariable("T");
        }

        // ─────────────────────────────
        // Case 2: Scalar Result
        // ─────────────────────────────
        if (colCount == 1 && rs.isLast()) {
            Object scalar = rs.getObject(1);
            engine.putVariable("scalarResult", scalar);
            return engine.getVariable("scalarResult");
        }

        // ─────────────────────────────
        // Case 3: Tabular Result (≥1 column, ≥1 row)
        // ─────────────────────────────
        String[] columnNames = new String[colCount];
        for (int i = 1; i <= colCount; i++) {
            columnNames[i - 1] = meta.getColumnName(i);
        }

        engine.eval("T = table();");
        engine.putVariable("colNames", columnNames);
        engine.eval("T.Properties.VariableNames = colNames;");

        // First row already fetched above with rs.next()
        do {
            Object[] rowData = new Object[colCount];
            for (int i = 1; i <= colCount; i++) {
                rowData[i - 1] = rs.getObject(i);
            }
            engine.putVariable("rowData", rowData);
            engine.eval("T = [T; cell2table(rowData, 'VariableNames', colNames)];");
        } while (rs.next());

        return engine.getVariable("T");
}
}
