startup();

p = polypheny.Polypheny('sql', 'jdbc:polypheny://localhost:20590', 'pa', '');

T = p.query("SELECT 1 AS test_column");
disp(T);

p.close();
