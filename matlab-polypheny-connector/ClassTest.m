clear all; clear classes
%conn = javaObject('polyphenyconnector.PolyphenyConnection', 'localhost', int32(20590), 'pa', '');
%exec = javaObject('polyphenyconnector.QueryExecutor', conn);

%res = exec.execute('sql', 'SELECT 1 AS x');
results = runtests('PolyphenyWrapperTest');
disp(results)

%disp(res);
