classdef PolyphenyWrapperTest < matlab.unittest.TestCase
    properties
        conn
    end
    
    methods (TestMethodSetup)
        function setupConnection(testCase)
            startup;
            testCase.conn = polypheny.Polypheny( ...
                'sql', 'localhost', int32(20590), 'pa', '' );
        end
    end
    
    methods (TestMethodTeardown)
        function closeConnection(testCase)
            testCase.conn.close();
        end
    end
    
    methods (Test)
        function testScalar(testCase)
            r = testCase.conn.query("SELECT 1 AS x");
            testCase.verifyEqual(r, 1);
        end
        
        function testTable(testCase)
            testCase.conn.query("DROP TABLE IF EXISTS wrapper_test");
            testCase.conn.query("CREATE TABLE wrapper_test (id INTEGER PRIMARY KEY, name VARCHAR)");
            testCase.conn.query("INSERT INTO wrapper_test VALUES (1,'Alice'),(2,'Bob')");

            T = testCase.conn.query("SELECT * FROM wrapper_test ORDER BY id");

            if istable(T)
                % Expected: table output with column "name"
                testCase.verifyEqual(T.name, {'Alice'; 'Bob'});
            elseif iscell(T)
                % Fallback: check the raw cell contents
                testCase.verifyEqual(T(:,2), {'Alice','Bob'}');
            else
                testCase.verifyFail("Unexpected return type: " + class(T));
            end
        end
        
        function testEmpty(testCase)
            T = testCase.conn.query("SELECT * FROM wrapper_test WHERE id=999");
            testCase.verifyEmpty(T);
        end
    end
end
