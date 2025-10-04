classdef PolyphenyWrapperTestSQL < matlab.unittest.TestCase
    properties
        conn   % polypheny.Polypheny wrapper
    end

    methods(TestClassSetup)
        function setUpNamespaceAndTable(testCase)
            % Open connection once for all tests
            testCase.conn = polypheny.Polypheny("localhost",20590,"pa","");

            % Drop leftovers if they exist
            try
                testCase.conn.query("sql","unittest_namespace", ...
                    "DROP TABLE IF EXISTS unittest_namespace.unittest_table");
                testCase.conn.query("sql","unittest_namespace", ...
                    "DROP NAMESPACE IF EXISTS unittest_namespace");
            catch
            end

            % Create namespace + table for execute()
            testCase.conn.query("sql","", ...
                "CREATE NAMESPACE unittest_namespace");
            testCase.conn.query("sql","unittest_namespace", ...
                "CREATE TABLE unittest_namespace.unittest_table (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY(id))");

            % Drop and recreate batch_table
            try
                testCase.conn.query("sql","unittest_namespace", ...
                    "DROP TABLE IF EXISTS unittest_namespace.batch_table");
            catch
            end
            testCase.conn.query("sql","unittest_namespace", ...
                "CREATE TABLE unittest_namespace.batch_table (" + ...
                "emp_id INT NOT NULL, " + ...
                "name VARCHAR(100), " + ...
                "gender VARCHAR(10), " + ...
                "birthday DATE, " + ...
                "employee_id INT, " + ...
                "PRIMARY KEY(emp_id))");
        end
    end

    methods(TestClassTeardown)
        function tearDownNamespaceAndTable(testCase)
            % Cleanup after all tests
            try
                testCase.conn.query("sql","unittest_namespace", ...
                    "DROP TABLE IF EXISTS unittest_namespace.unittest_table");
                testCase.conn.query("sql","unittest_namespace", ...
                    "DROP TABLE IF EXISTS unittest_namespace.batch_table");
                testCase.conn.query("sql","", ...
                    "DROP NAMESPACE IF EXISTS unittest_namespace");
            catch
            end
            testCase.conn.close();
        end
    end

    methods(TestMethodSetup)
        function clearTables(testCase)
            % Clear before each test
            try
                testCase.conn.query("sql","unittest_namespace", ...
                    "DELETE FROM unittest_namespace.unittest_table");
                testCase.conn.query("sql","unittest_namespace", ...
                    "DELETE FROM unittest_namespace.batch_table");
            catch
            end
        end
    end

    methods(TestMethodTeardown)
        function clearTablesAfter(testCase)
            % Clear again after each test
            try
                testCase.conn.query("sql","unittest_namespace", ...
                    "DELETE FROM unittest_namespace.unittest_table");
                testCase.conn.query("sql","unittest_namespace", ...
                    "DELETE FROM unittest_namespace.batch_table");
            catch
            end
        end
    end

    methods(Test)
        function testScalarLiteral(testCase)
            r = testCase.conn.query("sql","","SELECT 42 AS answer");
            testCase.verifyEqual(r,42);
        end

        function testEmptyLiteral(testCase)
            r = testCase.conn.query("sql","","SELECT * FROM (SELECT 1) t WHERE 1=0");
            testCase.verifyEmpty(r);
        end

        function testTableLiteral(testCase)
            r = testCase.conn.query("sql","unittest_namespace","SELECT 1 AS a, 2 AS b UNION ALL SELECT 3,4");
            testCase.verifyTrue(istable(r));
            testCase.verifyEqual(r.Properties.VariableNames,{'a','b'});
            testCase.verifyEqual(height(r),2);
            testCase.verifyEqual(table2cell(r(1,:)),{1,2});
            testCase.verifyEqual(table2cell(r(2,:)),{3,4});
        end

        function testInsert(testCase)
            r = testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (1,'Alice')");
            testCase.verifyEqual(r,1);
        end

        function testInsertAndSelect(testCase)
            testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (1,'Alice')");
            r = testCase.conn.query("sql","unittest_namespace","SELECT id,name FROM unittest_namespace.unittest_table");
            testCase.verifyTrue(istable(r));
            testCase.verifyEqual(r.Properties.VariableNames,{'id','name'});
            testCase.verifyEqual(height(r),1);
            testCase.verifyEqual(table2cell(r),{1,'Alice'});
        end

        function testScalarFromTable(testCase)
            testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (2,'Carol')");
            r = testCase.conn.query("sql","unittest_namespace","SELECT id FROM unittest_namespace.unittest_table WHERE name='Carol'");
            testCase.verifyEqual(r,2);
        end

        function testInsertAndSelectMultipleRows(testCase)
            testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (1,'Alice')");
            testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (2,'Bob')");
            r = testCase.conn.query("sql","unittest_namespace","SELECT id,name FROM unittest_namespace.unittest_table ORDER BY id");
            testCase.verifyTrue(istable(r));
            testCase.verifyEqual(height(r),2);
            testCase.verifyEqual(table2cell(r(1,:)),{1,'Alice'});
            testCase.verifyEqual(table2cell(r(2,:)),{2,'Bob'});
        end

        function testDeleteFromTable(testCase)
            testCase.conn.query("sql","unittest_namespace","INSERT INTO unittest_namespace.unittest_table VALUES (2,'Bob')");
            testCase.conn.query("sql","unittest_namespace","DELETE FROM unittest_namespace.unittest_table");
            r = testCase.conn.query("sql","unittest_namespace","SELECT * FROM unittest_namespace.unittest_table");
            testCase.verifyEmpty(r);
        end

        function testBatchInsertEmployees(testCase)
            queries = {
                "INSERT INTO unittest_namespace.batch_table VALUES (1,'Alice','F',DATE '1990-01-15',1001)"
                "INSERT INTO unittest_namespace.batch_table VALUES (2,'Bob','M',DATE '1989-05-12',1002)"
                "INSERT INTO unittest_namespace.batch_table VALUES (3,'Jane','F',DATE '1992-07-23',1003)"
                "INSERT INTO unittest_namespace.batch_table VALUES (4,'Tim','M',DATE '1991-03-03',1004)"
                "INSERT INTO unittest_namespace.batch_table VALUES (5,'Alex','M',DATE '1994-11-11',1005)"
                "INSERT INTO unittest_namespace.batch_table VALUES (6,'Mason','M',DATE '1988-04-22',1006)"
                "INSERT INTO unittest_namespace.batch_table VALUES (7,'Rena','F',DATE '1995-06-17',1007)"
                "INSERT INTO unittest_namespace.batch_table VALUES (8,'Christopher','M',DATE '1987-08-09',1008)"
                "INSERT INTO unittest_namespace.batch_table VALUES (9,'Lexi','F',DATE '1996-09-30',1009)"
                "INSERT INTO unittest_namespace.batch_table VALUES (10,'Baen','M',DATE '1990-10-05',1010)"
                "INSERT INTO unittest_namespace.batch_table VALUES (11,'Ricardo','M',DATE '1986-12-12',1011)"
                "INSERT INTO unittest_namespace.batch_table VALUES (12,'Tim','M',DATE '1993-02-02',1012)"
                "INSERT INTO unittest_namespace.batch_table VALUES (13,'Beya','F',DATE '1994-05-25',1013)"
                };
            counts = testCase.conn.queryBatch("sql","unittest_namespace",queries);
            testCase.verifyEqual(height(counts),13);
            disp(counts)
            testCase.verifyTrue(all(counts.RowsAffected == 1));
            r = testCase.conn.query("sql","unittest_namespace","SELECT COUNT(*) FROM unittest_namespace.batch_table");
            testCase.verifyEqual(r,13);
        end

        function testBatchRollbackOnFailure(testCase)
            queries = {
                "INSERT INTO unittest_namespace.batch_table VALUES (1,'Alice','F',DATE '1990-01-15',1001)"
                "BROKEN QUERY"
                };
            testCase.verifyError(@() testCase.conn.queryBatch("sql","unittest_namespace",queries),?MException);
            r = testCase.conn.query("sql","unittest_namespace","SELECT * FROM unittest_namespace.batch_table");
            testCase.verifyEmpty(r);
        end

        function testSyntaxError(testCase)
            testCase.verifyError(@() testCase.conn.query("sql","unittest_namespace","SELEC WRONG FROM nowhere"),?MException);
        end


        function testQueryWithSpaces(testCase)
            % Insert with leading spaces
            testCase.conn.query("sql","unittest_namespace", ...
                "  INSERT INTO unittest_namespace.unittest_table VALUES (1,'Alice')");
            testCase.conn.query("sql","unittest_namespace", ...
                " INSERT INTO unittest_namespace.unittest_table VALUES (2,'Bob')");
            
            r = testCase.conn.query("sql","unittest_namespace", ...
                "SELECT id,name FROM unittest_namespace.unittest_table ORDER BY id");
            
            testCase.verifyTrue(istable(r));
            testCase.verifyEqual(r.Properties.VariableNames,{'id','name'});
            testCase.verifyEqual(height(r),2);
            testCase.verifyEqual(table2cell(r(1,:)),{1,'Alice'});
            testCase.verifyEqual(table2cell(r(2,:)),{2,'Bob'});
        end
        
        function testConnectionFailure(testCase)
            testCase.verifyError(@() ...
                polypheny.Polypheny("localhost",9999,"pa","").query("sql","unittest_namespace","SELECT 1"), ...
                ?MException);
        end
        
        function testCommitFailureRollback(testCase)
            queries = {
                "INSERT INTO unittest_namespace.batch_table VALUES (1,'Alice','F',DATE '1990-01-15',1001)"
                "Intentional nonsense to produce a failure"
                };
            testCase.verifyError(@() ...
                testCase.conn.queryBatch("sql","unittest_namespace",queries),?MException);
        
            r = testCase.conn.query("sql","unittest_namespace", ...
                "SELECT * FROM unittest_namespace.batch_table");
            testCase.verifyEmpty(r);
        end
        
    end
    
end