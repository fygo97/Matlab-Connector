classdef PolyphenyWrapperTestMQL < matlab.unittest.TestCase
    properties
        conn
    end

    methods(TestClassSetup)
        function setUpNamespaceAndCollection(testCase)
            clc;
            % open connection once
            testCase.conn = polypheny.Polypheny("localhost",20590,"pa","");

            % try create collection
            try
                testCase.conn.query("mongo","mongotest", ...
                    'db.createCollection("unittest_collection")');
            catch
            end
        end
    end

    methods(TestClassTeardown)
        function tearDownNamespaceAndCollection(testCase)
            try
                testCase.conn.query("mongo","mongotest", ...
                    'db.unittest_collection.drop()');
            catch
            end
            testCase.conn.close();
        end
    end

    methods(TestMethodSetup)
        function clearCollection(testCase)
            try
                testCase.conn.query("mongo","mongotest", ...
                    'db.unittest_collection.deleteMany({})');
            catch
            end
        end
    end

    methods(TestMethodTeardown)
        function clearCollectionAfter(testCase)
            try
                testCase.conn.query("mongo","mongotest", ...
                    'db.unittest_collection.deleteMany({})');
            catch
            end
        end
    end

    methods(Test)

        function testDeleteManyRemovesAllDocs(testCase)
            % Drop & recreate collection
            testCase.conn.query("mongo","mongotest", 'db.unittest_collection.drop()');
            testCase.conn.query("mongo","mongotest", 'db.createCollection("unittest_collection")');

            % Insert three documents
            testCase.conn.query("mongo","mongotest", 'db.unittest_collection.insertOne({"id":1,"name":"Alice"})');
            testCase.conn.query("mongo","mongotest", 'db.unittest_collection.insertOne({"id":2,"name":"Bob"})');
            testCase.conn.query("mongo","mongotest", 'db.unittest_collection.insertOne({"id":3,"name":"Ciri"})');

            % Call deleteMany({})
            ack = testCase.conn.query("mongo","mongotest", 'db.unittest_collection.deleteMany({})');
            disp("Ack from deleteMany:");
            disp(ack);

            % Verify collection is empty
            docs = testCase.conn.query("mongo","mongotest", 'db.unittest_collection.find({})');
            docs = jsondecode(docs);
            testCase.verifyEmpty(docs, "Collection should be empty after deleteMany({})");
        end

        function testInsertManyAndNestedDocument(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"age":14})');
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"age":20})');
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"age":24})');
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"age":30,"adress":{"Country":"Switzerland","Code":4051}})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({"age":{$gt:29}})');
            disp(docs)
            decoded = jsondecode(docs);
            disp(decoded)
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'"age":30'));
        end

        function testBooleanField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"flag":true})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp(docs)
            decoded = jsondecode(docs);
            disp(decoded)
            testCase.verifyTrue(contains(docs(1),'"flag":true'));
            testCase.verifyClass(decoded.flag, 'logical'); % asserts that class(decoded.flag) == logical
        end

        function testIntegerAgeField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"age":42})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'"age":42'));
        end

        function testStringField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"name":"Alice"})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'"name":"Alice"'));
        end

        function testLongField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"big":1111111111111111111})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'"big":1111111111111111111'));
        end

        function testDoubleField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"pi":3.14159})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'"pi":3.14159'));
        end

        function testInsertAndQueryTwoDocsRawJson(testCase)
            % Clean collection
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.drop()');
            testCase.conn.query("mongo","mongotest",'db.createCollection("unittest_collection")');
    
            % Insert two docs
            testCase.conn.query("mongo","mongotest", ...
                'db.unittest_collection.insertOne({"id":1,"name":"Alice"})');
            testCase.conn.query("mongo","mongotest", ...
                'db.unittest_collection.insertOne({"id":2,"name":"Bob"})');
    
            % Query back
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp("Raw JSON:");
            disp(docs);
            decoded = jsondecode(docs);
            disp(decoded)
    
            % Assert raw JSON is exactly what we want
            testCase.verifyTrue(contains(docs(1),'"name":"Alice"'));
            testCase.verifyTrue(contains(docs(1),'"name":"Bob"'));
        end


        function testCountDocuments(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"name":"Bob"})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.countDocuments({})');
            disp(docs)
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs(1),'{"count":1}'));
        end

        function testArrayField(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"scores":[1,2,3]})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp(docs)
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyTrue(contains(docs,'"scores":[1,2,3]'));
        end

        function testFindOnEmptyCollection(testCase)
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp(docs)
            testCase.verifyEqual(docs,"[]"); 
        end

        function testInsertManyAndFindMultiple(testCase)
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"id":10,"name":"A"})');
            testCase.conn.query("mongo","mongotest",'db.unittest_collection.insertOne({"id":11,"name":"B"})');
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp(docs)
            testCase.verifyTrue(contains(docs,'"id":10'))
            testCase.verifyTrue(contains(docs,'"name":"A"'))
            testCase.verifyTrue(contains(docs,'"id":11'))
            testCase.verifyTrue(contains(docs,'"name":"B"'))
        end

        function testBatchInsertAndFind(testCase)
            queries = { ...
                'db.unittest_collection.insertOne({"name":"Alice","age":25})', ...
                'db.unittest_collection.insertOne({"name":"Alice","age":20})', ...
                'db.unittest_collection.insertOne({"name":"Bob","age":30})' };
            ignore = testCase.conn.queryBatch("mongo","mongotest",queries);
            queries2 = { ...
                'db.unittest_collection.find({"name":"Alice"})', ...
                'db.unittest_collection.find({"name":"Alice","age":20})', ...
                'db.unittest_collection.find({"name":"Bob","age":30})' };
            docs = testCase.conn.queryBatch("mongo","mongotest", queries2);
            disp(docs)
            decoded = jsondecode(docs);
            disp(decoded)              
            testCase.verifyEqual(numel(decoded{1}), 2);        % 2 docs in first query
            
            % check names
            names = {decoded{1}.name};                        % cell of names
            disp(names)
            testCase.verifyEqual(string(names), ["Alice","Alice"]);

        end


        function testBatchMixedOps(testCase)
            queries = { ...
                'db.unittest_collection.insertOne({"name":"Charlie","active":true})', ...
                'db.unittest_collection.countDocuments({})' };
            docs = testCase.conn.queryBatch("mongo","mongotest",queries);
            testCase.verifyEqual(numel(docs),1);
            decoded = jsondecode(docs)
            varname = fieldnames(decoded{2})
            disp(decoded{2}.count)
            testCase.verifyTrue(decoded{2}.count==1);
        end

        function testSyntaxErrorThrows(testCase)
            badQuery = 'db.unittest_collection.insertOne({"foo":123)'; % invalid JSON
            testCase.verifyError(@() testCase.conn.query("mongo","mongotest",badQuery),?MException);
        end

        function testMultiStatementFails(testCase)
            badMulti = [ ...
                'db.people.insertOne({"name":"Alice","age":20}); ' ...
                'db.people.insertOne({"name":"Bob","age":24}); ' ...
                'db.people.find({})' ];
            testCase.verifyError(@() testCase.conn.query("mongo","mongotest",badMulti),?MException);
        end

        function testBatchRollback(testCase)
            queries = { ...
                'db.unittest_collection.insertOne({"id":1,"name":"Alice"})', ...
                'db.unittest_collection.insertOne({"id":2,"name":"Bob"})', ...
                'db.unittest_collection.insertOne({"id":3,"name":"Janice"' }; % broken
            testCase.verifyError(@() testCase.conn.queryBatch("mongo","mongotest",queries),?MException);
            docs = testCase.conn.query("mongo","mongotest",'db.unittest_collection.find({})');
            disp(docs)
            testCase.verifyEqual(numel(docs),1);
            testCase.verifyEqual(docs,"[]")
        end

    end
end
