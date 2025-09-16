classdef Polypheny < handle
% POLYPHENY MATLAB wrapper for the Polypheny Java connector. Wraps polyphenyconnector.PolyphenyConnection 
% and polyphenyconnector.QueryExecutor to run queries from MATLAB
    properties (Access = private)
        polyConnection    % Java PolyphenyConnection
        queryExecutor     % Java QueryExecutor

    end
    
    methods

        
        function PolyWrapper = Polypheny( host, port, user, password )
            % Polypheny( LANGUAGE, HOST, PORT, USER, PASSWORD ): Set up Java connection + executor
            % LANGUAGE:  The database language ('sql', 'mongoql', 'cypher')
            % HOST:      Database host (e.g. 'localhost')
            % PORT:      Database port (integer)
            % USER:      Username
            % PASSWORD:  Password
            
            PolyWrapper.polyConnection = javaObject("polyphenyconnector.PolyphenyConnection",host, int32(port), user, password );
            PolyWrapper.queryExecutor = javaObject("polyphenyconnector.QueryExecutor", PolyWrapper.polyConnection );
        end
        

        
        function T = query( PolyWrapper, language, queryStr )
            % query( POLYWRAPPER, QUERYSTR ): Execute query via QueryExecutor.java
            % POLYWRAPPER: The PolyWrapper Matlab object
            % LANGUAGE:    The language of the query string -> SQL, MongoQL, Cypher
            % QUERYSTR:    The queryStr set by the user
            % @return T:   The result of the query -> return type differs for SQL,MongoQl and Cyper
            r = PolyWrapper.queryExecutor.execute(string(language), queryStr );
            
            if isempty( r )
                T = [];

            elseif isscalar( r )
                T = r;

            elseif isa(r,'java.lang.Object[]') && numel(r) == 2
                r = cell(r);
                colNames = cell(r{1});
                data     = cell(r{2});   % Object[][] → MATLAB cell
                T = cell2table(data, 'VariableNames', colNames);

            else
                T = []; % fallback to avoid cell2table crash
            end
        end

        function result = queryBatch( PolyWrapper, language, queryList )
            % queryBatch( POLYWRAPPER, QUERYLIST ): Execute batch of non-SELECT statements
            % QUERYLIST must be a cell array of SQL strings (INSERT, UPDATE, DELETE, etc.)
            %
            % Returns: int array with rows affected per statement

            if ~iscell( queryList ) % cell is the Matlab List type
                error( 'queryBatch expects a cell array of SQL strings' );
            end
            
            javaList = java.util.ArrayList();
            for i = 1:numel(queryList)
                javaList.add( string(queryList{i}) );
            end
            java_result = PolyWrapper.queryExecutor.executeBatch( string(language), javaList );
            result = double(java_result(:))';
        end
        
        function close( PolyWrapper )
            % close( POLYWRAPPER ): Close the Java connection
            % POLYWRAPPER: The PolyWrapper Matlab object
            PolyWrapper.polyConnection.close();
        end
    end
end
