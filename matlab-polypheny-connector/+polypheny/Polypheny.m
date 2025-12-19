classdef Polypheny < handle
% POLYPHENY MATLAB wrapper for the Polypheny Java connector. Wraps polyphenyconnector.PolyphenyConnection 
% and polyphenyconnector.QueryExecutor to run queries from MATLAB
    properties ( Access = private )
        polyConnection    % Java PolyphenyConnection
        queryExecutor     % Java QueryExecutor

    end
    
    methods
        
        function PolyWrapper = Polypheny( host, port, user, password )
            % Polypheny( LANGUAGE, HOST, PORT, USER, PASSWORD ): Set up Java connection + executor
            % LANGUAGE:  The database language ( 'sql', 'mongo', 'cypher' )
            % HOST:      Database host ( e.g. 'localhost' )
            % PORT:      Database port ( integer )
            % USER:      Username
            % PASSWORD:  Password
            
            % This makes sure that Matlab sees Java classes supplied by the .jar files in the Matlabtoolbox PolyphenyConnector.mtlbx
            try
                if ~polypheny.Polypheny.hasPolypheny( )
                    startup( );
                end
                PolyWrapper.polyConnection = javaObject( "polyphenyconnector.PolyphenyConnection",host, int32( port ), user, password );
                PolyWrapper.queryExecutor = javaObject( "polyphenyconnector.QueryExecutor", PolyWrapper.polyConnection );
            
            catch ME %Matlab Exception
                disp( "Error: " + ME.message )
            end
                
        end
        

        
        function matlab_result = query( PolyWrapper, language, namespace, queryStr )
            % query( POLYWRAPPER, QUERYSTR ): Execute query via QueryExecutor.java
            % POLYWRAPPER:             The PolyWrapper Matlab object
            % LANGUAGE:                The language of the query string -> SQL, mongo, Cypher
            % QUERYSTR:                The queryStr set by the user
            % @return matlab_result:   The result of the query -> return type differs for SQL,Mongo and Cypher


            try
                java_result = PolyWrapper.queryExecutor.execute( string( language ), string( namespace ), queryStr );

                switch lower( language )
                    case "sql"
                        if isempty( java_result )
                            matlab_result = [];
                        elseif isscalar( java_result )
                            matlab_result = java_result;
                        elseif isa( java_result,'java.lang.Object[]' ) && numel( java_result )==2
                            tmp = cell( java_result );
                            colNames = cell( tmp{1} );
                            data     = cell( tmp{2} );
                            matlab_result = cell2table( data, 'VariableNames', colNames );
                        else
                            matlab_result = [];
                        end

                    case "mongo"
                        if isa( java_result, 'java.util.List' )
                            % Current driver behavior: always returns List<String> of JSON docs
                            matlab_result = string(java_result);
                        elseif isnumeric( java_result )
                            % Not observed in current driver, but kept for forward compatibility
                            % (e.g. if Polypheny ever returns scalar counts directly)
                            matlab_result = java_result;
                        else
                            error( "Unexpected Mongo result type: %s", class( java_result ) );
                        end

                    case "cypher"
                        % TODO: integrate once Cypher executor is ready
                        error( "Cypher not supported yet." );

                    otherwise
                        error( "Unsupported language: %s", language );
                end

            catch ME
                error( "Query execution failed: %s", ME.message );
            end
        end

        function matlab_result = queryBatch( PolyWrapper, language, namespace, queryList )
            % queryBatch( POLYWRAPPER, QUERYLIST ): Execute batch of non-SELECT statements
            % QUERYLIST: A cell array of SQL strings ( INSERT, UPDATE, DELETE, etc. )
            %
            % Returns: int array with rows affected per statement

            if ~iscell( queryList )
                error( 'queryBatch expects a cell array of query strings' );
            end

            javaList = java.util.ArrayList();
            for i = 1:numel( queryList )
                javaList.add( string(queryList{i} ) );
            end

            switch lower(language)
                case "sql"
                    java_result = PolyWrapper.queryExecutor.executeBatchSql( javaList );
                    %matlab_result = double(java_result(:))';
                    vals = double(java_result(:));   % convert Java int[] to MATLAB column vector
                    matlab_result = array2table(vals, 'VariableNames', {'RowsAffected'});

                case "mongo"
                    java_result = PolyWrapper.queryExecutor.executeBatchMongo( string(namespace), javaList );
                    matlab_result = string( java_result ); % outer list

                case "cypher"
                    error( "Batch execution for Cypher not yet implemented." );

                otherwise
                    error( "Unsupported language: %s", language );
            end

        end
        
        function close( PolyWrapper )
            % close( POLYWRAPPER ): Close the Java connection
            % POLYWRAPPER: The PolyWrapper Matlab object
            PolyWrapper.polyConnection.close( );
        end
    end

    methods ( Static )
        function flag = hasPolypheny( )
            % HASPOLYPHENY Returns true if Polypheny Java classes are available because the exist( 'polyphenyconnector.PolyphenyConnection','class' )
            % returns 8 if Matlab sees the Java class and 0 otherwise.
            flag = ( exist( 'polyphenyconnector.PolyphenyConnection','class' ) == 8 );
        end

    end

end
