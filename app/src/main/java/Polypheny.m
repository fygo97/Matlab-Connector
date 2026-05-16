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
                

                switch lower( language )
                    case "sql"
                        % Java returns: Object[] { String[] colNames, String[] typeNames, Object[] columns }
                        java_result = PolyWrapper.queryExecutor.executeSql( queryStr );
                        
                        if isempty( java_result )
                            matlab_result = table();
                            return;
                        end

                        % Unpack the "Heist" package
                        rawColNames = cell( java_result(1) ); % Raw column names directly from Java
                        colData  = cell( java_result(3) ); % This is an Object array of primitive arrays

                        if isempty( colData )
                            matlab_result = table();
                        else
                            % 1. Replace illegal characters and handle
                            % starting numbers using the makeValidName
                            % function
                            % e.g. ["id$", "id_"] -> ["id_", "id_"] 
                            cleanColNames = matlab.lang.makeValidName(rawColNames);
                            
                            % 2. De-duplicate name collisions e.g. ["id_", "id_"] -> ["id_1", "id_2"] 
                            cleanColNames = matlab.lang.makeUniqueStrings(cleanColNames, {}, namelengthmax);
                            
                            % Force cleanColNames to be a column cell array matching colData(:)
                            cleanColNames = cleanColNames(:); 
                            colDataVector = colData(:);

                            % Direct Table Construction via structures (The speed demon approach)
                            s = cell2struct( colDataVector, cleanColNames, 1 );
                            matlab_result = struct2table( s );
                        end

                    case "mongo"
                            java_result = PolyWrapper.queryExecutor.executeMongo( "mongo", namespace, queryStr );
                            matlab_result = string( java_result );

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
