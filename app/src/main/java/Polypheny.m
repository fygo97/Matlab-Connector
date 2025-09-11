classdef Polypheny < handle
% POLYPHENY MATLAB wrapper for the Polypheny Java connector. Wraps polyphenyconnector.PolyphenyConnection 
% and QueryExecutor to run queries from MATLAB
    properties (Access = private)
        language          % 'sql' | 'mongoql' | 'cypher'
        polyConnection    % Java PolyphenyConnection
        queryExecutor     % Java QueryExecutor

    end
    
    methods

        %  
        function PolyWrapper = Polypheny(language, url, user, password )
            % Polypheny( LANGUAGE, URL, USER, PASSWORD ): Set up Java connection + executor
            % LANGUAGE:  The database language used for the query. Must be sql, mongoql or cypher.
            % URL:       The database url
            % USER:      The username
            % PASSWORD:  The password
            % Polywrapper: A Matlab object that is a wrapper for the two java classes
            %              QueryExecutioner.java and PolyphenyConnection.java 
            PolyWrapper.language = language;
            PolyWrapper.polyConnection = javaObject( "polyphenyconnector.PolyphenyConnection", url, user, password );
            PolyWrapper.queryExecutor = javaObject( "polyphenyconnector.QueryExecutor", PolyWrapper.polyConnection );
        end
        

        
        function T = query( PolyWrapper, queryStr )
            % query( POLYWRAPPER, QUERYSTR ): Execute query via QueryExecutor.java
            % POLYWRAPPER: The PolyWrapper Matlab object
            % QUERYSTR:    The queryStr set by the user
            % @return T:   The result of the query -> return type differs for SQL,MongoQl and Cyper
            r = PolyWrapper.queryExecutor.execute(string(PolyWrapper.language), queryStr );
            
            if isempty( r )
                T = [];

            elseif isscalar( r )
                T = r;

            else
                % Expect Java side to return { colNames, data }
                colNames = cell( r(1) );
                data     = r(2);
                T = cell2table( data, 'VariableNames', colNames );
            end
        end
        
        function close( PolyWrapper )
            % close( POLYWRAPPER ): Close the Java connection
            % POLYWRAPPER: The PolyWrapper Matlab object
            PolyWrapper.polyConnection.close();
        end
    end
end
