classdef Polypheny
    properties
        conn   % JDBC Connection
        opts   % defaults for mode, formats, etc.
        lastQueryStr   % store last query to avoid re-preparing unnecessarily
        lastStmt
    end

    methods
        function obj = Polypheny(url, user, pass, opts)
            % constructor: JDBC connect goes here
            % obj.conn = java.sql.DriverManager.getConnection(url, user, pass);
            obj.opts = opts;
            obj.lastQueryStr = "";
            obj.lastStmt = [];
        end

        function obj = close(obj)
            % JDBC close goes here
            if ~isempty(obj.conn)
                obj.conn.close();
            end
        end

        function stmt = prepareStatement(obj, queryStr)
            % Only prepare if query text has changed
            % if-clause is way cheaper than preparing the new statement
            if queryStr ~= obj.lastQueryStr
                stmt = obj.conn.prepareStatement(queryStr);
                obj.lastStmt = stmt;
                obj.lastQueryStr = queryStr;
            else
                stmt = obj.lastStmt;
            end
        end

        function stmt = bindParameters(~, stmt, params)
            % JDBC bind parameters
            for i = 1:numel(params)
                stmt.setObject(i, params{i});
            end
        end

        function rs = executeQuery(~, stmt)
            % Execute prepared statement
            % Re-preparing unnecessarily can be magnitudes slower
            rs = stmt.executeQuery();
        end

        function mode = lang2mode(~, language)
            language = lower(string(language));
            switch language
                case {"sql","postgres","ansi"}
                    mode = "relational";
                case {"mongo","mongoql","jsoniq"}
                    mode = "document";
                case {"cypher","gremlin","graph"}
                    mode = "graph";
                otherwise
                    mode = "relational"; % fallback
            end
        end

        function out = streamResults(~, rs, mode, varargin)
            % Route results to correct parser
            switch string(mode)
                case "relational"
                    out = streamRelational(rs);
                case "document"
                    out = streamDocument(rs, varargin{:});
                case "graph"
                    out = streamGraph(rs, varargin{:});
                otherwise
                    error("Unknown mode: %s", mode);
            end
        end

        function T = query(obj, language, queryStr, params, varargin)
            mode = obj.lang2mode(language);
            stmt = obj.prepareStatement(queryStr);
            stmt = obj.bindParameters(stmt, params);
            rs   = obj.executeQuery(stmt);
            T    = obj.streamResults(rs, mode, varargin{:});
        end
    end
end
