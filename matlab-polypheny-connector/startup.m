function startup
    % Get root folder of the toolbox
    root = fileparts(mfilename('fullpath'));

    % Paths to JARs
    jarPaths = { ...
        fullfile(root, 'jar',  'polypheny-all.jar'), ...
        fullfile(root, 'libs', 'polypheny-jdbc-driver-2.3.jar') ...
    };

    % Add JARs if not already on classpath
    for i = 1:numel(jarPaths)
        if ~any(strcmp(jarPaths{i}, javaclasspath('-all')))
            javaaddpath(jarPaths{i});
        end
    end

    % Try to register the JDBC driver dynamically
    try
        %java.lang.Class.forName('org.polypheny.jdbc.PolyphenyDriver');
        driver = javaObject('org.polypheny.jdbc.PolyphenyDriver');
        java.sql.DriverManager.registerDriver(driver);
    catch e
        warning('Could not register Polypheny JDBC driver dynamically: %s', char(e.message));
    end

    % Add MATLAB namespace folder (+polypheny)
    if exist(fullfile(root, '+polypheny'), 'dir')
        addpath(root);
    end

    disp('Polypheny connector initialized.');
end
