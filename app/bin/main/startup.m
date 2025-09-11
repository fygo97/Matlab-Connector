function startup
    % Get root folder of the toolbox
    root = fileparts( mfilename( 'fullpath' ) );

    % Add Java JARs
    javaaddpath( fullfile( root, 'jar',  'polypheny-all.jar' ) );
    javaaddpath( fullfile( root, 'libs', 'polypheny-jdbc-driver-2.3.jar' ) );

    % Add MATLAB class folder to path
    % addpath( fullfile( root, '+polypheny' ) );  % optional, allows tab-completion & easier access

    % Set Polypheny server jar path (for ProcessBuilder)
    javaMethod( 'setProperty', 'java.lang.System', ...
                'polypheny.jar', fullfile( root, 'libs', 'polypheny.jar' ) );

    disp('Polypheny connector initialized.');
end
