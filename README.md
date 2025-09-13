# Matlab-Connector
Addon for Matlab to connect and query a Polypheny database.

@REQUIREMENTS:
polypheny.jar  must be in the lib folder

JDK17 (newer versions don't support the MatlabEngine yet https://www.mathworks.com/support/requirements/language-interfaces.html)

Tests:
Before the entire Project can be built it is necessary to run the Polypheny Application on the Local machine. Matlab natively uses Java 8. Consequently the Add-on is written in Java 8, but the polypheny.jar in supplied in the latest version is run with Java 17. To avoid version conflicts it is necessary to run the Polypheny Application separately before building the tests, otherwise the build will fail. Automating this startup within the project was considered, but ultimately rejected due to cross-platform instability and environment-specific issues.