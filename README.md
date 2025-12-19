# Matlab-Connector
Addon for Matlab to connect and query a Polypheny database.

@REQUIREMENTS:
JDK17 (newer versions don't support the MatlabEngine yet https://www.mathworks.com/support/requirements/language-interfaces.html)

Not included in this repository:
- polypheny.jar (see Build)
- polypheny-jdbc-driver-2.3.jar (see Build)
- The Polypheny server is NOT started automatically (see Tests)
- The MATLAB toolbox (.mltbx) within the matlab-polypheny-connector (see Matlab Package Toolbox)

Tests:
Before the entire Project can be built it is necessary to run the Polypheny application on the local machine. Matlab natively uses Java 8. Consequently the Add-on is written in Java 8, but the polypheny.jar supplied in the latest version is run with Java 17. To avoid version conflicts it is necessary to run the Polypheny Application separately before building the tests, otherwise the build will fail. Automating this startup within the project was considered, but ultimately rejected due to cross-platform instability and environment-specific issues.


Build:
To build the project the following requirements must be satisfied:
1. The libs folder must contain
    -polypheny-jdbc-driver-2.3.jar
    -polypheny.jar

2. The Polypheny application must be active and running on the local machine (see "Tests").

3. gradle must be installed and sufficiently configured

4. MATLAB: R2023b or newer (uses built-in Java 8)

5. Polypheny JDBC driver: v2.3

6. Polypheny server: tested with v1.9

Shipping the Connector (Package Toolbox):
The connector can be distributed using a PolyphenyConnector.mltbx file that is sent to the user (or published). To create the .mltbx file one must make use of the matlab-polypheny-connector project folder in this repository.

1. Open Matlab (version see above)
2. go to "Home"
3. open "Add-Ons" and select "Package Toolbox"
4. select the matlab-polypheny-connector as folder and accept
5. select the project called "Toolbox1"
6. add the latest versions of the following .jar:
    - polypheny-all.jar to the jar folder
    - polypheny-jdbc-driver-2.3.jar to the libs folder
7. delete the PolyphenyConnector.mltbx file in Toolbox1/release (otherwise this is repackaged and bloats the new .mltbx file)

8. Make sure the preview looks in order: in particular:
    - Output file name should be: PolyphenyConnector.mltbx
    - Output location should be: [project root]\Toolbox1\release
    - Matlab Path should be "/"
    - the Class Path should include:
        jar/polypheny-all.jar
        libs/polypheny-jdbc-driver-2.3.jar
9. Click "Package Toolbox": Matlab will generate a PolyphenyConnector.mltbx file and store it in Toolbox1/release






