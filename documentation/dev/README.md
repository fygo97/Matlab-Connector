---
layout: page
title: "MATLAB Connector (Developers)"
toc: true
docs_area: "devs"
doc_type: doc
tags: Matlab Connector, Matlab Toolbox, Matlab, Connector
search: false
lang: en
---

# MATLAB Connector (Developer Documentation)

This page documents developer-specific details for maintaining and extending the MATLAB Connector for Polypheny.

## Requirements

- **Java:** JDK 17 or older  
  (newer versions are not supported by the MATLAB Engine)  
  https://www.mathworks.com/support/requirements/language-interfaces.html
- **Polypheny:** must be running locally
- **MATLAB:** R2023b or newer (ships with Java 8)

## Repository Contents

The following components are **not included** in this repository:

- `polypheny.jar` (see **Build**)
- `polypheny-jdbc-driver-2.3.jar` (see **Build**)
- Polypheny server startup (see **Tests**)
- Packaged MATLAB toolbox (`.mltbx`) (see **Packaging**)

## Tests

Before running or building tests, the Polypheny server must be started manually.

MATLAB uses Java 8 internally, while the Polypheny server requires Java 17.  
To avoid version conflicts, the Polypheny server is run **outside** the build process.

Automatic startup was evaluated but rejected due to:
- cross-platform instability
- environment-specific Java configuration issues

If the server is not running, the build will fail.

## Build

To build the project, ensure the following:

1. The `libs/` directory contains:
   - `polypheny.jar`
   - `polypheny-jdbc-driver-2.3.jar`

2. The Polypheny server is running locally.

3. Gradle is installed and configured.

4. Versions used:
   - **MATLAB:** R2023b+
   - **Polypheny JDBC driver:** 2.3
   - **Polypheny server:** tested with 1.9

## Packaging (MATLAB Toolbox)

The connector is distributed as a MATLAB toolbox (`.mltbx`).

### Steps
0. create a `.jar` file with your latest version of the Polypheny Connector by compiling the project. (**Tip**: The compiled `polypheny-all.jar` will be stored under )
1. Open MATLAB
2. Go to **Home**
3. Open **Add-Ons → Package Toolbox**
4. Select the `matlab-polypheny-connector` folder
5. Select the project **Toolbox1**
6. Add JARs:
   - `polypheny-all.jar` → `jar/`
   - `polypheny-jdbc-driver-2.3.jar` → `libs/`
7. Delete any existing `PolyphenyConnector.mltbx` in `Toolbox1/release`

8. Before we finally package the Toolbox you must verify the following are true:

- **Output file:** `PolyphenyConnector.mltbx`
- **Output location:** `Toolbox1/release`
- **MATLAB path:** `/`
- **Class path must include:**
    
    jar/polypheny-all.jar

    libs/polypheny-jdbc-driver-2.3.jar


9. Click **Package Toolbox** (top right corner in MATLAB)

MATLAB will generate the toolbox file in `Toolbox1/release`.