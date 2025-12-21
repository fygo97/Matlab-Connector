---
layout: page
title: "MATLAB Connector (Users)"
toc: true
docs_area: "users"
doc_type: doc
tags: Matlab Connector, Matlab Toolbox, Matlab, Connector
search: true
lang: en
---

# MATLAB Connector (User Documentation)

## Setting up your connector
The Polypheny-Matlab connector is supplied using a PolyphenyConnector.mltbx file that will automatically install the necessary package in your Matlab environment. To install it:
- open Matlab and wait until it is ready to run
- double-click the supplied PolyphenyConnector.mltbx and wait a few seconds. Matlab should inform you the package was successfully installed.

> **Note**
>
> MATLAB provides a graphical UI to manage installed Add-Ons. In practice, this UI can be unreliable.
> If uninstalling the Polypheny Connector via the UI fails (e.g. when installing an updated version),
> the toolbox can be force-removed manually.

## Manual Uninstall (Force Removal)

### 1. Locate the installed toolbox

Run the following in the MATLAB console:

    which polypheny.Polypheny -all

This returns the folder where the toolbox is installed, typically of the form:

    <path>\MathWorks\MATLAB Add-Ons\Toolboxes\PolyphenyConnector
>
>### 2. Delete the toolbox folder
>
>Delete the entire `PolyphenyConnector` directory from disk.
>If files are locked on Windows, close MATLAB first and then delete the folder.
>
>### 3. Remove the toolbox from the MATLAB search path

    rmpath('<path-to-PolyphenyConnector-parent>')

### 4. Update MATLAB’s internal toolbox cache

    rehash toolboxcache

### 5. Persist the updated search path

    savepath

### Fallback (only if the path is badly polluted)

    restoredefaultpath
    rehash toolboxcache
    savepath

This procedure bypasses MATLAB’s Add-On Manager and ensures the toolbox is fully removed
from disk, the search path, and MATLAB’s internal cache. Note the PolyphenyConnector might not show up in your UI anymore, even if installation was successful.

