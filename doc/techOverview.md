# Data Services Technical Overview

TODO:  KC/Gretchen to include section describing use cases, what it does and why.

## Table of Contents
0. [How it Works](#how-it-works)
0. [ETL Designer Guide](#etl-designer-guide)
0. [User Guide](#user-guide)
0. [Limitations](#limitations)
0. [Troubleshooting](#troubleshooting)

## How it Works

Data Services is implemented as a set of OSGi plugins and is included in all core Pentaho products. It allows data to be processed in Pentaho Data Integration and used in another tool in the form of a virtual database table.

Any Transformation can be used as a virtual table. Here's out it works:
  0. In Spoon, an ETL designer [creates a Data Service](https://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/020#Create_a_Pentaho_Data_Service) on the step which will generate the virtual table's rows.
  0. Meta data is saved to the transformation describing the the virtual table's name and any optimizations applied. Optimizations are optional should never affect the Data Service's output. They serve only to speed up processing.
  0. On saving the transformation to a repository, a table in the repository's MetaStore maps the virtual table to the transformation in which it is defined. A Carte or DI Server must be connected to this repository and running for any Data Services to be accessible.
  0. A user with a JDBC client [connects to the server](https://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/040). The client can list tables, view table structure, and submit SQL SELECT queries.
  0. When the server receives a SQL query, the table name is resolved and the user-defined **Service Transformation** is loaded from the repository. The SQL is parsed and a second **Generated Transformation** is created, containing all of the query operations (grouping, sorting, filtering).  
  0. Optimizations are applied to the Service Transformation in a best-effort fashion, depending on the user's query and constraints of the optimization type. These optimizations are intended to reduce the number of rows processed during query execution.
  0. Both transformations execute. Output from the service transformation is injected into the generated transformation, and output of the generated transformation is returned to the user as a Result Set.

A vast majority of the work here is done by the [pdi-dataservice-server-plugin](https://github.com/pentaho/pdi-dataservice-server-plugin). It is located in Karaf assembly in data-integration and data-integration-server.

The [pdi-dataservice-client-plugin](https://github.com/pentaho/pdi-dataservice-plugin/tree/master/pdi-dataservice-client/) contains the code for connecting to a server, issuing queries, and receiving result sets. It also contains the [DatabaseMetaPlugin](https://github.com/pentaho/pdi-dataservice-plugin/blob/master/pdi-dataservice-client/src/main/java/org/pentaho/di/trans/dataservice/client/DataServiceClientPlugin.java) which adds 'Pentaho Data Services' as a supported database connection type. It is included in the karaf assemblies of all supported Pentaho products and can be added to the classpath (along with a few dependencies) of other tools using JDBC connections.

## ETL Designer Guide
  - Transformation Design
  - Optimizations
    * Caching
     - *TEST*
       * Result set limitations
       * Cache capacity
    * Parameter Generation
    * Parameter Pushdown
  - Testing
    * Logging
  - Hosting
    * DI Server
    * Carte
    * Local

## User Guide
  - Queries
  - PDI
  - Analyzer <-- top use case
    * Modeling (workbench, dsw, SDR)
      * *TEST*
        - Properties
        - parent/child
        - AggTables
        - Big Filters
        - Compound slicers
        - count distinct
    * Shared dimensions
  - Reporting
    * PRD, preferred method?
    * PIR
    * *TEST*
  - External tools

## Limitations
  - Multi tenancy
  - Performance

## Troubleshooting
  - Local/Remote Files
