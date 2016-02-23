# Data Services Technical Overview

TODO:  KC/Gretchen to include section describing use cases, what it does and why.

0. How it Works
  - OSGi Plugins
    * Server
    * JDBC Client
  - Storage and Resolution
  - Execution
0. ETL Designer Guide
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
0. User Guide
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
0. Known and Unknown Limitations
  - Multi tenancy
  - Performance
0. Troubleshooting
  - Local/Remote Files
