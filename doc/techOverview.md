# Data Services Technical Overview

TODO:  KC/Gretchen to include section describing use cases, what it does and why.

## Table of Contents
0. [How it Works](#how-it-works)
0. [ETL Designer Guide](#etl-designer-guide)
0. [User Guide](#user-guide)
0. [Limitations](#limitations)
0. [Troubleshooting](#troubleshooting)

## How it Works
  - OSGi Plugins
    * Server
    * JDBC Client
  - Storage and Resolution
  - Execution

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
