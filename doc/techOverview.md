# Data Services Technical Overview

TODO:  KC/Gretchen to include section describing use cases, what it does and why.

## Table of Contents
0. [How it Works](#how-it-works)
0. [ETL Designer Guide](#etl-designer-guide)
  * [Optimizations](#optimizations)
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
  0. Optimizations may be applied to the Service Transformation, depending on the user's query and constraints of the optimization type. These optimizations are intended to reduce the number of rows processed during query execution.
  0. Both transformations execute. Output from the service transformation is injected into the generated transformation, and output of the generated transformation is returned to the user as a Result Set.

A vast majority of the work here is done by the [pdi-dataservice-server-plugin](https://github.com/pentaho/pdi-dataservice-server-plugin). It is located in Karaf assembly in data-integration and data-integration-server.

The [pdi-dataservice-client-plugin](https://github.com/pentaho/pdi-dataservice-plugin/tree/master/pdi-dataservice-client/) contains the code for connecting to a server, issuing queries, and receiving result sets. It also contains the [DatabaseMetaPlugin](https://github.com/pentaho/pdi-dataservice-plugin/blob/master/pdi-dataservice-client/src/main/java/org/pentaho/di/trans/dataservice/client/DataServiceClientPlugin.java) which adds 'Pentaho Data Services' as a supported database connection type. It is included in the karaf assemblies of all supported Pentaho products and can be added to the classpath (along with a few dependencies) of other tools using JDBC connections.

## ETL Designer Guide

Although any transformation step can be used as a Data Service, it is highly recommended that a transformation contains only one data service and that running the transformation has no persistent side-effects. When using data services with an OLAP engine like Mondrian, many SQL queries may be issued, and the entire transformation may run each time. Data Service transformations should therefore consist of inputs combined into one output.

### Testing

Data services can be tested directly within Spoon. After creating the data service, use the test dialog to run test queries [(see help)](https://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/020#Create_a_Pentaho_Data_Service).

It is also highly recommended to start a Carte server connected to the current repository and test the service from a [new database connection](https://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/040#Access_a_Pentaho_Data_Service) within Spoon. This will ensure that any resources used by the transformation will be accessible when executing remotely.

### Optimizations

Data service optimizations should be configured once the data service has been tested and output is verified.

Optimizations should never alter the output of a service. They are designed only to speed up data services by reducing the number of rows being processed. They are also designed to execute in a 'best-effort' fashion. If criteria for an optimization is not met, no changes will be made.

#### Caching
Caching is designed to run the Service Transformation only once for similar queries. When enabled, the output of the user-defined Service Transformation will be stored in a timed cache before being passed to the Generated Transformation.

On subsequent queries, this optimization will determine if the cache holds enough rows to answer the query. If it does, the Service Transformation will not run. Rows will be injected into the Generated Transformation directly from the cache. A Generated Transformation will always be created and run for each query.

Caching is enabled by default when a Data Service is created. The timeout for a cache (TTL) can be configured on a per-service basis within Spoon.

Depending on system resources and the size of the service output, [cache limits](#result-set-size) may be reached and performance will degrade severely.

#### Parameter Generation

Parameter Generation should be used when a service imports rows form an 'Table Input' or 'Mongo Input' Step. Use the [help pages](https://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/020#Optimize_a_Data_Service) to configure a Parameter Generation optimization.

This optimization will analyze the WHERE clause of an incoming query and push parts of the query down to an input step. The query fragment will be reformatted as SQL or JSON, depending on the input step type.

A Parameter Generation optimization can not be used if rows are modified between the input step and the service output step (e.g. Calc, Group By). Filtering, adding additional fields, merging with other rows, and altering row metadata is allowed and can be optimized.

#### Parameter Pushdown

Parameter Pushdown is a powerful optimization that can be used in many scenarios, but requires special consideration when designing the Service Transformation.

Mappings are configured to correlate virtual table columns to transformation parameters. When a query contains a simple `COLUMN = 'value'` predicate in the WHERE clause, the corresponding transformation parameter is set to `value`. A formatting string can optionally be modified to add a prefix or postfix to the value. A default value can be set from the Transformation Properties dialog.

When designing the transformation, be careful of the case where the parameter may be empty, meaning the query was not optimized. For example, consider the [marsPhoto sample transformation](https://github.com/hudak/nasa-samples/blob/master/mars.ktr)

![Parameter Pushdown configuration](resources/paramPushdown.png)

These parameters are injected into the row stream via a Get Variable Step

![Get Variables](resources/paramPushdown-getVar.png)

The `ROVER_FILTER` parameter is applied with a Filter Step. If the parameter is null, the filter could not be optimized and all rovers are queried.

![Filter query](resources/paramPushdown-filter.png)

The Value Format in the optimization configuration allows the `*_QUERY` parameters to optimize and HTTP request. A Concat Strings step is used to append the parameters to the end of the base URL.

![Filter query](resources/paramPushdown-url.png)

### Hosting

The recommended means of publishing a Data Service is running a **DI Server**. Connect Spoon to the EE Repository of a DI Server and save a transformation with a Data Service. The service will automatically be published to the DI Server and available to connected JDBC clients.

**No further configuration is required to the DI Server.** This differs from releases prior to 6.0, where an admin would have to modify the server's `slave-server-config.xml`. Starting with 6.0, users will automatically connect to the server's built-in repository and inherit the execution rights of the current session. JDBC users connected in this manner will only be able to query a data service if they would normally have execution rights for the respective transformation.

Alternatively, Data Services can be published from a **Carte** server. Before starting carte, save a Data Service to a repository in Spoon. In `${user.home}/.kettle/repositories.xml`, identify the name of the `repository` entry corresponding to the used repository.
```xml
<repositories>
  ...
  <repository>
    <name>myRepo</name>
    <description>My Repository</description>
  </repository>
</repositories>
```

Create a [slave server configuration file](https://help.pentaho.com/Documentation/5.3/0P0/0U0/050/060/010#Configure_Carte_Slave_Servers) and add a `repository` entry with a matching name.

```xml
<slave_config>
  <repository>
    <name>myRepo</name>
  </repository>

  <slaveserver>
    <name>master1</name>
    <hostname>localhost</hostname>
    <port>8080</port>
  </slaveserver>

</slave_config>
```

JDBC clients connecting to Carte will be able to execute data services saved in the repository.

Although not recommended, it is possible to configure a DI Server to use an alternative repository by adding a `repository` element to `data-integration-server/pentaho-solutions/system/kettle/slave-server-config.xml`

## User Guide

### SQL Queries
Data Services support a limited subset of SQL.  The capabilities are documented here: [6.0](http://help.pentaho.com/Documentation/6.0/0L0/0Y0/090/080), [6.1](http://help.pentaho.com/Documentation/6.1/0L0/0Y0/090/080)


**Some important things to keep in mind:**
- Calculations are *not* supported.  This impacts things like Mondrian’s native filter and topcount since it will attempt to push down simple filter calculations to the database (see above).
- Aggregate functions can give incorrect results when NULLs are included in the results.  For example, min/max will include NULLs in the results, where the SQL spec says they should be excluded ([PDI-14974](http://jira.pentaho.com/browse/PDI-14974), [PDI-14422](http://jira.pentaho.com/browse/PDI-14422))
- Fields cannot be referenced with the “kettle” schema (e.g. “Kettle”.”Table”.”Field”).  ([PRD-5560](http://jira.pentaho.com/browse/PRD-5560))
- Nested selects are not supported
- No table joining is supported.  Data Services only work with single tables.


### PDI
**Data Refinery** Pentaho Data Services make a good datasource for use in the Data Refinery.  The Build Model job step is able to select from data services that are defined in any of the transformation steps that are connected in your job.  The Annotate Stream transformation step is a good choice for where to attach your data service.  Your transformation will be run twice in the typical data refinery setup.  The first time is from the flow of the main transformation, and the second time as a result of the Build Model step connecting to the service for modeling.  This is important to know for troubleshooting when you are connected to a remote repository and running the job locally.  The job runs once locally and once remotely.  In this case, make sure you aren't refencing any local files.  The Publish Model job step will publish a JDBC connection to the BA Server using the URL from the DI Repository you are connected to.  Publish will fail if you are not connected to a DI repository since the data service would not be accessible outside your local session.

### Analyzer Modeling
**Star Schemas** Analyzer and Mondrian typically use a Star Schema, where there are separate tables for facts and dimensions.  Pentaho Data Services do not support joining multiple transformations together.  This means you will have to model your schema against one flat table.

**Parent-Child Hierarchies** Parent-child hierarchies usually require a closure table in order to have adequate performance.  Since closure tables also require a sql join, you cannot use them in your schema backed by a Pentaho Data Service.  Therefore, we recommend against creating any Parent-child hierarchies for all but the smallest of data sets. 

**Aggregate Tables** Aggregate tables are allowed in your schema, of course you still cannot link to any dimension tables.  Each aggregate table needs to be defined as separate transformation with the attached data service.  You may use PDI's included steps for grouping and sorting to build your aggregate transformation.  You may also choose to configure your data input step to do the aggregation at the source.  MDX queries that are able to utilize your aggregate table will do so when querying for cell data, but queries for member data will not use the aggregate table.

**Modeling Tools** You may use any of your usual tools for creating a Mondrian schema.  Pentaho Schema Workbench, Data Source Wizard, PDI Annotations or manual schema creation can all generate valid schemas for use with Pentaho Data Services.  Keep in mind the limitations as descrbed here.  For example, because you may not use a Star Schema, the Shared Dimension annotation would generate an invalid schema for a Pentaho Data Service connection.

**Mondrian Properties** If your schema defines calculated measures that use any arithmetic operations in the calculation, then you will need to disable the native filter option in Mondrian.  For example, if your measure computes an average by dividing a sum measure by a count measure, then that measure will cause SQL failures when used on a report, unless you disable native filter.  The option is specified in mondrian.properties with the name "mondrian.native.filter.enable".

### Reporting

#### PRD

For the most part PRD reports will work well with Data Services. Parameterized reports in particular can benefit from the optimization features of Data Services:

**Query Pushdown.**  If the underlying datasources in a Data Service include Table Input or MongoDB Input, including Query Pushdown optimizations will allow the parameters selected within the report to be included in the queries to the source data, which can greatly limit the amount of processing the service needs to perform.  Query Pushdown can handle relatively complex WHERE clauses, and can work well with both Single-Select style parameters as well as Multi-Select.

**Parameter Pushdown.**  For other input sources (e.g. a REST input), Parameter Pushdown can be leveraged to make parameterized PRD reports more efficient.  Single values selected within one or more report parameters can be pushed down and used to limit the underlying data source.  Note that this won’t work with Multi-Select parameters, since Parameter Pushdown does not support IN lists.

There are two current limitations to be aware of when creating PRD reports.

1.  When defining your SQL query, the SQL Query Designer is able to load and display the virtual tables and fields available from a data service.  The filenames set in the editor, however, use the full path (e.g. “Kettle”.”VirtualTable”.’VirtualField”).  Data Services SQL does not support prefixing the “Kettle” schema name within column references (PRD-5560).  The workaround is to manually edit the query to remove the “Kettle”.

2.  Including parameters in your query can cause design-time issues, since PRD will place NULL values in parameters when doing things like preview or listing the available columns in the Query tree in the Data Sets pane ([PRD-5662](http://jira.pentaho.com/browse/PRD-5662)).  The workaround for this is to not use parameters while doing report design, swapping them in at the point you’re ready to test the report locally or publish.  This design time limitation should not impact successful execution of reports with parameters.

The one place the above limitation can have run-time impact is if the PRD parameter is set to "Validate Values" and can have a NULL (or N/A) value.  PRD will attempt to validate by passing a NULL in the JDBC call, which hits the same error as [PRD-5662](http://jira.pentaho.com/browse/PRD-5662) describes.  This error doesn't actually prevent running the report, but does display a validation error below the prompt.

Another potential “gotcha” with PRD/Data Services report construction is that the datatypes from the transformation in the data service may not translate to what you expect in the virtual table.  For example, an Integer field in a transformation will be widened to a Long in the resultset.  Make sure to check the datatype as displayed in the Data Set tree when defining parameter datatypes.

Also, virtual table names in SQL are case sensitive, so make sure to match the casing from the defined service.

#### PIR

Interactive Reporting models for use with Data Services can be created both with the Data Source Wizard and with Metadata Editor.  Be aware that if using Data Source Wizard you must select “Database Tables” and cannot define a SQL query for the source.  This is because DSW will wrap any SQL specified as a sub-select, and the Data Services SQL parser is not currently capable of handling such queries.  The error when attempting to use SQL is not particularly helpful, either:  “Unable to generate model:  null”.  [BISERVER-13064](http://jira.pentaho.com/browse/BISERVER-13064)

Models can be created using Pentaho Metadata Editor as well, with the limitation that no joins can be defined (since Data Services supports querying only a single table).

As with PRD reports, including report parameters can make performance optimizations with Query Pushdown and Parameter Pushdown very effective (see above).

### External tools

## Limitations
#### Result Set size
     - *TEST*
#### Multi tenancy

## Troubleshooting
  - Local/Remote Files
