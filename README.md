# PentahoToSpark
Sample project skills demonstration

This is sample project based on case studies from "Pentaho Data Integration Beginner's Guide, Second Edition" Chapter 9:Performing Advanced Operations with Databases.
At first, the tasks were performed in pentahoDI, and then rewritten to be performed in Spark in the Scala language.

Folders:
code_09 contains data for enviroment preparation from "PDI Beginner's Guide" source. While guide's lab using MySQL, I've changed scripts for PostgresQL.
input contains some input files for tasks.
jars contains some drivers for Spark.
output contains output results.
PentahoDI_transformations contains tasks files for PentahoDI.
Screenshots contains screenshots of Pentaho and Spark tasks execution for comparison.
Spark_transformations contains tasks files for Spark.

How to reproduce tasks
1. Prepare the environment
2. Install PentahoDI
3. Start data-integration/Spoon.bat
4. Open transformation and run it
5. Install Spark
6. Start spark-shell
7. Type :load /path to file XXX.scala

Case studies
Doing Simple Lookup
Doing Complex Lookup
Filling DataWarehouse
Adding Regions
Loading the Manufacturers
Keeping a History of Changes
Keeping a History of Regions
