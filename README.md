# PentahoToSpark
## Sample project skills demonstration

This is sample project based on case studies from ["Pentaho Data Integration Beginner's Guide, Second Edition"](https://github.com/happyapple668/gavin-repo/blob/master/books/BI/Kettle/Pentaho%20Data%20Integration%20Beginner's%20Guide%2C%20Second%20Edition.pdf) Chapter 9:Performing Advanced Operations with Databases.
At first, the tasks were performed in pentahoDI, and then rewritten to be performed in Spark in the Scala language.

### Folders:
__code_09__ contains data for enviroment preparation from "PDI Beginner's Guide" source. While guide's lab using MySQL, I've changed scripts for PostgresQL.
__input__ contains some input files for tasks.
__jars__ contains some drivers for Spark.
__output__ contains output results.
__PentahoDI_transformations__ contains tasks files for PentahoDI.
__Screenshots__ contains screenshots of Pentaho and Spark tasks execution for comparison.
__Spark_transformations__ contains tasks files for Spark.

### How to reproduce tasks
1. Prepare the environment
2. Install [PentahoDI](https://sourceforge.net/projects/pentaho/)
3. Start data-integration/Spoon.bat
4. Open transformation and run it
5. Install [Spark](https://spark.apache.org/downloads.html)
6. Start spark-shell
7. Type :load /path to file XXX.scala

### Case studies
Doing Simple Lookup
Doing Complex Lookup
Filling DataWarehouse
Adding Regions
Loading the Manufacturers
Keeping a History of Changes
Keeping a History of Regions
