//Load JDBC driver to connect to database:
:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar
//Load data from database:
val cities=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.cities").option("user","postgres").option("password","1").load()
val countries=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.countries").option("user","postgres").option("password","1").load()
//Lookup for countries:
val regions=cities.join(countries,cities("cou_id") === countries("cou_id"),"inner").drop("cou_id").withColumn("country_name",trim(col("country_name")))
//Load XLS driver:
:require C:/GitHub/PentahoToSpark/jars/spark-excel_2.12-0.13.7.jar
//Import XLS driver to load data from excel file:
import com.crealytics.spark.excel._
//Load regions from file: change following lines to choose correct file
val regions0=spark.read.format("com.crealytics.spark.excel").option("header",true).load("C:/pdi_files/input/regions.xls")
// val regions0=spark.read.format("com.crealytics.spark.excel").option("header",true).load("C:/pdi_files/input/regions2008.xls")
// val regions0=spark.read.format("com.crealytics.spark.excel").option("header",true).load("C:/pdi_files/input/myregions.xls")
//Lookup for regions:
val regions1=regions.join(regions0,regions("country_name") === regions0("country"),"left").drop("country")
//Fit dataframe to dataWarehouse table:
val regions2=regions1.withColumn("version_1",lit(null)).withColumn("country_1",lit(null)).withColumn("start_date",lit("1900-01-01").cast("date")).withColumn("end_date",lit("2199-12-31").cast("date")).withColumn("version",lit(1)).withColumn("dummy",lit("N/A")).withColumnRenamed("country_name","country").withColumn("current",lit(current_timestamp()))
//Use window for generating technical key:
import org.apache.spark.sql.expressions.Window
val id=Window.partitionBy('dummy).orderBy('country,'city_name)
val regions3=regions2.withColumn("id", rank over id)
//Fit dataframe to dataWarehouse table:
val regions4=regions3.select('id,'start_date,'end_date,'version,'country_1,'version_1,'country,'region,'current)
//Input changedate from command line:
val date = scala.io.StdIn.readLine("Put the date in format YYYY/MM/DD or press ENTER for today ")
//Convert input to dataframe:
val date1=Seq(date).toDF("inputdate")
//Convert String to Date:
val date2=date1.withColumn("changedate", when(length('inputdate) === 0, current_date()).otherwise(to_date('inputdate, "yyyy/MM/dd")))
//Load data from datawarehouse:
val lk_regions_2=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/js_dw").option("driver","org.postgresql.Driver").option("dbtable","public.lk_regions_2").option("user","postgres").option("password","1").load().cache()
//Make action to store data in cache otherwise they will be erased:
lk_regions_2.count()
//Drop empty rows:
val lk_regions_21=lk_regions_2.na.drop(Seq("country"))
//Union new and old data:
val lk_regions_22=lk_regions_21.union(regions4)
//Remove duplicates without change in "region" and "country":
val lk_regions_23=lk_regions_22.dropDuplicates("region","country")
//Group by product code and filter more than one records:
val lk_regions_24=lk_regions_23.groupBy('country.as("cntr")).count().filter("count > 1")
//Add "changedate" column:
val lk_regions_25=lk_regions_24.join(date2)
//Select countries with changes:
val lk_regions_26=lk_regions_23.join(lk_regions_25, lk_regions_23("country") === lk_regions_25("cntr"),"inner")
//Modify date and status columns for countries with changes:
val lk_regions_27=lk_regions_26.withColumn("version", when(lk_regions_26("current") < current_timestamp(), $"version").otherwise($"count")).withColumn("start_date", when(lk_regions_26("current") < current_timestamp(), $"start_date").otherwise(lk_regions_26("changedate"))).withColumn("end_date", when(lk_regions_26("current") < current_timestamp() && lk_regions_26("end_date") === "2199-12-31", lk_regions_26("changedate")).otherwise($"end_date"))
//Drop unnecessary columns:
val lk_regions_28=lk_regions_27.drop("cntr","count","inputdate","changedate")
//Combine initial and changed records:
val lk_regions_29=regions4.union(lk_regions_28)
//Sort data in order to drop correct duplicates:
val lk_regions_210=lk_regions_29.sort('country,'current,desc("version"))
//Drop initial and left changed records:
val lk_regions_211=lk_regions_210.dropDuplicates("region","country")
//Add "dummy" column to generate new technical key:
val lk_regions_212=lk_regions_211.drop('id).withColumn("dummy",lit("NA"))
//Use window for generating technical key:
import org.apache.spark.sql.expressions.Window
val id=Window.partitionBy('dummy).orderBy('version,'country)
val lk_regions_213=lk_regions_212.withColumn("id", rank over id)
//Fit dataframe to dataWarehouse table:
val lk_regions_214=lk_regions_213.select('id,'start_date,'end_date,'version,'country_1,'version_1,'country,'region,'current)
//Add empty row:
val lk_regions_215=lk_regions_214.union(Seq((0,null,null,1,null,null,null,null,null)).toDF)
//Fill dataWarehouse:
lk_regions_215.write.format("jdbc").mode("overwrite").option("delete from",true).option("url","jdbc:postgresql://localhost:5432/js_dw").option("driver","org.postgresql.Driver").option("dbtable","public.lk_regions_2").option("user","postgres").option("password","1").save()