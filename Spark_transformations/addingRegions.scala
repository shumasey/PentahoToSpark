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
//Load regions from file:
val regions0=spark.read.format("com.crealytics.spark.excel").option("header",true).load("C:/pdi_files/input/regions.xls")
//Lookup for regions:
val regions1=regions.join(regions0,regions("country_name") === regions0("country"),"left").drop("country")
//Fit dataframe to dataWarehouse table:
val regions2=regions1.withColumnRenamed("city_name","city").withColumnRenamed("country_name","country").withColumn("dummy",lit("N/A")).withColumnRenamed("city_id","id_js").withColumn("lastupdate",lit(current_timestamp()))
//Use window for generating technical key
import org.apache.spark.sql.expressions.Window
val id=Window.partitionBy('dummy).orderBy('country,'city)
val regions3=regions2.withColumn("id", rank over id)
//Add empty row:
val regions4=regions3.union(Seq((0,"N/A","N/A","N/A","N/A",null,0)).toDF)
//Fit dataframe to dataWarehouse table:
val regions5=regions4.select('id,'city,'country,'region,'id_js,'lastupdate)
//Fill dataWarehouse:
regions5.write.format("jdbc").mode("overwrite").option("delete from",true).option("url","jdbc:postgresql://localhost:5432/js_dw").option("driver","org.postgresql.Driver").option("dbtable","public.lk_regions").option("user","postgres").option("password","1").save()