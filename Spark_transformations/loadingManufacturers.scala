//Load JDBC driver to connect to database:
:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar
//Load data from database:
val manufact=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.manufacturers").option("user","postgres").option("password","1").load()
//Fit dataframe to dataWarehouse table:
val manufact1=manufact.withColumnRenamed("man_code","id_js").withColumnRenamed("man_desc","name").withColumn("dummy",lit("N/A")).withColumn("lastupdate",lit(current_date()))
//Use window for generating technical key
import org.apache.spark.sql.expressions.Window
val id=Window.partitionBy('dummy).orderBy('id_js)
val manufact2=manufact1.withColumn("id", rank over id)
//Fit dataframe to dataWarehouse table:
val manufact3=manufact2.select('id,'name,'id_js,'lastupdate)
//Fill dataWarehouse:
manufact3.write.format("jdbc").mode("overwrite").option("delete from",true).option("url","jdbc:postgresql://localhost:5432/js_dw").option("driver","org.postgresql.Driver").option("dbtable","public.lk_manufacturers").option("user","postgres").option("password","1").save()