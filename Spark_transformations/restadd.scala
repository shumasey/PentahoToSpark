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