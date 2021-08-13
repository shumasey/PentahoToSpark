:require /C:/data-integration/drivers/postgresql-42.2.22.jar
val cities=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.cities").option("user","postgres").option("password","1").load()
val countries=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.countries").option("user","postgres").option("password","1").load()
val regions1=cities.join(countries,cities("cou_id") === countries("cou_id"),"inner").drop("cou_id")
val regions2=regions1.withColumnRenamed("city_name","city").withColumnRenamed("country_name","country").withColumn("region",lit("N/A")).withColumnRenamed("city_id","id_js").withColumn("lastupdate",lit(current_date()))
import org.apache.spark.sql.expressions.Window
val id=Window.partitionBy('region).orderBy('country,'city)
val regions3=regions2.withColumn("id", rank over id)
val regions4=regions3.union(Seq((0,"N/A","N/A","N/A",null,0)).toDF)
val regions5=regions4.select('id,'city,'country,'region,'id_js,'lastupdate)
regions5.write.format("jdbc").mode("overwrite").option("delete from",true).option("url","jdbc:postgresql://localhost:5432/js_dw").option("driver","org.postgresql.Driver").option("dbtable","public.lk_regions").option("user","postgres").option("password","1").save()