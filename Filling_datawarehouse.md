# Filling DataWarehouse
## Task
A dimension of DataWarehouse is an entity that describes your business, load a dimension that stores geographical information from On-Line Transaction Processing (OLTP) system.
## Solution in Pentaho DI (fill_dw.ktr)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/FillingDataWarehouse_PDI.png)
## Solution in Spark (fillingDW.scala)
```scala
//Load JDBC driver to connect to database:
	:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar

//Load data from database:
	val cities=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.cities")
		.option("user","postgres")
		.option("password","1")
		.load()
	val countries=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.countries")
		.option("user","postgres")
		.option("password","1")
		.load()

//Lookup for countries 
	val regions1=cities.join(countries,cities("cou_id") === countries("cou_id"),"inner").drop("cou_id")

//Fit dataframe to dataWarehouse table
	val regions2=regions1.withColumnRenamed("city_name","city")
		.withColumnRenamed("country_name","country")
		.withColumn("region",lit("N/A"))
		.withColumnRenamed("city_id","id_js")
		.withColumn("lastupdate",lit(current_timestamp()))
	
//Use window for generating technical key
	import org.apache.spark.sql.expressions.Window
	val id=Window.partitionBy('region).orderBy('country,'city)
	val regions3=regions2.withColumn("id", rank over id)

//Add empty row
	val regions4=regions3.union(Seq((0,"N/A","N/A","N/A",null,0)).toDF)

//Fit dataframe to dataWarehouse table
	val regions5=regions4.select('id,'city,'country,'region,'id_js,'lastupdate)

//Fill dataWarehouse
	regions5.write.format("jdbc")
		.mode("overwrite")
		.option("delete from",true) //this option just empty the table, otherwise new table will be created
		.option("url","jdbc:postgresql://localhost:5432/js_dw")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.lk_regions")
		.option("user","postgres")
		.option("password","1")
		.save()
```
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/FillingDataWarehouse_Spark.png)