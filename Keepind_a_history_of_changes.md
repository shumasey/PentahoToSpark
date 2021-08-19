# Keeping a History of Changes
## Task
A dimension where changes may occur from time to time is named __Slowly__Changing__Dimension__(SCD).__ If, when you update a SCD dimension, you don't preserve historical values but overwrite the old values, the dimension is called __Type__I__slowly__changing__dimension__(Type__I__SCD).__ Sometimes you would like to keep a history of the changes. Load a puzzles dimension along with the history of the changes in puzzles attributes.
## Solution in Pentaho DI (keep_history.ktr)
### Transformation
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/KeepingHistoryPDItransf.png)
### Execution result
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/KeepingHistoryPDI.png)
## Solution in Spark (keepingHistory.scala)
```scala
//Load JDBC driver to connect to database:
	:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar

//Load data from database:
	val products=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.products")
		.option("user","postgres")
		.option("password","1")
		.load()
		
//Filter puzzles
	val products1=products.select('pro_code,'man_code,'pro_name,'pro_theme).filter(col("pro_type").contains("PUZZLE"))

//Fit dataframe to dataWarehouse table:
	val products2=products1.withColumnRenamed("man_code","id_js_man")
		.withColumnRenamed("pro_name","name")
		.withColumn("dummy",lit("N/A"))
		.withColumn("lastupdate",lit(current_timestamp()))
		.withColumnRenamed("pro_theme","theme")
		.withColumnRenamed("pro_code","id_js_prod")
		.withColumn("start_date",lit("1900-01-01").cast("date"))
		.withColumn("end_date",lit("2199-12-31").cast("date"))
		.withColumn("version",lit(1))
		.withColumn("current",lit("Y"))
		
//Use window for generating technical key:
	import org.apache.spark.sql.expressions.Window
	val id=Window.partitionBy('dummy).orderBy('id_js_prod)
	val products3=products2.withColumn("id", rank over id)

//Fit dataframe to dataWarehouse table:
	val products4=products3.select('id,'name,'theme,'id_js_prod,'id_js_man,'start_date,'end_date,'version,'current,'lastupdate)

//Load data from datawarehouse:
	val lk_puzzles=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/js_dw")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.lk_puzzles")
		.option("user","postgres")
		.option("password","1")
		.load()
		.cache()
		
//Make action to store data in cache otherwise they will be erased:
	lk_puzzles.count()

//Drop empty rows:
	val lk_puzzles1=lk_puzzles.na.drop()

//Union new and old data:
	val lk_puzzles2=lk_puzzles1.union(products4)

//Remove duplicates without change in "theme" and "product code":
	val lk_puzzles3=lk_puzzles2.dropDuplicates("theme","id_js_prod")

//Group by product code and filter more than one records:
	val lk_puzzles4=lk_puzzles3.groupBy('id_js_prod.as("prodid")).count().filter("count > 1")

//Select puzzles with changes:
	val lk_puzzles5=lk_puzzles3.join(lk_puzzles4, lk_puzzles3("id_js_prod") === lk_puzzles4("prodid"),"inner")

//Modify date and status columns for puzzles with changes:
	val lk_puzzles6=lk_puzzles5.withColumn("current", when(lk_puzzles5("lastupdate") < current_timestamp(), "N").otherwise("Y"))
		.withColumn("version", when(lk_puzzles5("lastupdate") < current_timestamp(), $"version").otherwise($"version"+1))
		.withColumn("start_date", when(lk_puzzles5("lastupdate") < current_timestamp(), $"start_date").otherwise(current_date()))
		.withColumn("end_date", when(lk_puzzles5("lastupdate") < current_timestamp(), current_date()).otherwise($"end_date"))

//Drop unnecessary columns:
	val lk_puzzles7=lk_puzzles6.drop("prodid","count")

//Combine initial and changed records:
	val lk_puzzles8=products4.union(lk_puzzles7)

//Sort data in order to drop correct duplicates:
	val lk_puzzles9=lk_puzzles8.sort('id_js_prod,'lastupdate,desc("version"))

//Drop initial and left changed records:
	val lk_puzzles10=lk_puzzles9.dropDuplicates("theme","id_js_prod","current")

//Add "dummy" column to generate new technical key:
	val lk_puzzles11=lk_puzzles10.drop('id).withColumn("dummy",lit("NA"))

//Use window for generating technical key:
	import org.apache.spark.sql.expressions.Window
	val id=Window.partitionBy('dummy).orderBy('version,'id_js_prod)
	val lk_puzzles12=lk_puzzles11.withColumn("id", rank over id)

//Fit dataframe to dataWarehouse table:
	val lk_puzzles13=lk_puzzles12.select('id,'name,'theme,'id_js_prod,'id_js_man,'start_date,'end_date,'version,'current,'lastupdate)

//Add empty row:
	val lk_puzzles14=lk_puzzles13.union(Seq((0,"N/A","N/A",0,0,null,null,1,"Y",null)).toDF)

//Fill dataWarehouse:
	lk_puzzles14.write.format("jdbc")
		.mode("overwrite")
		.option("delete from",true)	//this option just empty the table, otherwise new table will be created
		.option("url","jdbc:postgresql://localhost:5432/js_dw")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.lk_puzzles")
		.option("user","postgres")
		.option("password","1")
		.save()
```
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/KeepingHistorySpark.png)
## Task
In the preceding section, you loaded a dimension with products by using a Dimension lookup/update step. You ran the transformation once, caused the insertion of one record for each product, and a special record with values N/A for the descriptive fields. Now make some changes in the operational database: in _/code09/scripts_ locate the _update_jumbo_products.sql_ script and run it. This makes changes for some JUMBO puzzles. Then run the transformation again to see how the Dimension lookup/update step stores history.
### Execution result
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/KeepingHistoryPDI_updated.png)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/KeepingHistorySpark_updated.png)