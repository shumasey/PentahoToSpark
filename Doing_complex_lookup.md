# Doing complelex lookup
## Task
If your customers ordered a product that is out of stock you don't want to let them down, so you will suggest some alternative puzzles to buy.
## Solution in Pentaho DI (recommendations.ktr)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingComplexLookup_PDI.png)
## Solution in Spark (complexLookups.scala)
```scala
//Import XML driver to load XML data:
	import com.databricks.spark.xml._
	
//Load JDBC driver to connect to database:
	:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar
	
//Load XML data:
	val order=spark.read.format("xml")
		.option("rowTag","order")
		.xml("C:/GitHub/PentahoToSpark/input/orders.xml")
	
//Group products by product's code and concat customers:
	val ordersum=order.groupBy($"man_code",$"prod_code")
		.agg(count("_ordernumber") as "quantity",concat_ws(",",collect_list("idcus")) as "customers")
	
//Load data from database:
	val products=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.products")
		.option("user","postgres")
		.option("password","1")
		.load()
	
// Lookup products
	val mergedf=ordersum.join(products,products("pro_code")
		.contains(ordersum("prod_code")),"inner")
		.filter($"pro_stock" < $"quantity")
	
	val sugg1=mergedf.select('customers,'quantity as "quantity_param",'pro_theme as "theme_param",'pro_name as "puzz_name")
	
// Lookup products for suggestions
	val sugg2=sugg1.as("a").join(products.as("b"),$"a.theme_param" === $"b.pro_theme")
		.filter($"a.quantity_param" < $"b.pro_stock")
	
// Use window for generating technical key
	import org.apache.spark.sql.expressions.Window
	val theme=Window.partitionBy('pro_theme).orderBy('pro_code)
	val ranked=sugg2.withColumn("rank", rank over theme)
	
	ranked.select('customers,'quantity_param,'theme_param,'puzz_name,'man_code,'pro_code,'pro_name)
		.filter($"rank" < 5)
		.show(false)
```
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingComplexLookup_Spark.png)