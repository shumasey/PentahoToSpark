# Doing Simple Lookup
## Task
Suppose that you have an online system for your customers to order products. On a daily
basis, the system creates a file with the orders information. Now, you will check if you have
stock for the ordered products and make a list of the products you'll have to buy.
## Solution in Pentaho DI (products_to_buy.ktx)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupPDI_1.png)
## Solution in Spark (simpleLookups.scala)
Import XML driver to load XML data:
"'{scala} {
	import com.databricks.spark.xml._
}'"
Load JDBC driver to connect to database:
'''
	:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar
'''
Load XML data:
	val order=spark.read.format("xml")
		.option("rowTag","order")
		.xml("C:/GitHub/PentahoToSpark/input/orders.xml")

Load data from database:
	val products=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.products")
		.option("user","postgres")
		.option("password","1")
		.load()
		
Group products by product's code:
	val ordersum=order.groupBy($"man_code" as "manu_code",$"prod_code").count()
	
Lookup groupped products in database:
	val mergedf=ordersum.join(products,products("pro_code").contains(ordersum("prod_code")),"inner")
	
Filter quantity and output results:
	mergedf.select('man_code,'prod_code,'pro_name,'count as "quantity")
		.filter($"count" > $"pro_stock")
		.coalesce(1)
		.write.mode("overwrite")
		.option("header",true)
		.option("delimiter",";")
		.csv("C:/GitHub/PentahoToSpark/output/products_to_buy")
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupSpark_1.png)

