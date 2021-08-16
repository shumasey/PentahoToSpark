# Doing Simple Lookup
## Task
Suppose that you have an online system for your customers to order products. On a daily
basis, the system creates a file with the orders information. Now, you will check if you have
stock for the ordered products and make a list of the products you'll have to buy.
## Solution in Pentaho DI (products_to_buy.ktx)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupPDI_1.png)
## Solution in Spark (simpleLookups.scala)
```scala
//Import XML driver to load XML data:
	import com.databricks.spark.xml._

//Load JDBC driver to connect to database:
	:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar

//Load XML data:
	val order=spark.read.format("xml")
		.option("rowTag","order")
		.xml("C:/GitHub/PentahoToSpark/input/orders.xml")

//Load data from database:
	val products=spark.read.format("jdbc")
		.option("url","jdbc:postgresql://localhost:5432/postgres")
		.option("driver","org.postgresql.Driver")
		.option("dbtable","public.products")
		.option("user","postgres")
		.option("password","1")
		.load()
		
//Group products by product's code:
	val ordersum=order.groupBy($"man_code" as "manu_code",$"prod_code").count()
	
//Lookup groupped products in database:
	val mergedf=ordersum.join(products,products("pro_code").contains(ordersum("prod_code")),"inner")
	
//Filter quantity and output results:
	mergedf.select('man_code,'prod_code,'pro_name,'count as "quantity")
		.filter($"count" > $"pro_stock")
		.coalesce(1)
		.write.mode("overwrite")
		.option("header",true)
		.option("delimiter",";")
		.csv("C:/GitHub/PentahoToSpark/output/products_to_buy")
```
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupSpark_1.png)

## Task
Create a new transformation and do the following:
1. Taking as source the orders file create a list of the customers that ordered products.
2. Include their name, last name, and full address.
3. Order the data by country name.
4. Verify that there is a product with the given manufacturerand product codes. If the data is valid check the stock and proceed. If not, make a list so the cases can be handled later by the customer care department.
## Solution in Pentaho DI (delivery.ktx)
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupPDI_2.png)
## Solution in Spark (simpleLookups.scala)
```scala
// Load customers data from database
val customers=spark.read.format("jdbc")
	.option("url","jdbc:postgresql://localhost:5432/postgres")
	.option("driver","org.postgresql.Driver")
	.option("dbtable","public.customers")
	.option("user","postgres")
	.option("password","1")
	.load()
	
// Lookup customers
val del1=order.join(customers,order("idcus") === customers("cus_id"),"inner")

// Calculate quantity
val del2=del1.groupBy($"man_code" as "manu_code",$"prod_code")
	.agg(count("_ordernumber") as "quantity",first("add_street") as "street",first("add_zipcode") as "zipcode",first("city_id") as "cityid",first("cus_id") as "cusid",first("cus_lastname") as "lastname",first("cus_name") as "name",first("_ordernumber") as "ordernumber")

// Lookup products
val del3=del2.join(products,products("pro_code").contains(del2("prod_code")),"inner")

// Filter product on stock
val del4=del3.filter($"pro_stock" >= $"quantity")

// Load cities data from database
val cities=spark.read.format("jdbc")
	.option("url","jdbc:postgresql://localhost:5432/postgres")
	.option("driver","org.postgresql.Driver")
	.option("dbtable","public.cities")
	.option("user","postgres")
	.option("password","1")
	.load()

// Lookup cities
val del5=del4.join(cities,del4("cityid") === cities("city_id"),"inner")

// Load countries data from database
val countries=spark.read.format("jdbc")
	.option("url","jdbc:postgresql://localhost:5432/postgres")
	.option("driver","org.postgresql.Driver")
	.option("dbtable","public.countries")
	.option("user","postgres")
	.option("password","1")
	.load()

// Lookup countries
val del6=del5.join(countries,del5("cou_id") === countries("cou_id"),"inner").drop("cou_id")
// Output results
del6.select('name,'lastname,'zipcode,'street,'city_name,'country_name,'ordernumber)
	.sort("country_name")
	.coalesce(1)
	.write.mode("overwrite")
	.option("header",true)
	.option("delimiter",";")
	.csv("C:/GitHub/PentahoToSpark/output/delivery")	
del3.filter($"pro_stock" < $"quantity")
	.select('man_code,'prod_code,'quantity,'street,'zipcode,'cityid,'cusid,'lastname,'name,'ordernumber,'pro_name,'pro_stock)
	.coalesce(1	)
	.write.mode("overwrite")
	.option("header",true)
	.option("delimiter",";")
	.csv("C:/GitHub/PentahoToSpark/output/empty_stock")
```
![img](https://github.com/shumasey/PentahoToSpark/blob/main/Screenshots/DoingSimpleLookupSpark_2.png)