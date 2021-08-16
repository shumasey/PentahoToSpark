//Import XML driver to load XML data:
import com.databricks.spark.xml._
//Load JDBC driver to connect to database:
:require C:/GitHub/PentahoToSpark/jars/postgresql-42.2.22.jar
//Load XML data:
val order=spark.read.format("xml").option("rowTag","order").xml("C:/GitHub/PentahoToSpark/input/orders.xml")
//Load data from database:
val products=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.products").option("user","postgres").option("password","1").load()
//Group products by product's code:
val ordersum=order.groupBy($"man_code" as "manu_code",$"prod_code").count()
//Lookup groupped products in database:
val mergedf=ordersum.join(products,products("pro_code").contains(ordersum("prod_code")),"inner")
//Filter quantity and output results:
mergedf.select('man_code,'prod_code,'pro_name,'count as "quantity").filter($"count" > $"pro_stock").coalesce(1).write.mode("overwrite").option("header",true).option("delimiter",";").csv("C:/GitHub/PentahoToSpark/output/products_to_buy")
val customers=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.customers").option("user","postgres").option("password","1").load()
val del1=order.join(customers,order("idcus") === customers("cus_id"),"inner")
val del2=del1.groupBy($"man_code" as "manu_code",$"prod_code").agg(count("_ordernumber") as "quantity",first("add_street") as "street",first("add_zipcode") as "zipcode",first("city_id") as "cityid",first("cus_id") as "cusid",first("cus_lastname") as "lastname",first("cus_name") as "name",first("_ordernumber") as "ordernumber")
val del3=del2.join(products,products("pro_code").contains(del2("prod_code")),"inner")
val del4=del3.filter($"pro_stock" >= $"quantity")
val cities=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.cities").option("user","postgres").option("password","1").load()
val del5=del4.join(cities,del4("cityid") === cities("city_id"),"inner")
val countries=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.countries").option("user","postgres").option("password","1").load()
val del6=del5.join(countries,del5("cou_id") === countries("cou_id"),"inner").drop("cou_id")
del6.select('name,'lastname,'zipcode,'street,'city_name,'country_name,'ordernumber).sort("country_name").coalesce(1).write.mode("overwrite").option("header",true).option("delimiter",";").csv("C:/GitHub/PentahoToSpark/output/delivery")
del3.filter($"pro_stock" < $"quantity").select('man_code,'prod_code,'quantity,'street,'zipcode,'cityid,'cusid,'lastname,'name,'ordernumber,'pro_name,'pro_stock).coalesce(1).write.mode("overwrite").option("header",true).option("delimiter",";").csv("C:/GitHub/PentahoToSpark/output/empty_stock")