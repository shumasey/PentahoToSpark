import com.databricks.spark.xml._
:require /C:/data-integration/drivers/postgresql-42.2.22.jar
val order=spark.read.format("xml").option("rowTag","order").xml("C:/pdi_files/input/orders.xml")
val ordersum=order.groupBy($"man_code",$"prod_code").agg(count("_ordernumber") as "quantity",concat_ws(",",collect_list("idcus")) as "customers")
val products=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/postgres").option("driver","org.postgresql.Driver").option("dbtable","public.products").option("user","postgres").option("password","1").load()
val mergedf=ordersum.join(products,products("pro_code").contains(ordersum("prod_code")),"inner").filter($"pro_stock" < $"quantity")
val sugg1=mergedf.select('customers,'quantity as "quantity_param",'pro_theme as "theme_param",'pro_name as "puzz_name")
val sugg2=sugg1.as("a").join(products.as("b"),$"a.theme_param" === $"b.pro_theme").filter($"a.quantity_param" < $"b.pro_stock")
import org.apache.spark.sql.expressions.Window
val theme=Window.partitionBy('pro_theme).orderBy('pro_code)
val ranked=sugg2.withColumn("rank", rank over theme)
ranked.select('customers,'quantity_param,'theme_param,'puzz_name,'man_code,'pro_code,'pro_name).filter($"rank" < 5).show(false)