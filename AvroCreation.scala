import org.apache.spark.sql.SparkSesseion
import packageName._

val spark= SparkSesseion.builder().getorcreate()





val df= spark.read.csv("NYCfiles/green_tripdata_2021-2.csv")





