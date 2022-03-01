import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object main extends App with Session with Time{
  val desafio = new Desafios()
  desafio.tres()
  val df4 = desafio.cuatro()
  val df5 = desafio.cinco(df4)
  desafio.seis(df5)
  desafio.siete(df4)
  desafio.ocho("2021","05","17")
  desafio.nueve("20210410",10)
}



