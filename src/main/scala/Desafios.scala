
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{arrays_zip, bround, col, explode, format_number, monotonically_increasing_id, row_number, sum}

class Desafios extends Time with Session{
  def tres():Unit ={
    val df_raw = spark.read.json("./Desafio3/Sellers.json")
    val df_cured = df_raw.select("body.site_id","body.id","body.nickname","body.points")
    val ruta_actual = "./Desafio3/MPE/"+now_year+"/"+now_month+"/"+now_day+"/"

    df_cured.withColumnRenamed("site_id","siteId")
    df_cured.withColumnRenamed("sellerId","sellerId")
    df_cured.withColumnRenamed("nickname","sellerNickname")

    df_cured.select("*").filter(col("body.points") > 0).write.format("csv").save(ruta_actual + "positivo")
    df_cured.select("*").filter(col("body.points") === 0).write.format("csv").save(ruta_actual + "cero")
    df_cured.select("*").filter(col("body.points") < 0).write.format("csv").save(ruta_actual + "negativo")
  }

  def cuatro():sql.DataFrame = {
    val df_raw = spark.read.json("./Desafio4/MPE1004.json")

    df_raw.select("results.id","results.sold_quantity","results.available_quantity")
      .withColumn("allinone"
        ,arrays_zip(col("id")
          ,col("sold_quantity")
          ,col("available_quantity")))
      .drop("id","sold_quantity","available_quantity")
      .withColumn("allinone",explode(col("allinone")))
      .withColumn("rowId",monotonically_increasing_id()+1)
      .select(col("rowId")
        ,col("allinone.id").alias("itemId")
        ,col("allinone.sold_quantity").alias("soldQuantity")
        ,col("allinone.available_quantity").alias("availableQuantity"))
  }

  def cinco(df4:sql.DataFrame):sql.DataFrame = {
    val df_raw_visits = spark.read.format("csv").option("header", "true").load("./Desafio5/visits.csv")
    val df_raw_producto = df4.select("itemId", "soldQuantity")
      .withColumnRenamed("itemId","itemId_producto")

    df_raw_producto.join(df_raw_visits, df_raw_producto("itemId_producto") === df_raw_visits("itemId"), "inner")
      .drop(col("itemId_producto"))
      .select("itemId", "soldQuantity", "visits")
      .filter(col("soldQuantity") > 0)
  }

  def seis(df5:sql.DataFrame):sql.DataFrame = {
    val df_raw = df5
    val df_cured = df_raw
      .withColumn("conversionRate",col("soldQuantity")/col("visits"))
      .withColumn("conversionRate",format_number(col("conversionRate"),4))
    val window_cured = Window.orderBy(col("conversionRate").desc)
    df_cured.withColumn("conversionRanking",row_number().over(window_cured))
  }

  def siete(df4:sql.DataFrame): sql.DataFrame = {
    val df_raw = df4
    df_raw
      .withColumn("stockTotal",sum(col("availableQuantity")).over())
      .withColumn("stockPercentage",bround(col("availableQuantity")/col("stockTotal")*100,2))
      .drop("stockTotal")
      .orderBy(col("stockPercentage").desc)
  }

  def ocho(year:String, month:String, day:String): Unit = {
      def generateMonthlyPathList(year:String, month:String, day:String):Unit = {
        val url = "https://estaurl@noexiste/"
        var actual = getToday()
        val until = year + "-" + month + "-" + day
        while (DayBeforeOrEqualThan(actual, until)) {
          println(url + getYear(actual) + "/" + getMonth(actual) + "/" + getDay(actual) + "/")
          actual = getNextDay(actual)
        }
      }

    generateMonthlyPathList(year,month,day)

  }

  def nueve(date:String,days:Int): Unit = {
    def generateLastDaysPaths(date:String,days:Int):Unit = {
      val url = "https://estaurl@noexiste/"
      val until = ParseYYYYMMDDtoDate(date)
      var actual = addDays(until,-days+1)

      while (DayBeforeOrEqualThan(actual, until)) {
        println(url + getYear(actual) + "/" + getMonth(actual) + "/" + getDay(actual) + "/")
        actual = getNextDay(actual)
      }
    }

    generateLastDaysPaths(date,days)

  }
}
