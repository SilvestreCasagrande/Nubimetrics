import java.time.LocalDate
import java.time.format.DateTimeFormatter

trait Time {
  private val monthFormatter = DateTimeFormatter.ofPattern("MM")
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val YYYYMMDDFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def getToday():String = {
    LocalDate.now().toString
  }

  def getDay(fecha:String):String = {
    LocalDate.parse(fecha,dateFormatter).getDayOfMonth.toString
  }
  def getMonth(fecha:String):String = {
    LocalDate.parse(fecha,dateFormatter).format(monthFormatter)
  }
  def getYear(fecha:String):String = {
    LocalDate.parse(fecha,dateFormatter).getYear.toString
  }

  def getNextDay(fecha:String):String = {
    LocalDate.parse(fecha,dateFormatter).plusDays(1).toString
  }

  def addDays(fecha:String,num:Int):String ={
    LocalDate.parse(fecha,dateFormatter).plusDays(num).toString
  }

  def DayEqualsDay(fecha1:String,fecha2:String):Boolean = {
    LocalDate.parse(fecha1,dateFormatter).equals(LocalDate.parse(fecha2,dateFormatter))
  }

  def DayBeforeThanDay(fecha1:String,fecha2:String):Boolean = {
    LocalDate.parse(fecha1,dateFormatter).isBefore(LocalDate.parse(fecha2,dateFormatter))
  }

  def DayBeforeOrEqualThan(fecha1:String,fecha2:String):Boolean = {
    DayBeforeThanDay(fecha1,fecha2) || DayEqualsDay(fecha1,fecha2)
  }

  def ParseYYYYMMDDtoDate(fecha:String):String = {
    LocalDate.parse(fecha,YYYYMMDDFormatter).format(dateFormatter)
  }

  val now_date = getToday()
  val now_day = getDay(now_date)
  val now_month = getMonth(now_date)
  val now_year = getYear(now_date)

}
