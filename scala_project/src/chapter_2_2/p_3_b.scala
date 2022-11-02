package chapter_2_2

object p_3_b extends App {
  import scala.io.StdIn.readLine

  println("Введите размер ЗП (руб.):")
  val my_salary: Int = readLine().toInt

  println("Введите размер премии (%):")
  val bonus: Float = readLine().toFloat

  println("Введите размер компенсации за питание (руб.):")
  val eat: Int = readLine().toInt

  println("Размер налога на ЗП в РФ - 13%.")

  println("Размер ЗП в месяц после вычета налога:")
  println(((my_salary * (1 + bonus / 100) + eat) / 12) * 0.87)

}
