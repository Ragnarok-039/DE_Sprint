package chapter_2_2

object p_3_g extends App {

  val my_list: List[Int] = List(100, 150, 200, 80, 120, 75, 30, 145, 45, 55, 95, 187, 354, 15, 180, 250, 210)
  val mid: Int = 120
  var step: Int = 0

  for (i <- my_list) {
    step += 1
    if (i >= mid) {
      println(s"ЗП: $i. Номер сотрудника: $step.")
    }
  }

}
