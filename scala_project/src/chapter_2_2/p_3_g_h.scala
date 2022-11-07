package chapter_2_2

object p_3_g_h extends App {

  val my_list: List[Int] = List(100, 150, 200, 80, 120, 75, 30, 145, 45, 55, 95, 187, 354, 15, 180, 250, 210)
  val mid: Int = 120
  var step: Int = 0

  for (i <- my_list) {
    step += 1
    if (i >= mid) {
      println(s"ЗП: $i. Номер сотрудника: $step.")
    }
  }
  println()


  var new_list: List[Double] = List.empty
  for (i <- my_list) {
    var now = (i * 1.07).round
    new_list ::= now
  }

  println("ЗП до индексирования по возрастанию:")
  println(my_list.sorted)
  println("ЗП после индексирования по возрастанию:")
  println(new_list.sorted)

}
