package chapter_2_2

import scala.collection.mutable.ListBuffer

object p_3_d extends App {

//  ЗП из пункта b. Закрепил постоянным значением. Естественно, в пункте b может быть любым.
  val my_salary: Double = 175.0

  val salaries: List[Double] = List(100, 150, 200, 80, 120, 75)
  val new_salaries: List[Double] = my_salary +: salaries

  println(new_salaries)

  println(s"Самая высокая ЗП: ${new_salaries.max}")
  println(s"Самая низкая ЗП: ${new_salaries.min}")
  println()

  val new_workers: List[Double] = List(350, 90)
  val new_salaries_2: List[Double] = new_workers ++ new_salaries
  println("Отсортированный по возрастанию список ЗП:")
  println(new_salaries_2.sorted)
  println()


  val list_salaries_f: ListBuffer[Double] = List()
  val worker_0: Double = 130
  for (i <- new_salaries_2.sorted) {
    if (i >= worker_0) {
      list_salaries_f += worker_0
    }
    list_salaries_f += i
  }
}
