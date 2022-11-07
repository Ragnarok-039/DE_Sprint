package chapter_2_2

import scala.+:
import scala.collection.mutable.ListBuffer

object p_3_d_f extends App {

  //  ЗП из пункта b. Закрепил постоянным значением. Естественно, в пункте b может быть любым.
  val my_salary: Double = 175.0

  val salaries: List[Double] = List(100, 150, 200, 80, 120, 75)
  val new_salaries: List[Double] = my_salary +: salaries

  println(new_salaries)

  println(s"Самая высокая ЗП: ${new_salaries.max}")
  println(s"Самая низкая ЗП: ${new_salaries.min}")
  println()


  println("Добавили сотрудников с окладами 350 и 90 тысяч.")
  val new_workers: List[Double] = List(350, 90)
  val new_salaries_2: List[Double] = new_workers ++ new_salaries
  println("Отсортированный по возрастанию список ЗП:")
  println(new_salaries_2.sorted)
  println()


  println("Добавили сотрудника с окладом 130 тысяч.")
  var list_salaries_f = List.empty[Double]
  val worker_0: Double = 130
  var step: Int = 0
  var new_salaries_3 = new_salaries_2.sorted
  for (i <- new_salaries_3) {
      if (i <= worker_0) {
        step += 1
      }
  list_salaries_f = new_salaries_3.slice(0, step)
    list_salaries_f = list_salaries_f :+ worker_0
    list_salaries_f = list_salaries_f ++ new_salaries_3.slice(step, new_salaries_3.length)
  }
  println(list_salaries_f)
  println()
}
