package chapter_2_2

object p_3_d extends App {

  val salaries: List[Double] = List(100, 150, 200, 80, 120, 75)
  val first: Double = salaries.head * 1.2
  val new_salaries: List[Double] = first +: salaries.drop(1)

  println(new_salaries)

}
