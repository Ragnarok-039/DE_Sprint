package chapter_2_2

object p_3_c extends App {

  val salaries: List[Float] = List(100, 150, 200, 80, 120, 75)
  val avg: Float = salaries.sum / salaries.length

  for (i <- salaries)
    if (i >= avg) {val num: Float = (i / avg - 1) * 100
      println(s"Зарплата сотрудника - ${i.toInt} больше, чем средняя на ${num.round} %")}
    else {val num: Float = (avg / i - 1) * 100
      println(s"Зарплата сотрудника - ${i.toInt} меньше, чем средняя на ${num.round} %")}

}
