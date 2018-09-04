package cn.maitian.test

object StudyTest {
      def Test(): Unit ={
        val oneTwo = List(1, 2)
        val threeFour = List(3, 4).::(7)
        val oneTwoThreeFour = oneTwo ::: threeFour
        println(oneTwoThreeFour)
        val twoThree = List(2, 3)
        val oneTwoThree =3:: 1 :: twoThree:::threeFour
        println(oneTwoThree)
        val oneTwoThreeN = 1 :: 2 :: 3 :: Nil
        println(oneTwoThreeN)
        val thrill = "Will"::"fill"::"until"::Nil
        println(thrill.exists(s => s == "until"))
        println(thrill.exists(_ == "until"))

        //元组tuple
        val pair = (99, "Luftballons")
        println(pair._1)
        println(pair._2)

        //特质 trait


        val romanNumeral = Map( 1 -> "I", 2 -> "II", 3 -> "III", 4 -> "IV", 5 -> "V" ) //->隐式转换
      }
  def main(args: Array[String]): Unit = {


    val s="abcdef123"
    for(c<-s){
      println(c.toByte)
    }
  }
}
