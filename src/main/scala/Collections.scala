import scala.util.Random

/**
  * Created by jnachbar on 6/23/17.
  */
object Collections {
  val rand = new Random(34234)

  def expandTuple(t: ((Int, Int), Int)) = Seq(t._1._1,  t._1._2, t._2)
  def main(args: Array[String]): Unit = {
    val seq1 = Seq.fill(100)(rand.nextInt()/10)
    val seq2 = Seq.fill(100)(rand.nextInt()/10)
    val seq3 = Seq.fill(100)(rand.nextInt()/10)
    val combSeq = seq1.zip(seq2).zip(seq3)
    println(combSeq.head)
    val avSeq = combSeq.map(expandTuple).map(_.sum).map(_/3)
    //val avSeq = combSeq.map(value => (value._1._1 + value._1._2 + value._2) / 3)
    println(avSeq.head)
    val totalAvg = avSeq.sum/avSeq.length

    println(totalAvg)

    val mysum = avSeq.foldLeft(0)(_ + _)
    val mysum2 = avSeq.reduce(_ + _)
    val mysum3 = avSeq.tail.foldLeft(avSeq.head)(_ + _)


  }
}
