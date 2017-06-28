import scala.util.Random

val rand = new Random(34234)

def expandTuple(t: ((Int, Int), Int)) = Seq(t._1._1,  t._1._2, t._2)

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