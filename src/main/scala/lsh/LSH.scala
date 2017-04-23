package lsh

import org.apache.spark.rdd.RDD

/**
  * Created by usuario on 22/04/2017.
  */
class LSH[T](shingleSize: Int, numHash: Int, numBands: Int) extends Serializable with Function1[RDD[(T, String)], RDD[List[T]]]  {
  val r = scala.util.Random
  val randInts = (0 :: List.fill(numHash - 1)(r.nextInt)).zipWithIndex

  def apply(data: RDD[(T, String)]): RDD[List[T]] = {
    val signatures: RDD[((Int, T), (Int, Int))] =
      data.flatMap({ case (id, text: String) => {
        val shingleList = text.sliding(shingleSize).toList
        randInts.map({ case (h, i) => ((i % numBands, id), (i, shingleList.map(_.hashCode() ^ h).min))})
      }
    })

    val clusters: RDD[((Int, Int), T)] =
      signatures.groupByKey().map({case (k,v) => ((k._1, v.toList.sortWith(_._1 <= _._1).map(_._2).hashCode()), k._2)})

    clusters.groupBy(_._1).map(_._2.toList.map(_._2))
  }

}
