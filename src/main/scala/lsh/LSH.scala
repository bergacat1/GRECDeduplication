package lsh

import org.apache.spark.rdd.RDD

/**
  * Created by usuario on 22/04/2017.
  */
class LSH[T](shingleSize: Int, numHash: Int, numBands: Int) extends Function1[RDD[(T, String)], RDD[List[T]]] {
  val r = scala.util.Random
  val randInts = (0 :: List.fill(199)(r.nextInt)).zipWithIndex

  def apply(data: RDD[(T, String)]): RDD[List[T]] = {
    val signatures: RDD[((Int, T), (Int, Int))] =
      data.flatMap({ case (id, text: String) => {
        val shingleList = text.sliding(9).toList
        randInts.map({ case (h, i) => ((i % 50, id), (i, shingleList.map(_.hashCode() ^ h).min))})
      }
    })

    val clusters =
      signatures.groupByKey().map({case (k,v) => ((k._1, v.toList.sortWith(_._1 <= _._1).map(_._2).hashCode()), k._2)})

    clusters.groupByKey().map(_._2.toList)
  }

}
