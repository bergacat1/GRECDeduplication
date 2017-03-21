package deduplication

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by usuario on 21/03/2017.
  */
case class JournalArticle(code: Integer, publicationYear: Integer, title: String, nAuthors: Integer, authors: String, volume: Integer, numJournal: String, iniPage: String, endPage: String, DOI: String, journalType: String, journalTypeDesc: String, issn: String, journalCode: Integer, journalDesc: String, isiCode: Integer, impactFactor: Double, classification: String, sameAs: Integer)

case class JournalAuthor(code: Integer, nif: String, name: String)

case class IndexArticle(index: Long, article: JournalArticle)

case class CartessianIndexArticles(index1: Long, article1: JournalArticle, index2: Long, article2: JournalArticle)

object GRECDeduplication {

  val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val session: SparkSession = SparkSession.builder().getOrCreate()

  def getRawArticles: DataFrame = session.read.option("header", "true")
                                                    .option("charset", "UTF8")
                                                    .option("delimiter",";")
                                                    .csv("F:\\Users\\Albert\\IdeaProjects\\GRECDeduplication\\src\\main\\resources\\VCAMIBER_PUBLICACIONS_REVISTES_DPT1605.csv")

  def main(args: Array[String]) {
    val rawArticles = getRawArticles

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
