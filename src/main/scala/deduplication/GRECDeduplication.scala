package deduplication

import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{lit, udf}

/**
  * Created by usuario on 21/03/2017.
  */
case class JournalArticle(code: Integer, publicationYear: Integer, title: String, nAuthors: Integer, authors: String,
                          volume: Integer, numJournal: String, iniPage: String, endPage: String, DOI: String,
                          journalType: String, journalTypeDesc: String, issn: String, journalCode: Integer, journalDesc: String,
                          isiCode: Integer, impactFactor: Double, classification: String, sameAs: Integer){
  def isNearDuplicate(article: JournalArticle): Boolean = {
    val numEqualFields = this.productIterator.zip(article.productIterator).count((articles) => articles._1 == articles._2)
    numEqualFields >= this.productArity / 2
  }
}

case class JournalAuthor(code: Integer, nif: String, name: String)

case class IndexArticle(index: Long, article: JournalArticle)

case class CartessianIndexArticles(index1: Long, article1: JournalArticle, index2: Long, article2: JournalArticle)

object GRECDeduplication {

  val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val session: SparkSession = SparkSession.builder().getOrCreate()
  import session.implicits._

  def getRawArticles: DataFrame = session.read.option("header", "true")
                                                    .option("charset", "UTF8")
                                                    .option("delimiter",";")
                                                    .csv("F:\\Users\\Albert\\IdeaProjects\\GRECDeduplication\\src\\main\\resources\\VCAMIBER_PUBLICACIONS_REVISTES_DPT1605.csv")

  def cleanData(data: DataFrame): (Dataset[JournalAuthor], Dataset[JournalArticle]) = {
    val articlesRenamedCols = data.withColumnRenamed("NIF", "nif")
      .withColumnRenamed("NOM", "name")
      .withColumnRenamed("CODI", "code")
      .withColumnRenamed("ANY_PUBLICACIO", "publicationYear")
      .withColumnRenamed("TITOL", "title")
      .withColumnRenamed("NAUTORS", "nAuthors")
      .withColumnRenamed("AUTORS", "authors")
      .withColumnRenamed("VOLUM", "volume")
      .withColumnRenamed("NUM_REVISTA", "numJournal")
      .withColumnRenamed("PINI", "iniPage")
      .withColumnRenamed("PFI", "endPage")
      .withColumnRenamed("TIPUS", "journalType")
      .withColumnRenamed("TIPUS_DESC", "journalTypeDesc")
      .withColumnRenamed("ISSN", "issn")
      .withColumnRenamed("REVISTA_CODI", "journalCode")
      .withColumnRenamed("REVISTA_DESC", "journalDesc")
      .withColumnRenamed("CODI_ISI", "isiCode")
      .withColumnRenamed("FACTOR_IMPACTE", "impactFactor").drop("FACTOR_IMPACTE")
      .withColumnRenamed("CLASSIFICACIO", "classification")
      .withColumn("sameAs", lit(0))

    val articlesFilledNa = articlesRenamedCols.na.fill("-1", Array("volume", "numJournal", "iniPage", "endPage", "journalCode", "isiCode", "impactFactor"))

    val extractYear = udf[String, String]("(\\d{4})".r.findFirstMatchIn(_).map(_ group 1).getOrElse("0"))
    val formatDouble = udf[String, String]( _.replace(",", "."))

    val articles = articlesFilledNa
      .withColumn("code", articlesFilledNa("code").cast(IntegerType))
      .withColumn("publicationYear", extractYear(articlesFilledNa("publicationYear")).cast(IntegerType))
      .withColumn("nAuthors", articlesFilledNa("nAuthors").cast(IntegerType))
      .withColumn("volume", articlesFilledNa("volume").cast(IntegerType))
      .withColumn("journalCode", articlesFilledNa("journalCode").cast(IntegerType))
      .withColumn("isiCode", articlesFilledNa("isiCode").cast(IntegerType))
      .withColumn("impactFactor", formatDouble(articlesFilledNa("impactFactor")).cast(DoubleType))

    val journalAuthors = articles.select("nif", "name", "code").as[JournalAuthor]
    val journalArticles = articles.drop("nif").drop("name").dropDuplicates().as[JournalArticle]

    (journalAuthors, journalArticles)
  }

  def main(args: Array[String]) {
    val rawArticles = getRawArticles

    val (authors, articles): (Dataset[JournalAuthor], Dataset[JournalArticle]) = cleanData(rawArticles)
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
