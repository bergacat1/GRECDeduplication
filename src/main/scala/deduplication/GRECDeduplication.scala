package deduplication

import lsh.LSH
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

/**
  * Created by usuario on 21/03/2017.
  */
case class JournalArticle(code: Integer, publicationYear: Integer, title: String, nAuthors: Integer, authors: String,
                          volume: Integer, numJournal: String, iniPage: String, endPage: String, DOI: String,
                          journalType: String, journalTypeDesc: String, issn: String, journalCode: Integer, journalDesc: String,
                          isiCode: Integer, impactFactor: Double, classification: String, sameAs: BigInt) {
  def isNearDuplicate(article: JournalArticle): Boolean = {
    val numEqualFields = this.productIterator.zip(article.productIterator).count((articles) => articles._1 == articles._2)
    numEqualFields >= this.productArity / 2
  }
}

case class JournalAuthor(code: Integer, nif: String, name: String)

case class IndexArticle(index: Long, article: JournalArticle)

case class CartessianIndexArticles(index1: Long, article1: JournalArticle, index2: Long, article2: JournalArticle)

object GRECDeduplication {
  val session: SparkSession = SparkSession
    .builder()
    .appName("GRECDeduplication")
    .config("spark.master", "local")
    .config("spark.driver.maxResultSize", "8g")
    .getOrCreate()

  import session.implicits._

  val DivisionSize = 100

  val lsh = new LSH[Int](9, 200, 20)

  def main(args: Array[String]) {
    deduplicationProcess()
  }

  def deduplicationProcess(): Unit = {
    val rawArticles: DataFrame = session.read.option("header", "true")
      .option("charset", "UTF8")
      .option("delimiter", ";")
      .csv("src/main/resources/VCAMIBER_PUBLICACIONS_REVISTES_DPT1605.csv")

    val (authors, articles): (Dataset[JournalAuthor], Dataset[JournalArticle]) = cleanData(rawArticles)


    val duplicateReferences = obtainDuplicateReference(articles)

    val deduplicatedArticles = applyDeduplicateReference(articles, duplicateReferences)

    deduplicatedArticles.filter(_.sameAs != null).show()
  }

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
    val formatDouble = udf[String, String](_.replace(",", "."))

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

  def getConnectedGraphs(lsh_buckets: RDD[List[Int]]): RDD[(Int, Int)] = {
    val edges: RDD[(VertexId, VertexId)] = lsh_buckets.flatMap(x => x.combinations(2).map(t => (t.head, t.last)))
    val graph = Graph.fromEdgeTuples(edges, "")
    val cc = graph.connectedComponents()
    cc.vertices.map(x => (x._1.toInt, x._2.toInt))
  }

  def obtainDuplicateReference(articles: Dataset[JournalArticle]): RDD[(Int, Int)] = {
    val lsh_buckets: RDD[List[Int]] = lsh(articles.rdd.map(a => (a.code, a.title)))
    val similarArticles = getConnectedGraphs(lsh_buckets)
    similarArticles
  }

  def applyDeduplicateReference(articles: Dataset[JournalArticle], duplicateReferences: RDD[(Int, Int)]): Dataset[JournalArticle] = {
    val deduplicated = articles.drop("sameAs").join(duplicateReferences.toDF("code", "sameAs"), Seq("code"), "left_outer").as[JournalArticle]
    deduplicated
  }
}
