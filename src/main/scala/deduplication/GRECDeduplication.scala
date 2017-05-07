package deduplication

import java.util.Calendar

import info.debatty.java.stringsimilarity.NormalizedLevenshtein
import lsh.LSH
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

/**
  * Created by usuario on 21/03/2017.
  */
case class JournalArticle(code: Int, publicationYear: String, title: String, nAuthors: String, authors: String,
                          volume: String, numJournal: String, iniPage: String, endPage: String, DOI: String,
                          articleType: String, articleTypeDesc: String, issn: String, journalCode: String, journalDesc: String,
                          isiCode: String, impactFactor: String, classification: String, sameAs: BigInt) {
  // Article info
  val PublicationYearWeight = 25
  val TitleWeight = 30
  val NAuthorsWeight = 10
  val AuthorsWeight = 30
  val ArticleTypeWeight = 5
  val ArticleInfoThreshold = 0.6

  // Article in journal
  val VolumeWeight = 10
  val NumJournalWeight = 10
  val IniPageWeight = 20
  val EndPageWeight = 20
  val DOIWeight = 40
  val ArticleInJournalThreshold = 0.6

  // Journal
  val ISSNWeight = 30
  val JournalCodeWeight = 30
  val JournalDescriptionWeight = 10
  val IsiCodeWeight = 30
  val JournalThreshold = 0.6

  def isNearDuplicate(article: JournalArticle): Boolean = {
    def sameArticleInfo: Boolean = {
      var mark = 0.0
      var maxMark = 0

      if(this.publicationYear != null && article.publicationYear != null){
        maxMark += PublicationYearWeight
        if(this.publicationYear == article.publicationYear) mark += PublicationYearWeight
      }


      if(this.title != null && article.title != null){
        maxMark += TitleWeight
        mark += (1 - new NormalizedLevenshtein().distance(this.title, article.title)) * TitleWeight
      }

      if(this.nAuthors != null && article.nAuthors != null){
        maxMark += NAuthorsWeight
        if(this.nAuthors == article.nAuthors) mark += NAuthorsWeight
      }

      if(this.authors != null && article.authors != null){
        maxMark += AuthorsWeight
        mark += (1 - new NormalizedLevenshtein().distance(this.authors, article.authors)) * AuthorsWeight
      }

      if((this.articleType != null && article.articleType != null)
        || (this.articleTypeDesc != null && article.articleTypeDesc != null)){
        maxMark += ArticleTypeWeight
        if(this.articleType == article.articleType || this.articleTypeDesc == article.articleTypeDesc) mark += ArticleTypeWeight
      }

      maxMark == 0 || mark / maxMark > ArticleInfoThreshold
    }

    def sameArticleInJournal: Boolean = {
      var mark = 0.0
      var maxMark = 0

      if(this.volume != null && article.volume != null){
        maxMark += VolumeWeight
        if(this.volume == article.volume) mark += VolumeWeight
      }

      if(this.numJournal != null && article.numJournal != null){
        maxMark += NumJournalWeight
        if(this.numJournal == article.numJournal) mark += NumJournalWeight
      }

      if(this.iniPage != null && article.iniPage != null){
        maxMark += IniPageWeight
        if(this.iniPage == article.iniPage) mark += IniPageWeight
      }

      if(this.endPage != null && article.endPage != null){
        maxMark += EndPageWeight
        if(this.endPage == article.endPage) mark += EndPageWeight
      }

      if(this.DOI != null && article.DOI != null){
        maxMark += DOIWeight
        if(this.DOI == article.DOI) mark += DOIWeight
      }

      maxMark == 0 || mark / maxMark > ArticleInJournalThreshold
    }

    def sameJournal: Boolean = {
      var mark = 0.0
      var maxMark = 0

      if(this.issn != null && article.issn != null){
        maxMark += ISSNWeight
        if(this.issn == article.issn) mark += ISSNWeight
      }

      if(this.journalCode != null && article.journalCode != null){
        maxMark += JournalCodeWeight
        if(this.journalCode == article.journalCode) mark += JournalCodeWeight
      }

      if(this.journalDesc != null && article.journalDesc != null){
        maxMark += JournalDescriptionWeight
        mark += (1 - new NormalizedLevenshtein().distance(this.journalDesc, article.journalDesc)) * JournalDescriptionWeight
      }

      if(this.isiCode != null && article.isiCode != null){
        maxMark += IsiCodeWeight
        if(this.isiCode == article.isiCode) mark += IsiCodeWeight
      }

      maxMark == 0 || mark / maxMark > JournalThreshold
    }

    sameArticleInfo && sameArticleInJournal && sameJournal
  }
}

case class JournalAuthor(code: Integer, nif: String, name: String)

case class IndexArticle(index: Long, article: JournalArticle)

case class CandidateArticles(article1: JournalArticle, article2: JournalArticle)


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
    //deduplicatedArticles.orderBy("sameAs").write.csv("src/main/resources/result.csv")

    //rawArticles.write.csv("test")

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
      .withColumnRenamed("TIPUS", "articleType")
      .withColumnRenamed("TIPUS_DESC", "articleTypeDesc")
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
      .withColumn("publicationYear", extractYear(articlesFilledNa("publicationYear")))
      .withColumn("nAuthors", articlesFilledNa("nAuthors"))
      .withColumn("volume", articlesFilledNa("volume"))
      .withColumn("journalCode", articlesFilledNa("journalCode"))
      .withColumn("isiCode", articlesFilledNa("isiCode"))
      .withColumn("impactFactor", formatDouble(articlesFilledNa("impactFactor")))

    val journalAuthors = articles.select("nif", "name", "code").as[JournalAuthor]
    val journalArticles = articles.drop("nif").drop("name").dropDuplicates().as[JournalArticle].sample(false, 0.6)

    (journalAuthors, journalArticles)
  }

  def getConnectedGraphs(edges: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    val graph = Graph.fromEdgeTuples(edges.map(duplicate => (duplicate._1.toLong, duplicate._2.toLong)), "")
    val cc = graph.connectedComponents()
    cc.vertices.map(x => (x._1.toInt, x._2.toInt))
  }

  def compareAndGetDuplicates(lsh: RDD[List[Int]], articles: Dataset[JournalArticle]): RDD[(Int, Int)] = {
    val distinctLsh = lsh.flatMap(candidates => candidates.combinations(2))
      .map(candidatePair => if(candidatePair.head > candidatePair.last) (candidatePair.last, candidatePair.head)
                            else (candidatePair.head, candidatePair.last))
      .distinct().toDF("id1", "id2")
    val id1Join = distinctLsh.joinWith(articles, $"id1" === $"code").toDF("id1", "article1")
    val id2Join = distinctLsh.joinWith(articles, $"id2" === $"code").toDF("id2", "article2")
    id1Join.join(id2Join, $"id1" === $"id2").drop("id1", "id2").as[CandidateArticles]
    .filter(candidatePair => candidatePair.article1.isNearDuplicate(candidatePair.article2))
      .map(duplicate => (duplicate.article1.code, duplicate.article2.code)).rdd
  }

  def obtainDuplicateReference(articles: Dataset[JournalArticle]): RDD[(Int, Int)] = {
    val lsh_buckets: RDD[List[Int]] = lsh(articles.rdd.map(a => (a.code, a.title)))
    val duplicates = compareAndGetDuplicates(lsh_buckets, articles)
    val similarArticles = getConnectedGraphs(duplicates)
    similarArticles
  }

  def applyDeduplicateReference(articles: Dataset[JournalArticle], duplicateReferences: RDD[(Int, Int)]): Dataset[JournalArticle] = {
    val deduplicated = articles.drop("sameAs").join(duplicateReferences.toDF("code", "sameAs"), Seq("code"), "left_outer").as[JournalArticle]
    deduplicated
  }
}
