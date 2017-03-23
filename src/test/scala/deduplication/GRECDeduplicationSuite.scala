package deduplication

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by usuario on 23/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class GRECDeduplicationSuite extends FunSuite {

  test("near duplicate with half equal fields"){
    val article1 = JournalArticle(1, 2000, "Title", 5, "five authors", 1, "1", "1", "2", "DOI", "A", "Journal Type Description", "1", 1, "Journal Description", 1, 0, "classification", 0)
    val article2 = JournalArticle(1, 2000, "Title", 5, "five authors", 1, "1", "1", "2", "BAD DOI", "A", "Journal Type Description", "1", 0, "", 0, 0, "classification", 0)
    assert(article1.isNearDuplicate(article2))
  }

  test("near duplicate less than half equal fields"){
    val article1 = JournalArticle(1, 2000, "Title", 5, "five authors", 1, "1", "1", "2", "DOI", "A", "Journal Type Description", "1", 1, "Journal Description", 1, 0, "classification", 0)
    val article2 = JournalArticle(5, 2001, "Title", 4, "four authors", 0, "0", "0", "1", "BAD DOI", "A", "", "1", 0, "", 0, 0, "1", 0)
    assert(!article1.isNearDuplicate(article2))
  }
}
