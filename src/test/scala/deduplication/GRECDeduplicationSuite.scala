package deduplication

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by usuario on 23/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class GRECDeduplicationSuite extends FunSuite {

  trait ArticlesPool {
    val article1 = JournalArticle(7608,"2002","Un modelo interactivo ubicuo aplicado al patrimonio natural y cultural del area del Montsec","5",
      "Sendín M, Granollers T, Lorés J, Aguiló C, Balaguer A.","16","0","43","48","","AA","Article d'investigació","1137-3601","906",
      "Inteligencia Artificial. Revista Iberoamericana de Inteligencia Artificial","0","0","",0)
    val article2 = JournalArticle(1545,"2002","Un modelo interactivo ubícuo aplicado al patrimonio natural y cultural del area del Montsec","5",
      "Sendín M, Granollers T, Lorés J, Aguiló C, Balaguer A.","16","0","","","","AA","Article d'investigació","1137-3601","906",
      "Inteligencia Artificial. Revista Iberoamericana de Inteligencia Artificial","0","0","",0)
    val article3 = JournalArticle(1545,"2002","Un modelo interactivo ubicuo aplicado al patrimonio natural y cultural del area del Montsant","4",
      "Sendín M, Granollers T, Aguiló C, Oliva M.","16","0","43","48","","AA","Article d'investigació","1137-3601","906",
      "Inteligencia Artificial. Revista Iberoamericana de Inteligencia Artificial","0","0","",0)
  }

  test("near duplicate with half equal fields"){
    val article1 = JournalArticle(1, "2000", "Title", "5", "five authors", "1", "1", "1", "2", "DOI", "A", "Journal Type Description", "1", "1", "Journal Description", "1", "0", "classification", 0)
    val article2 = JournalArticle(1, "2000", "Title", "5", "five authors", "1", "1", "1", "2", "BAD DOI", "A", "Journal Type Description", "1", "0", "", "0", "0", "classification", 0)
    assert(article1.isNearDuplicate(article2))
  }

  test("near duplicate less than half equal fields"){
    val article1 = JournalArticle(1, "2000", "Title", "5", "five authors", "1", "1", "1", "2", "DOI", "A", "Journal Type Description", "1", "1", "Journal Description", "1", "0", "classification", 0)
    val article2 = JournalArticle(5, "2001", "Title", "4", "four authors", "0", "0", "0", "1", "BAD DOI", "A", "", "1", "0", "", "0", "0", "1", 0)
    assert(!article1.isNearDuplicate(article2))
  }

  test("duplicate article"){
    new ArticlesPool {
      assert(article1.isNearDuplicate(article2))
    }
  }
}
