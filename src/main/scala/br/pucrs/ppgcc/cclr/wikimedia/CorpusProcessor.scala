package br.pucrs.ppgcc.cclr.wikimedia

import br.pucrs.acad.ppgcc.cclrner.{PrepareSentences, SimplifyLink}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object CorpusProcessor extends App {
  type ArticleTransformation = (String, String) => (String, String)
  type replacement = ((String, String), String)
  val pattern = """^.+/(.+)$""".r
  val expectedClasses = Set("Person", "Organisation", "Place")

    val sc = new SparkContext(new SparkConf().setAppName("Annotate Named Entities on Wikimedia texts"))

    val language = args(0)

    val baseWikimediaPath = args(1)
    val baseDBpediaPath = args(2)

    val outPath = args(3)

    val selectedClassesFile = sc textFile s"$baseDBpediaPath/instance_types_pt.ttl"
    val selectedDBpediaClasses = selectedClassesFile.filter(!_.startsWith("#")).map(rdfObjects).filter({case(_, o) => o contains "dbpedia"})
    val selectedClasses = selectedDBpediaClasses.map({case(s, o) => (s, className(o))}).filter({case(_, o) => expectedClasses contains o})


    /* REDIRECTS */
    val redirectPairsFile = sc textFile s"$baseDBpediaPath/redirects_transitive_pt.ttl"
    val redirectPairs = redirectPairsFile map(rdfObjects(_).swap)

    val instancesClasses = redirectPairs join selectedClasses map {case(_, redirects) => redirects} union selectedClasses
    instancesClasses cache()

    /* WIKIPEDIA ARTICLES */
    /* Mantém somente artigos com referência às instâncias das classes selecionadas */

    val wikiPagesFile = sc textFile s"$baseDBpediaPath/wikipedia_links_pt.ttl"
    val wikiPages = wikiPagesFile.filter(_ contains "isPrimaryTopicOf").map(rdfObjects) // (dbpedia, wikipedia)

    val wikiLinksFile = sc textFile s"$baseDBpediaPath/page_links_pt.ttl"
    val wikiLinks = wikiLinksFile.filter(!_.startsWith("#")).map(rdfObjects(_).swap) //.partitionBy(new HashPartitioner(16))
    val linkedClasses = wikiLinks.join(instancesClasses).map({ case (_, classes) => classes}).distinct() // (dbpedia,class)

    val selectedPages = linkedClasses.join(wikiPages).map({case(dbpedia, (selectedClass, wikipage)) => (dbpedia, 0)}).distinct()

    /* Converte títulos em IRIs da DBpedia */
    val transformTitle = titleAsURI(language)
    val wikiDump = sc.sequenceFile(s"$baseWikimediaPath/part*", classOf[Text], classOf[Text])
    val wikiArticles = wikiDump.map({case(t,b) =>(t.toString, b.toString)}).filter({case(_, b) => filterRedirect(b)}).map({case (t, b) => transformTitle(t, b)})

    /* Remove marcações da wiki markup language, mantendo somente marcações dos links internos. */
    val portugueseParser = wikiAsPlain(language)
    val selectedArticles = wikiArticles.join(selectedPages).map({case(title, (body, _)) => portugueseParser(title, body)})
    val plainArticles = selectedArticles.filter({case (_, body) => filterEmpty(body) && filterRedirect(body)})

    /* Extrai instâncias com formas textuais para compor lista de nomes alternativos */
    val allNames = plainArticles map {case (uri, text) => (uri, (allForms(text).distinct, text))}
    val mentions = allNames flatMap {case (iri, (forms, text)) => forms map {form => (iri, form)} } map {case (iri, (instance, form)) => (instance, (iri, form))}
    val mentionTypes = mentions join instancesClasses map {case (instance, ((iri, form), typeClass)) => (iri, ((form, instance), typeClass))}
    val replacements = mentionTypes groupByKey()

    // Substituições da maior ocorrência para a menor ocorrência
    val articlesAndReplacements = allNames join replacements map {case (iri, ((_, text), replaces)) => (iri, (text, replaces.toList.sortBy(-_._2.length)))}
    val result = articlesAndReplacements map {case (iri, (text, replaceList)) => (iri, dropRemaining(doReplaceInArticle(text, replaceList)) )}

    val simplifier = simplifyLink()

    val prepare = prepareSentences()

    val sentences = result.map({case(title, text) => simplifier(title, text) }).map({case(title, text) => prepare(title, text)._2})

    sentences saveAsTextFile s"$outPath/$language-sentences"
  sc.stop()

  def className(iri: String): String = {
    val pattern(cName) = iri
    cName
  }

  def rdfObjects(triple: String): (String, String) = {
    triple split "\\s+" match {
      case Array(s, _, o, _) => (dropBrackets(s), dropBrackets(o))
    }
  }

  def dropBrackets(iri: String): String = {
    iri filterNot {
      Set('<', '>').contains
    }
  }

  def wikiAsPlain(language: String): ArticleTransformation = {
    val converter = new ParseArticleBody(language)
    (title, body) => {
      (title, converter asPlainText body)
    }
  }

  def titleAsURI(language: String): ArticleTransformation = {
    val titleTransformer = new TitleTransformer(language)
    (title, body) => {
      (titleTransformer transformToIRI title, body)
    }
  }

  def filterEmpty(body: String): Boolean = {
    !body.equals("")
  }

  def filterRedirect(body: String): Boolean = {
    !(body.startsWith("#REDIRECT") || body.startsWith("#Redirect"))
  }

  def allForms(text: String): Array[(String, String)] = {
    val links = """\{\[(.+?\|.+?)\]\}""".r
    val inst = """(\{\[)(.+?)\|(.+?)(\]\})""".r
    links.findAllIn(text).toArray.map { case inst(_, form, instance, _) => (instance, form)}
  }

  @scala.annotation.tailrec
  final def doReplaceInArticle(articleText: String, replacements: List[replacement]): String = replacements.headOption match {
    case Some(((form, instance), typeClass)) => {
      val toMatch = s"""{[$form|$instance]}"""
      val toReplace = s"""<[$form|$typeClass]>"""

      doReplaceInArticle(articleText replaceAllLiterally(toMatch, toReplace), replacements.tail)
    }
    case None => articleText
  }

  def dropRemaining(articleText: String): String = {
    val links = """\{\[(.+?\|.+?)\]\}""".r
    val inst = """(\{\[)(.+?)\|(.+?)(\]\})""".r

    def f(matchText: String): (String, String) = {
      val inst(open, form, iri, close) = matchText
      (matchText, form)
    }

    val pairs = links.findAllIn(articleText).toArray.map(f)

    pairs.foldLeft(articleText) { case (text, (from, to)) => text.replaceAllLiterally(from, to)}
  }

  def simplifyLink(): ArticleTransformation = {
    val simplifier = new SimplifyLink

    (title, text) => {
      (title, simplifier map text)
    }
  }

  def prepareSentences(): ArticleTransformation = {
    val prepare = new PrepareSentences

    (title, text) => {
      (title, prepare map text)
    }
  }
}
