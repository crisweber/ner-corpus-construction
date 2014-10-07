/**
 * Created by cristoferweber on 8/30/14.
 * export SPARK_CLASSPATH=/Users/cristoferweber/.ivy2/cache/info.bliki.wiki/bliki-core/jars/bliki-core-3.0.19.jar:/Users/cristoferweber/.ivy2/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar:/Users/cristoferweber/.ivy2/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar:/Users/cristoferweber/.ivy2/cache/org.apache.commons/commons-compress/jars/commons-compress-1.4.1.jar:/Users/cristoferweber/.ivy2/cache/commons-logging/commons-logging/jars/commons-logging-1.0.4.jar:/Users/cristoferweber/.ivy2/cache/commons-codec/commons-codec/jars/commons-codec-1.2.jar:/Users/cristoferweber/.ivy2/cache/org.tukaani/xz/jars/xz-1.0.jar:/Users/cristoferweber/.m2/repository/br/pucrs/ppgcc/cclr/wikimedia/1.0.0/wikimedia-1.0.0.jar
 * export SPARK_DRIVER_MEMORY=4g
 * bin/spark-shell
 */

import org.apache.hadoop.io.Text
import br.pucrs.ppgcc.cclr.wikimedia.ParseArticleBody
import br.pucrs.ppgcc.cclr.wikimedia.TitleTransformer

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._

val conf = new SparkConf().setAppName("processResources").setMaster("local").set("spark.executor.memory", "4g")
val sc = new SparkContext(conf)

val pattern = """^.+/(.+)$""".r
val expectedClasses = Set("Person", "Organisation", "Place")

type ArticleTransformation = (String, String) => (String, String)
type replacement = ((String, String), String)

def className(iri:String): String = {
  val pattern(cName) = iri
  cName
}

def dropBrackets(iri:String): String = {
  iri filterNot {Set('<', '>').contains}
}

def rdfObjects(triple:String): (String, String) = {
  triple split "\\s+"  match {case Array(s, _, o, _) => (dropBrackets(s), dropBrackets(o))}
}

def wikiAsPlain(language:String): ArticleTransformation = {
  val converter = new ParseArticleBody(language)
  (title, body) => {
    (title, converter asPlainText body)
  }
}

def titleAsURI(language:String): ArticleTransformation = {
  val titleTransformer = new TitleTransformer(language)
  (title, body) => {
    (titleTransformer transformToIRI title, body)
  }
}

def filterEmpty(body:String): Boolean = {
  !body.equals("")
}

def filterRedirect(body:String): Boolean = {
  !(body.startsWith("#REDIRECT") || body.startsWith("#Redirect"))
}

def allForms(text: String): Array[(String, String)] = {
  val links = """\{\[(.+?\|.+?)\]\}""".r
  val inst = """(\{\[)(.+?)\|(.+?)(\]\})""".r
  links.findAllIn(text).toArray.map {case inst(_, form, instance, _) => (instance, form)}
}

@scala.annotation.tailrec
def doReplaceInArticle(articleText: String, replacements: List[replacement]): String = replacements.headOption match {
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

  pairs.foldLeft(articleText){case (text, (from, to)) => text.replaceAllLiterally(from,to.toString)}
}

/* DBPEDIA CLASS INSTANCES */

val selectedClassesFile = sc textFile "/Users/cristoferweber/Documents/mestrado/pesquisa/WikipediaSilverCorpus/Português/instance_types_pt.ttl"
val selectedDBpediaClasses = selectedClassesFile.filter(!_.startsWith("#")).map(rdfObjects).filter({case(_, o) => o contains "dbpedia"})
val selectedClasses = selectedDBpediaClasses.map({case(s, o) => (s, className(o))}).filter({case(_, o) => expectedClasses contains o})


/* REDIRECTS */
val redirectPairsFile = sc textFile "/Users/cristoferweber/Documents/mestrado/pesquisa/WikipediaSilverCorpus/Português/redirects_transitive_pt.ttl"
val redirectPairs = redirectPairsFile map(rdfObjects(_).swap)

val instancesClasses = redirectPairs join selectedClasses map {case(_, redirects) => redirects} union selectedClasses
instancesClasses cache()

/* WIKIPEDIA ARTICLES */

/* Mantém somente artigos com referência às instâncias das classes selecionadas */
val wikiPagesFile = sc textFile "/Users/cristoferweber/Documents/mestrado/pesquisa/WikipediaSilverCorpus/Português/wikipedia_links_pt.ttl"
val wikiPages = wikiPagesFile.filter(_ contains "isPrimaryTopicOf").map(rdfObjects) // (dbpedia, wikipedia)

val wikiLinksFile = sc.textFile("/Users/cristoferweber/Documents/mestrado/pesquisa/WikipediaSilverCorpus/Português/page_links_pt.ttl")
val wikiLinks = wikiLinksFile.filter(!_.startsWith("#")).map(rdfObjects(_).swap).partitionBy(new HashPartitioner(16))
val linkedClasses = wikiLinks.join(instancesClasses).map({case(_, classes) => classes}).distinct(10) // (dbpedia,class)

val selectedPages = linkedClasses.join(wikiPages).map({case(dbpedia, (selectedClass, wikipage)) => (dbpedia, 0)}).distinct()

/* Remove marcações da wiki markup language, mantendo somente marcações dos links internos. */
val portugueseParser = wikiAsPlain("pt")

val transformTitle = titleAsURI("pt")

val wikiDump = sc.sequenceFile("/Users/cristoferweber/Downloads/part-r-00000", classOf[Text], classOf[Text])
val wikiArticles = wikiDump.map({case(t,b) =>(t.toString, b.toString)}).filter({case(_, b) => filterRedirect(b)}).map({case (t, b) => transformTitle(t, b)})

val selectedArticles = wikiArticles.join(selectedPages).map({case(title, (body, _)) => portugueseParser(title, body)})
val plainArticles = selectedArticles.filter({case (_, body) => filterEmpty(body) && filterRedirect(body)})

/* Extrai instâncias com formas textuais para compor lista de nomes alternativos */

val allNames = plainArticles map {case (uri, text) => (uri, (allForms(text).distinct, text))}

val mentions = allNames flatMap {case (iri, (forms, text)) => forms map {form => (iri, form)} } map {case (iri, (instance, form)) => (instance, (iri, form))}

val mentionTypes = mentions join instancesClasses map {case (instance, ((iri, form), typeClass)) => (iri, ((form, instance), typeClass))}

val replacements = mentionTypes groupByKey()

val articlesAndReplacements = allNames join replacements map {case (iri, ((_, text), replaces)) => (iri, (text, replaces.toList.sortBy(-_._2.length)))}

val v = articlesAndReplacements map {case (iri, (text, replaceList)) => (iri, dropRemaining(doReplaceInArticle(text, replaceList)) )}



val names = allNames flatMap {case (uri, (text, forms)) => forms} distinct()
names.cache()

val sortedNames = names sortByKey(true, 1)

val rdfNames = sortedNames map {case (instance, form) => s"""$instance rdfs:label "$form" ."""}

rdfNames saveAsTextFile "/Users/cristoferweber/Documents/mestrado/pesquisa/WikipediaSilverCorpus/Português/alt_names_pt.ttl"

val namesClasses = names join instancesClasses map {case (iri, (name, dbpediaClass)) => (name, (iri, dbpediaClass))}

val kv = namesClasses.collect()





/*
* (name,(IRI,class)) local
* <[name|IRI]> texts
* */
