package spannerlog.nlp

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable

class CoreNlpAnnotatorTests extends FlatSpec with Matchers {

  "1. a NER tool" should "recognize entities" in {
    val text = "Warren Buffett is so deeply tied with Coca-Cola " +
      "that one of his most personal anecdotes--that he at one " +
      "time drank five cans of Cherry Coke each day--is forever " +
      "linked with the company. Buffett added Coca-Cola stock to " +
      "Berkshire Hathaway's portfolio after the stock market crash " +
      "of 1987, buying up $1 billion or so in stock at a time when " +
      "prices were generally low."

    val textAnnotator = new CoreNlp("tokenize, ssplit, pos, lemma, ner", isOneSentence = false)
    val nes = textAnnotator.ner(text)
    println(nes.mkString("\n"))
    text.substring(nes.head.location.begin-1, nes.head.location.end-1) should equal ("Warren Buffett")
    text.substring(nes(1).location.begin-1, nes(1).location.end-1) should equal ("Coca-Cola")
    text.substring(nes.last.location.begin-1, nes.last.location.end-1) should equal ("$1 billion")
  }

  "2. a NER tool" should "recognize entities" in {
    val text = "Google Bought Waze For $1.1B Giving A Social Data Boost To Its Mapping Business"

    val textAnnotator = new CoreNlp("tokenize, ssplit, pos, lemma, ner", isOneSentence = false)
    val nes = textAnnotator.ner(text)
    nes.foreach(e => println(text.substring(e.location.begin-1, e.location.end-1) + " " + e))
//    text.substring(nes.head.location.begin-1, nes.head.location.end-1) should equal ("Warren Buffett")
//    text.substring(nes(1).location.begin-1, nes(1).location.end-1) should equal ("Coca-Cola")
//    text.substring(nes.last.location.begin-1, nes.last.location.end-1) should equal ("$1 billion")
  }

  "3. a sentence splitter tool" should "split sentences" ignore {
    val text = "Warren Buffett is so deeply tied with Coca-Cola " +
      "that one of his most personal anecdotes--that he at one " +
      "time drank five cans of Cherry Coke each day--is forever " +
      "linked with the company. Buffett added Coca-Cola stock to " +
      "Berkshire Hathaway's portfolio after the stock market crash " +
      "of 1987, buying up $1 billion or so in stock at a time when " +
      "prices were generally low."

    val textAnnotator = new CoreNlp("tokenize, ssplit", isOneSentence = false)
    val sentences = textAnnotator.sentence(text)
    println(sentences)
  }

  "4. a sentence splitter tool" should "split sentences" ignore {
    val text = "SAN JOSE CHIAPA, Mexico  (Reuters) - The automotive industry's growing love affair with Mexico was celebrated here on Saturday as Audi executives laid the foundation stone for its first assembly plant in the Americas.   Volkswagen AG's ( VOWG_p.DE ) premium brand is joining a parade of automakers who have announced plans to build cars in a country that is seen as a doorway not only to the rest of North and South America but to the world. Audi officials said the $1.3 billion plant will open in three years and eventually be the German brand's only source globally for its Q5 SUVs after it opens in mid-2016. \"Mexico was chosen very deliberately,\" Audi Chairman Rupert Stadler told more than 500 industry and government officials outside the town of San Jose Chiapa in central Mexico. \"It is situated between North and South America, making it a linchpin between the two regions.\" He added that Mexico was an \"ideal export base.\" With numerous free trade agreements, a cheap, well-educated labor force, and proximity to the lucrative U.S. auto market, combined with growing demand in South America, automakers have been lining up for two years to set up shop in a country that could eventually overtake Brazil as Latin America's biggest economy. Audi's ceremony came two days after Japanese automaker Honda Motor Co ( 7267.T ) said it would build a $470 million transmission plant in the central state of Guanajuato, near an $800 million assembly plant that is expected to begin operations in February 2014. Other automakers who have announced plans to open plants in Mexico include Mazda Motor Co ( 7261.T ) and Nissan Motor Co( 7201.T ), while companies already there - General Motors Co ( GM.N ) and Ford Motor Co ( F.N ) - continue to pump hundreds of millions into their plants. The new plants also are attracting supplier factories and more could be on the way as Nissan's Infiniti brand, BMW ( BMWG.DE ) and Hyundai ( 005380.KS ) are weighing the possibility of building plants in North America. When Nissan said in January 2012 that it would build a $2 billion plant in the central state of Aguascalientes to open in late 2013, CEO Carlos Ghosn called Mexico, where it has now two plants and is the market leader in sales, production and exports, a \"key engine\" to Nissan's growth in the Americas. The Japanese automaker exports to 115 countries from Mexico. Last year, Mexico attracted $3.7 billion in announced investments by automakers alone, matching the U.S. total, according to the Center for Automotive Research in Ann Arbor, Michigan. IHS Automotive estimated that investments by automakers in Mexico over the next few years could total $3 billion annually. From 2000 to 2013, vehicle production in Mexico has risen almost 3 percent annually, compared with declines in the United States and Canada of 1.3 percent and 2.4 percent, respectively, according to Boston Consulting Group. That trend will hold through 2018 as Mexico's production is forecast to grow 5 percent annually, compared with 3 percent growth in the United States and a 4 percent decline in Canada. Mexico is the eighth largest producer of vehicles in the world. Audi executives touted Mexico's good infrastructure, competitive cost structures and existing free-trade agreements in picking the site for the new plant, which will cover an area the size of 400 soccer fields. They called the Mexian plant a \"dream moment.\" VW has a VW plant in nearby Puebla City and an engine plant in Silao. VW has said it wants to boost sales in the United States, the world's No. 2 auto market, to 1 million vehicles by 2018, including 200,000 from Audi. It is the same time frame in which VW has pledged to become the world's largest automaker. Last year, VW's U.S. sales totaled more than 577,000, including 139,310 for Audi. The new plant in Mexico is also part of the automaker's plan for Audi to reach annual global sales of more than 2 million by 2020 with the aim of snatching the luxury crown from BMW globally as well as challenging its luxury rival and Daimler's ( DAIGn.DE ) Mercedes-Benz in the U.S. market. BMW and Mercedes have had production footprints in North America since the 1990s and each sell about twice as many cars in the United States as Audi. Access to the lucrative U.S. market isn't the only draw though as Mexico has 12 free trade agreements with 44 countries, while the United States' 14 trade deals cover only 20 countries. Xavier Mosquet, leader of Boston Consulting Group's automotive practice, said Mexico was the \"better choice\" over the United States for exporting to the rest of Latin America. Also in Mexico's favor are the rising labor costs in China, lower transportation costs and even Mexico's own growing economy, industry officials and analysts said. In 2010, Mexico's total compensation per worker was $3.94 an hour, said Kristin Dziczek, director of labor and industry at the Center for Automotive Research, citing Bureau of Labor Statistics data. That compared with $3.45 in China, $34.59 in the United States and $52.60 in Germany. And the Mexican work force is increasingly well educated. Audi officials say 2,500 applicants, many with degrees, have expressed interest in the 3,800 jobs they will have at the San Jose Chiapa plant and they haven't even advertised the jobs yet. Nomura said last August that Mexico could overtake Brazil as Latin America's top economy as early as 2022. Mexico's annual economic growth is projected to increase 4.5 percent in that period, up from 2.1 percent annual growth from 2002 to 2010. Mexico also serves as a natural hedge for many foreign automakers against stronger currencies at home or disasters like the tsunami in 2011 that hurt the Japanese automakers, IHS analyst Guido Vildozo said. \"It's probably one of the reasons why you're hearing that some of the other automakers are also kicking the tires,\" he said. (Reporting by Ben Klayman in Puebla, Mexico; editing by Gunna Dickson)"

    val textAnnotator = new CoreNlp("tokenize, ssplit", isOneSentence = false)
    val sentences = textAnnotator.sentence(text)
    println(sentences)
  }

  "5. a sentiment analysis tool" should "analyze sentiment" in {
    val text = "The best experience ever.\nThe worst experience ever.\nIt was bad.\nIt was good.\nIt was ok."

    val textAnnotator = new CoreNlp("tokenize, ssplit, pos, parse, sentiment", isOneSentence = false)
    val sentiments: immutable.Seq[SentimentAnnotation] = textAnnotator.sentiment(text)

    sentiments(0) should equal (SentimentAnnotation(Span(0,25),"Very positive",4))
    sentiments(1) should equal (SentimentAnnotation(Span(26,52),"Very negative",0))
    sentiments(2) should equal (SentimentAnnotation(Span(53,64),"Negative",1))
    sentiments(3) should equal (SentimentAnnotation(Span(65,77),"Positive",3))
    sentiments(4) should equal (SentimentAnnotation(Span(78,88),"Neutral",2))
  }

}
