package spannerlog.app_00.exp_a_persons;

import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Triple;
import org.apache.log4j.BasicConfigurator;
import edu.stanford.nlp.simple.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CoreNLPDemo {

    public void runSimple() {
//        Sentence sent = new Sentence("Lucy is in the sky with diamonds.");
//        List<String> nerTags = sent.nerTags();  // [PERSON, O, O, O, O, O, O, O]
//        String firstPOSTag = sent.posTag(0);   // NNP
        Document doc = new Document("add your text here! It can contain multiple sentences.");
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences
            // We're only asking for words -- no need to load any models yet
            System.out.println("The second word of the sentence '" + sent + "' is " + sent.word(1));
            // When we ask for the lemma, it will load and run the part of speech tagger
            System.out.println("The third lemma of the sentence '" + sent + "' is " + sent.lemma(2));
            // When we ask for the parse, it will load and run the parser
            System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            // ...
        }
    }

    public void runWithProps() throws IOException {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text from the file..
        String text = "Last night, Adly Mansour, the interim leader of Egypt, announced plans to \n" +
                "reform Egypt's constitution and hold a new round of parliamentary and \n" +
                "presidential elections. The interim president also announced a judicial \n" +
                "investigation into yesterday's shooting of at least 51 supporters of deposed \n" +
                "president Mohamed Morsi. \n" +
                "\n" +
                "Mansour plans to form a panel within fifteen days to review and suggest changes\n" +
                "to the now-suspended constitution. Those amendments would be voted on in a \n" +
                "referendum within four months. Parliamentary elections would then be held, \n" +
                "perhaps in early 2014, followed by presidential elections upon the forming \n" +
                "of a new parliament.";

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // run all Annotators on this text
        pipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the POS tag of the token
                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

                System.out.println("word: " + word + " pos: " + pos + " ne:" + ne);
            }

            // this is the parse tree of the current sentence
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            System.out.println("parse tree:\n" + tree);

            // this is the Stanford dependency graph of the current sentence
//            SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
//            System.out.println("dependency graph:\n" + dependencies);
        }

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
        Map<Integer, CorefChain> graph =
                document.get(CorefCoreAnnotations.CorefChainAnnotation.class);

    }

    public void runWithClassifier() throws Exception {
        BasicConfigurator.configure();

        String serializedClassifier = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";
        AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);

        /* For the hard-coded String, it shows how to run it on a single
         sentence, and how to do this and produce several formats, including
         slash tags and an inline XML output format. It also shows the full
         contents of the {@code CoreLabel}s that are constructed by the
         classifier. And it shows getting out the probabilities of different
         assignments and an n-best list of classifications with probabilities.
        */

        String[] example = {"Good afternoon Rajat Raina, how are you today?",
                "I go to school at Stanford University, which is located in California."};
        for (String str : example) {
            System.out.println(classifier.classifyToString(str));
        }
        System.out.println("---");

        for (String str : example) {
            // This one puts in spaces and newlines between tokens, so just print not println.
            System.out.print(classifier.classifyToString(str, "slashTags", false));
        }
        System.out.println("---");

        for (String str : example) {
            // This one is best for dealing with the output as a TSV (tab-separated column) file.
            // The first column gives entities, the second their classes, and the third the remaining text in a document
            System.out.print(classifier.classifyToString(str, "tabbedEntities", false));
        }
        System.out.println("---");

        for (String str : example) {
            System.out.println(classifier.classifyWithInlineXML(str));
        }
        System.out.println("---");

        for (String str : example) {
            System.out.println(classifier.classifyToString(str, "xml", true));
        }
        System.out.println("---");

        for (String str : example) {
            System.out.print(classifier.classifyToString(str, "tsv", false));
        }
        System.out.println("---");

        // This gets out entities with character offsets
        int j = 0;
        for (String str : example) {
            j++;
            List<Triple<String, Integer, Integer>> triples = classifier.classifyToCharacterOffsets(str);
            for (Triple<String, Integer, Integer> trip : triples) {
                System.out.printf("%s over character offsets [%d, %d) in sentence %d.%n",
                        trip.first(), trip.second(), trip.third, j);
            }
        }
        System.out.println("---");

        // This prints out all the details of what is stored for each token
        int i = 0;
        for (String str : example) {
            for (List<CoreLabel> lcl : classifier.classify(str)) {
                for (CoreLabel cl : lcl) {
                    System.out.print(i++ + ": ");
                    System.out.println(cl.toShorterString());
                }
            }
        }

        System.out.println("---");

//      Sentence sent = new Sentence("Lucy is in the sky with diamonds.");
//      List<String> nerTags = sent.nerTags();  // [PERSON, O, O, O, O, O, O, O]
//      String firstPOSTag = sent.posTag(0);   // NNP
//
//      System.out.println(nerTags);
//      System.out.println(firstPOSTag);
    }

}
