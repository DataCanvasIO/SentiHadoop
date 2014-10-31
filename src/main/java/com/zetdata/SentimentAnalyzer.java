package com.zetdata;

import com.zetdata.avro.NlpResult;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.ejml.simple.SimpleMatrix;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * A wrapper class which creates a suitable pipeline for the sentiment
 * model and processes raw text.
 *
 * @author Xiaolin Zhang
 */
public class SentimentAnalyzer {
    private StanfordCoreNLP pipeline = null;

    public void Init() {
        Properties props = new Properties();
        // props.setProperty("ssplit.eolonly", "true");
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");

        this.pipeline = new StanfordCoreNLP(props);
    }

    static ArrayList<Double> getRootVector(CoreMap sentence) {
        Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
        SimpleMatrix vector = RNNCoreAnnotations.getPredictions(tree);
        ArrayList<Double> result = new ArrayList<Double>();

        for (int i = 0; i < vector.getNumElements(); ++i) {
            result.add(vector.get(i));
        }

        return result;
    }

    static String getRootSentiment(CoreMap sentence) {
        return sentence.get(SentimentCoreAnnotations.ClassName.class);
    }

    public NlpResult processSentence(String text) {
        Annotation annotation = pipeline.process(text);
        List<CoreMap> coreMaps = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        if (coreMaps.size() < 1) {
            // Default value as "Neutral"
            return NlpResult.newBuilder()
                    .setText(text)
                    .setSentiment("Neutral")
                    .setSentimentVector(Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0))
                    .build();
        }
        CoreMap sentence = coreMaps.get(0);
        String sentimentResult = getRootSentiment(sentence);
        ArrayList<Double> sentimentVector = getRootVector(sentence);

        return NlpResult.newBuilder()
                .setText(text)
                .setSentiment(sentimentResult)
                .setSentimentVector(sentimentVector)
                .build();
    }

    public List<NlpResult> processParagraph(String text) {
        List<NlpResult> results = new ArrayList<NlpResult>();

        Annotation annotation = pipeline.process(text);
        List<CoreMap> coreMaps = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        if (coreMaps.size() < 1) {
            // Default value as "Neutral"
            NlpResult dummy = NlpResult.newBuilder()
                    .setText(text)
                    .setSentiment("Neutral")
                    .setSentimentVector(Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0))
                    .build();

            results.add(dummy);
            return results;
        }

        for(CoreMap cm: coreMaps){
            String sentimentResult = getRootSentiment(cm);
            ArrayList<Double> sentimentVector = getRootVector(cm);

            results.add(NlpResult.newBuilder()
                    .setText(cm.toString())
                    .setSentiment(sentimentResult)
                    .setSentimentVector(sentimentVector)
                    .build());
        }

        return results;
    }

    public static void main(String[] args) throws Exception {
        SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
        sentimentAnalyzer.Init();

        // Process stdin.  Each line will be treated as a single sentence.
        System.err.println("Reading in text from stdin.");
        System.err.println("Please enter one sentence per line.");
        System.err.println("Processing will end when EOF is reached.");
        BufferedReader reader = new BufferedReader(IOUtils.encodedInputStreamReader(System.in, "utf-8"));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            if (line.length() > 0) {
                // NlpResult nlp_result = mySentiment.sentimentAnalysis(line);
                // System.out.println(nlp_result.toString());
                for(NlpResult nlp_result : sentimentAnalyzer.processParagraph(line)) {
                    System.out.println(nlp_result.toString());
                }
            } else {
                // Output blank lines for blank lines so the tool can be
                // used for line-by-line text processing
                System.out.println("");
            }
        }

    }
}