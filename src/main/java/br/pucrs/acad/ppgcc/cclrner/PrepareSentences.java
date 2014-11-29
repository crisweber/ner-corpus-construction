package br.pucrs.acad.ppgcc.cclrner;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

import java.io.Serializable;
import java.io.StringReader;
import java.util.List;

/**
 * Created by cristoferweber on 28/04/14.
 */
public class PrepareSentences implements Serializable {

    public String map(String text) {

        String result = "";
        DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(text));
        for (List<HasWord> sentence : dp) {

            if (!sentence.isEmpty()) {

                boolean firstWord = true;
                boolean isLinkOpening = false;
                boolean hasLink = false;

                StringBuilder resultingSentence = new StringBuilder(text.length());
                for (HasWord word : sentence) {
                    String wordForm = word.word();

                    if (!firstWord) {
                        resultingSentence.append(' ');
                    } else {
                        firstWord = false;
                    }

                    if(isLinkOpening) {
                        if(wordForm.contains("_")) {
                            hasLink = true;
                        } else {
                            isLinkOpening = false;
                        }
                    }

                    switch (wordForm) {
                        case "-LCB-":
                            resultingSentence.append("{");
                            isLinkOpening = true;
                            break;
                        case "-RCB-":
                            resultingSentence.append("}");
                            break;
                        case "-LRB-":
                            resultingSentence.append("(");
                            break;
                        case "-RRB-":
                            resultingSentence.append(")");
                            break;
                        case "-LSB-":
                            resultingSentence.append("[");
                            break;
                        case "-RSB-":
                            resultingSentence.append("]");
                            break;
                        default:
                            resultingSentence.append(wordForm);
                            break;
                    }
                }

                if(hasLink)
                    result = resultingSentence.toString();
            }
        }

        return result;
    }
}