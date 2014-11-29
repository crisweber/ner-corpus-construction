package br.pucrs.acad.ppgcc.cclrner;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;

import java.io.Serializable;
import java.io.StringReader;

/**
 * Created by cristoferweber on 25/04/14.
 */
public class SimplifyLink implements Serializable {

    private static final String BEGIN_LINK_TAG = "<[";
    private static final String END_LINK_TAG = "]>";

    public String map(String text) {

        final StringBuilder newText = new StringBuilder(text.length());
        int beginPos;
        int lastIndex = 0;

        do {

            /* Search for an opening tag, and stops if found nothing. */
            beginPos = text.indexOf(BEGIN_LINK_TAG, lastIndex);

            if(beginPos == -1) {
                newText.append(text.substring(lastIndex));
                break;
            }

            /* If there's an opening tag, replicate text until that position. */
            newText.append(text.substring(lastIndex, beginPos));

            int afterOpeningTag = beginPos + BEGIN_LINK_TAG.length();

            /* Search for a closing tag after the last opening tag. */
            lastIndex = text.indexOf(END_LINK_TAG, afterOpeningTag);

            /* For the improbable case when no closing tag was found, replicate text from last opening tag 'til the end */
            if(lastIndex == -1) {
                newText.append(text.substring(afterOpeningTag));
                break;
            }

            final String linkedInstance = text.substring(afterOpeningTag, lastIndex);
            final String replacement = asSimplifiedLink(linkedInstance);

            newText.append(replacement);

            lastIndex += END_LINK_TAG.length();

        } while (true);

        return newText.toString();

    }

    private String asSimplifiedLink(String linkedInstance) {
        int pipePosition = linkedInstance.indexOf("|");

        if(pipePosition < 0) {
            return linkedInstance;
        }

        String form = linkedInstance.substring(0, pipePosition);
        String nerClass = linkedInstance.substring(pipePosition + 1);

        boolean isFirst = true;

        StringBuilder simplifiedLink = new StringBuilder();

        PTBTokenizer<CoreLabel> ptbt;
        ptbt = new PTBTokenizer<>(new StringReader(form),
                                  new CoreLabelTokenFactory(),
                                  "normalizeOtherBrackets=false");

        simplifiedLink.append('{');

        while(ptbt.hasNext()) {
            CoreLabel word = ptbt.next();
            if(!isFirst) {
                simplifiedLink.append(' ');
            } else {
                isFirst = false;
            }

            simplifiedLink.append(word.word() + "_" + nerClass.toUpperCase());
        }

        simplifiedLink.append('}');
        return simplifiedLink.toString();
    }

}
