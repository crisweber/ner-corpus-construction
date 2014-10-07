package br.pucrs.ppgcc.cclr.wikimedia;

import java.io.Serializable;

/**
 * Created by cristoferweber on 23/04/14.
 */
public class TitleTransformer implements Serializable {

	private static final long serialVersionUID = 1L;
	private final String language;

	public TitleTransformer(String language) {
		this.language = language;
	}
	
	public String transformToIRI(String wikipediaTitle) {
        StringBuilder dbpediaURI = new StringBuilder((int)(31 + wikipediaTitle.length() * 1.05));

        if(language.equals("en") || language.equals("")) {
            dbpediaURI.append("http://dbpedia.org/resource/");
        } else {
            dbpediaURI.append("http://").append(language).append(".dbpedia.org/resource/");
        }

        for(char uriChar : wikipediaTitle.toCharArray()) {
            switch(uriChar) {
                case ' ':
                    dbpediaURI.append('_');
                    break;
                case '"':
                case '#':
                case '%':
                case '<':
                case '>':
                case '?':
                case '[':
                case '\\':
                case ']':
                case '^':
                case '`':
                case '{':
                case '|':
                case '}':
                    dbpediaURI.append('%').append(toHex(uriChar / 16)).append(toHex(uriChar % 16));
                    break;
                default:
                    dbpediaURI.append(uriChar);

            }
        }
        return dbpediaURI.toString();
    }

    private char toHex(int charpart) {
        return (char) (charpart < 10 ? '0' + charpart : 'A' + charpart - 10);
    }

}
