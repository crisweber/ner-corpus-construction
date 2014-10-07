package br.pucrs.ppgcc.cclr.wikimedia;

import java.io.Serializable;

import info.bliki.wiki.model.WikiModel;

public class ParseArticleBody implements Serializable {

	private static final long serialVersionUID = 1L;
	private final PlainTextConverter bodyConverter;

	public ParseArticleBody(String language) {
		TitleTransformer titleTransformer = new TitleTransformer(language);
		bodyConverter = new PlainTextConverter(titleTransformer);
	}

	public String asPlainText(String wikiMarkupSource) {
		WikiModel model = new WikiModel("", "");
		String result = model.render(bodyConverter, wikiMarkupSource);
		if(null == result) {
			return "";
		} else {
			return result.trim();
		}
	}
}
