package br.pucrs.ppgcc.cclr.wikimedia;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TestParser {

	private ParseArticleBody parser;
	private String paragraph;

	@Before
	public void setup() {
		parser = new ParseArticleBody("pt");
		paragraph = "[[Marilyn Monroe|'''Marilyn Monroe''']] nasceu '''Norma Jeane Mortenson''' em 1 de Junho de 1926, na ala de caridade do Hospital do Condado de Los Angeles.<ref name=\"history\">{{Citar web|língua= inglês |url=http://marilynmonroe.com/history/|título=A Brief Marilyn Monroe History|publicado=marilynmonroe.com|acessodata=22 de dezembro de 2013}}</ref><ref name=\"MSN\">{{Citar web|língua= inglês |url=http://www.webcitation.org/query?id=1257013828901049|título=http://www.webcitation.org/query?id=1257013828901049|publicado=webcitation.org|acessodata=22 de dezembro de 2013}}</ref> Segundo o biógrafo Fred Lawrence Guiles, sua avó, Della Monroe Grainger, levou-a para ser [[batizada]] por [[Aimee Semple McPherson]] como '''Norma Jeane Baker'''. Norma obteve uma ordem do Tribunal de Justiça do Estado de [[Nova York]] e legalmente mudou seu nome para Marilyn Monroe em 23 de fevereiro de 1956.<ref name=\"history\"/>\n";
        //paragraph = "Formou-se pela [[Universidade de Virginia]], tendo estudado também na [[Universidade de Michigan]]. '''Heber Doust Curtis''' foi um [[astronomia|astrônomo]] [[Estados Unidos|americano]]. Trabalhou no [[Observatório Lick]] entre 1902 e 1920, continuando as pesquisas sobre [[nebulosa]]s iniciada por [[James Edward Keeler]]. Foi eleito presidente da [[Sociedade Astronômica do Pacífico]] em 1912 e apontado diretor do [[Observatório Allegheny]]. Participou com [[Harlow Shapley]] no que ficou conhecido como ''O [[Grande Debate (astronomia)|Grande Debate]]'', um debate sobre a natureza das nebulosas e [[galáxia]]s e o tamanho do [[universo]].";
	}
	
	@Test
	public void test() {
		assertEquals("Cristofer", parser.asPlainText("Cristofer"));
	}

	@Test
	public void parserParagraph() {
		String result = (parser.asPlainText(paragraph));
		assertTrue(result.contains("{["));
	}
}
