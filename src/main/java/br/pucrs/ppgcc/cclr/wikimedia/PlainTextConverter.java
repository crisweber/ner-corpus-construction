package br.pucrs.ppgcc.cclr.wikimedia;

import info.bliki.htmlcleaner.BaseToken;
import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.htmlcleaner.TagToken;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.filter.WPList;
import info.bliki.wiki.filter.WPTable;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.tags.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PlainTextConverter implements ITextConverter, Serializable {

	private static final long serialVersionUID = -129357906480799966L;
	private static final String BEGIN_LINK_MARKER = "{[";
    private static final String END_LINK_MARKER = "]}";

    private final TitleTransformer iriConversor;

    public PlainTextConverter(TitleTransformer iriConversor) {
        this.iriConversor = iriConversor;
    }

    @Override
    public void nodesToText(List<? extends Object> nodes,
                            Appendable resultBuffer, IWikiModel model) throws IOException {
        if (nodes != null && !nodes.isEmpty()) {
            try {
                int level = model.incrementRecursionLevel();

                if (level > Configuration.RENDERER_RECURSION_LIMIT) {
                    resultBuffer
                            .append("Error - recursion limit exceeded rendering tags in PlainTextConverter#nodesToText().");
                    return;
                }
                Iterator<? extends Object> childrenIt = nodes.iterator();
                while (childrenIt.hasNext()) {
                    Object item = childrenIt.next();
                    if (item != null) {
                        if (item instanceof List) {
                            nodesToText((List) item, resultBuffer, model);
                        } else if (item instanceof WPATag) {
                            appendLink(resultBuffer, (WPATag) item);
                        } else if (item instanceof ContentToken) {
                            appendContent(resultBuffer, (ContentToken) item);
                        } else if (item instanceof WPTag) {
                            WPTag tag = (WPTag)item;
                            if(!tag.getName().startsWith("h")) {
                                tag.renderHTMLWithoutTag(this, resultBuffer, model);
                                //Skip all Headers
                                //resultBuffer.append('\n');
                            }
                        } else if (item instanceof WPList) {
                            ((WPList) item).renderPlainText(this, resultBuffer, model);
                        } else if (item instanceof WPTable) {
                            ((WPTable) item).renderPlainText(this, resultBuffer, model);
                        } else if (item instanceof SourceTag) {
                            resultBuffer.append("Source code.");
                        } else if (item instanceof RefTag) {
                            // Skip all references
                        } else if (item instanceof MathTag) {
                            resultBuffer.append("math expression");
                        } else if (item instanceof HTMLTag) {
                            HTMLTag tag = ((HTMLTag) item);
                            nodesToText(tag.getChildren(), resultBuffer, model);
                        } else if (item instanceof TagNode) {
                            TagNode node = (TagNode) item;
                            Map<String, Object> map = node.getObjectAttributes();
                            if (map != null && map.size() > 0) {
                            } else {
                                node.getBodyString(resultBuffer);
                            }
                        }
                    }
                }
            } finally {
                model.decrementRecursionLevel();
            }
        }
    }

    private void appendContent(Appendable resultBuffer, ContentToken item) throws IOException {
        String content = item.getContent();
        String text = removeTemplates(content);

        if(!(text.equals("\n") || text.equals("") )) {
            resultBuffer.append(text.trim());
            resultBuffer.append(' ');
        }
    }

    static String removeTemplates(String content) {
        final String beginTemplate = "{{";
        final String endTemplate = "}}";
        final StringBuilder newContent = new StringBuilder(content.length());
        int lastIndex = 0;
        int beginPos;
        do {
            beginPos = content.indexOf(beginTemplate, lastIndex);
            if(beginPos == -1) {
                newContent.append(content.substring(lastIndex));
                break;
            }

            newContent.append(content.substring(lastIndex, beginPos));

            lastIndex = content.indexOf(endTemplate, beginPos + 2);

            if(lastIndex == -1) {
                newContent.append(content.substring(beginPos + 2));
                break;
            } else {
                lastIndex += 2;
            }
        } while(true);

        return newContent.toString();
    }

    private void appendLink(Appendable resultBuffer, WPATag link) throws IOException {
        final Map<String, String> linkAttributes = link.getAttributes();
        boolean missingTitle = !linkAttributes.containsKey("title");

        final List linkChildren = link.getChildren();
        boolean missingChildren = linkChildren.isEmpty();

        if(!(missingChildren && missingTitle)) {
            String title;
            if(missingTitle) {

                if(missingTitle) {
                    title = linkChildren.get(0).toString();
                } else {
                    title = linkAttributes.get("title");
                }

            } else {
                title = linkAttributes.get("title");
            }

            String linkedTitle;
            BaseToken token = link;
            while(token instanceof TagNode) {
                TagNode tag = (TagNode)token;
                if(tag.getChildren().size() > 0)
                    token = (BaseToken)tag.getChildren().get(0);
                else
                    break;
            }
            linkedTitle = token.toString();

            resultBuffer.append(BEGIN_LINK_MARKER);
            resultBuffer.append(linkedTitle).append('|');
            resultBuffer.append(iriConversor.transformToIRI(title));
            resultBuffer.append(END_LINK_MARKER);
            resultBuffer.append(' ');
        }
    }

    @Override
    public boolean noLinks() {
        return false;
    }

    @Override
    public void imageNodeToText(TagNode imageTagNode, ImageFormat imageFormat,
                                Appendable resultBuffer, IWikiModel model) throws IOException {

    }

}
