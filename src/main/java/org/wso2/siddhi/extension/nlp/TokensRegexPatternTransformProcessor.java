package org.wso2.siddhi.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.ListEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.*;

/**
 * Created by malithi on 9/3/14.
 */

@SiddhiExtension(namespace = "nlp", function = "findTokensRegexPattern")
public class TokensRegexPatternTransformProcessor extends TransformProcessor{

    private static Logger logger = Logger.getLogger(TokensRegexPatternTransformProcessor.class);
    private Map<String, Integer> paramPositions = new HashMap<String, Integer>(1);
    private String regex;
    private StanfordCoreNLP pipeline;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {

        logger.debug("Query Initialized");

        try {
            regex = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter regex",e);
            throw new QueryCreationException("Parameter regex should be of type string");
        }

        for (int i=1; i < expressions.length; i++) {
            if (expressions[i] instanceof Variable) {
                Variable var = (Variable) expressions[i];
                String attributeName = var.getAttributeName();
                paramPositions.put(attributeName, inStreamDefinition.getAttributePosition(attributeName));
            }
        }

        logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                paramPositions.keySet()));


        initPipeline();

        // Create outstream
        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("tokensRegexPatternMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            // Create outstream attributes for all the attributes in the input stream
            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        logger.debug(String.format("Event received. Event Timestamp:%d Regex:%s",inEvent.getTimeStamp(), regex));

        Object [] inStreamData = inEvent.getData();

        TokenSequencePattern pattern = TokenSequencePattern.compile(regex);
        TokenSequenceMatcher matcher = null;

        Iterator<Map.Entry<String, Integer>> iterator = paramPositions.entrySet().iterator();
        Annotation document = pipeline.process((String)inEvent.getData(paramPositions.get(iterator.next().getKey())));

        InListEvent transformedListEvent = new InListEvent();

        for (CoreMap sentence:document.get(CoreAnnotations.SentencesAnnotation.class)){
            matcher = pattern.getMatcher(sentence.get(CoreAnnotations.TokensAnnotation.class));

            while( matcher.find()){
                Object [] outStreamData = new Object[inStreamData.length + 1];
                outStreamData[0] = matcher.group();
                System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                        outStreamData));
            }
        }

        return transformedListEvent;
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InListEvent transformedListEvent = new InListEvent();
        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                ListEvent resultListEvent = (ListEvent) processEvent((InEvent)event);
                transformedListEvent.setEvents(resultListEvent.getEvents());
            }
        }
        return transformedListEvent;
    }

    @Override
    protected Object[] currentState() {
        return new Object[0];
    }

    @Override
    protected void restoreState(Object[] objects) {

    }

    @Override
    public void destroy() {

    }

    private void initPipeline(){
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
    }
}
