package org.wso2.siddhi.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
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
import org.wso2.siddhi.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.BoolConstant;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.*;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findNameEntityType")
public class NameEntityTypeTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(NameEntityTypeTransformProcessor.class);
    private Map<String, Integer> paramPositions = new HashMap<String, Integer>(1);
    private Constants.EntityType entityType;
    private boolean groupSuccessiveEntities;
    private StanfordCoreNLP pipeline;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition streamDefinition, StreamDefinition streamDefinition2, String s, SiddhiContext siddhiContext) {
        logger.debug("Query Initialized");

        String entityTypeParam = null;
        try {
            entityTypeParam = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter entityType",e);
            throw new QueryCreationException("Parameter entityType should be of type string");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.error("Entity Type ["+ entityTypeParam + "] is not defined",e);
            throw new QueryCreationException("Parameter entityType should be one of " + Constants.EntityType.values());
        }

        try {
            groupSuccessiveEntities = ((BoolConstant)expressions[1]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter groupSuccessiveEntities",e);
            throw new QueryCreationException("Parameter groupSuccessiveEntities should be of type boolean");
        }

        for (int i=2; i < expressions.length; i++) {
            if (expressions[i] instanceof Variable) {
                Variable var = (Variable) expressions[i];
                String attributeName = var.getAttributeName();
                paramPositions.put(attributeName, inStreamDefinition.getAttributePosition(attributeName));
            }
        }

        logger.debug(String.format("Query parameters initialized. EntityType: %s GroupSuccessiveEntities %s " +
                        "Stream Parameters: %s", entityTypeParam, groupSuccessiveEntities,
                paramPositions.keySet()));

        initPipeline();

        // Create outstream
        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("nameEntityTypeMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            // Create outstream attributes for all the attributes in the input stream
            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        logger.debug(String.format("Event received. Event Timestamp:%d Entity Type:%s GroupSuccessiveEntities:%s",
                inEvent.getTimeStamp(), entityType.name(), groupSuccessiveEntities));

        Object [] inStreamData = inEvent.getData();

        Iterator<Map.Entry<String, Integer>> iterator = paramPositions.entrySet().iterator();
        Annotation document = new Annotation((String)inEvent.getData(paramPositions.get(iterator.next().getKey())));
        pipeline.annotate(document);

        InListEvent transformedListEvent = new InListEvent();

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        if (groupSuccessiveEntities){
            List<Event> eventList = new ArrayList<Event>();
            for (CoreMap sentence : sentences) {
                int previousCount = 0;
                int count = 0;

                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    String word = token.get(CoreAnnotations.TextAnnotation.class);

                    int previousWordIndex;
                    if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                        count++;
                        if (previousCount != 0 && (previousCount + 1) == count) {
                            previousWordIndex = eventList.size() - 1;
                            String previousWord = (String)eventList.get(previousWordIndex).getData0();
                            eventList.remove(previousWordIndex);

                            previousWord = previousWord.concat(" " + word);
                            Object [] outStreamData = new Object[inStreamData.length + 1];
                            outStreamData[0] = previousWord;
                            System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                            transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                    outStreamData));
                        } else {
                            Object [] outStreamData = new Object[inStreamData.length + 1];
                            outStreamData[0] = word;
                            System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                            transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                    outStreamData));
                        }
                        previousCount = count;
                    } else {
                        count = 0;
                        previousCount = 0;
                    }
                }
            }
            Event[] events = new Event[eventList.size()];
            transformedListEvent.setEvents(eventList.toArray(events));
        }else {
            for (CoreMap sentence : sentences) {
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
                    if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                        Object [] outStreamData = new Object[inStreamData.length + 1];
                        outStreamData[0] = word;
                        System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                        transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                outStreamData));
                    }
                }
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
