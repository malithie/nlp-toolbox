package org.wso2.siddhi.extension.nlp;

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
import org.wso2.siddhi.extension.nlp.dictionary.Dictionary;
import org.wso2.siddhi.extension.nlp.utility.Constants;
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
@SiddhiExtension(namespace = "nlp", function = "findNameEntityTypeViaDictionary")
public class NameEntityTypeViaDictionaryTransformProcessor extends TransformProcessor {
    private static Logger logger = Logger.getLogger(NameEntityTypeTransformProcessor.class);
    private Map<String, Integer> paramPositions = new HashMap<String, Integer>(1);
    private Constants.EntityType entityType;
    private Dictionary dictionary;

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
            logger.error("Entity Type ["+ entityTypeParam + "] is not defined", e);
            throw new QueryCreationException("Parameter entityType should be one of " + Constants.EntityType.values());
        }

        String dictionaryFilePath;
        try {
            dictionaryFilePath = ((StringConstant)expressions[1]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter dictionaryFilePath",e);
            throw new QueryCreationException("Parameter dictionaryFilePath should be of type string");
        }

        try {
            dictionary = new Dictionary(entityType, dictionaryFilePath);
        } catch (Exception e) {
            logger.error("Error creating dictionary", e);
            throw new QueryCreationException("Failed to initialize dictionary");
        }

        for (int i=2; i < expressions.length; i++) {
            if (expressions[i] instanceof Variable) {
                Variable var = (Variable) expressions[i];
                String attributeName = var.getAttributeName();
                paramPositions.put(attributeName, inStreamDefinition.getAttributePosition(attributeName));
            }
        }

        logger.debug(String.format("Query parameters initialized. EntityType: %s DictionaryFilePath: %s " +
                        "Stream Parameters: %s", entityTypeParam, dictionaryFilePath,
                paramPositions.keySet()));

        // Create outstream
        if (outStreamDefinition == null) { //WHY DO WE HAVE TO CHECK WHETHER ITS NULL?
            this.outStreamDefinition = new StreamDefinition().name("nameEntityTypeViaDictionaryMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            // Create outstream attributes for all the attributes in the input stream
            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        logger.debug(String.format("Event received. Event Timestamp:%d Entity Type:%s DictionaryFilePath:%s",
                inEvent.getTimeStamp(), entityType.name(), dictionary.getXmlFilePath()));

        Object [] inStreamData = inEvent.getData();

        Iterator<Map.Entry<String, Integer>> iterator = paramPositions.entrySet().iterator();
        String text = (String)inEvent.getData(paramPositions.get(iterator.next().getKey()));

        InListEvent transformedListEvent = new InListEvent();

        Set<String> dictionaryEntries = dictionary.getEntries(entityType);

        for (String entry:dictionaryEntries){
            if(text.contains(entry)){
                Object [] outStreamData = new Object[inStreamData.length + 1];
                outStreamData[0] = entry;
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

}
