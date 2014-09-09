package org.wso2.siddhi.extension.nlp;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;

/**
 * Created by malithi on 9/3/14.
 */
public class RelationshipByVerbTransformProcessor extends TransformProcessor {
    @Override
    protected InStream processEvent(InEvent inEvent) {
        return null;
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        return null;
    }

    @Override
    protected Object[] currentState() {
        return new Object[0];
    }

    @Override
    protected void restoreState(Object[] objects) {

    }

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition streamDefinition, StreamDefinition streamDefinition2, String s, SiddhiContext siddhiContext) {

    }

    @Override
    public void destroy() {

    }
}
