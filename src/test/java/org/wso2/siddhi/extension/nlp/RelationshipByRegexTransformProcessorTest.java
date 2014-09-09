package org.wso2.siddhi.extension.nlp;

import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

public class RelationshipByRegexTransformProcessorTest extends TestCase {
    private static Logger logger = Logger.getLogger(RelationshipByRegexTransformProcessorTest.class);

    @Test
    public void testFindRelationshipByRegex() throws InterruptedException {
        logger.info("FindRelationshipByRegex Test 1");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> list = new ArrayList<Class>();
        list.add(RelationshipByRegexTransformProcessor.class);

        siddhiConfiguration.setSiddhiExtensions(list);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        InputHandler inputHandler = siddhiManager.defineStream("define stream DataStream ( text string )");

        String queryReference = siddhiManager.addQuery("from DataStream#transform.nlp:findRelationshipByRegex" +
                "( '{}=verb >/nsubj|agent/ {}=subject ?>/dobj/ {}=object', text ) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        inputHandler.send(new Object[]{"Midfield High School students and fans alike were surprised when Coach Bill Addison submitted his resignation on Thursday."});
        inputHandler.send(new Object[]{"While this announcement was made following the end of a 4-6 season record—the Tigers’ first losing season in over 20 years—the school board maintains that Addison was not asked to resign."});
        inputHandler.send(new Object[]{"“Yeah, he may not have been forced to leave, but you still can’t help but wonder if maybe he saw it coming and wanted to avoid the inevitable,” speculated one fan who wished to remain anonymous."});
        inputHandler.send(new Object[]{"Addison accepted the head coaching position at Midfield in 2007, when the Tigers were in the middle of a downward slide from their glory days in the early 2000s, specifically their road to the state title in 2002. After a slight turnaround in 2009 and 2010, when they lost only three and two games respectively and still made the playoffs, the team fell to only 6-4 in 2011."});
        inputHandler.send(new Object[]{"Rumors began to circulate that a coaching change might be in the works. However, the team got off to a great start in 2012 with three consecutive wins to open the season. This was followed by four losses in a row, including a devastating 45-3 loss to their archrivals, the Stoney Brook Warriors. The team managed to win one more game, but then lost the last two against unranked teams. Still, Thursday’s announcement came as a shock to many."});
        inputHandler.send(new Object[]{"“We had no idea,” said quarterback Brody Jennings, a junior. “I mean, sure, it wasn’t our best year, but you can’t win ‘em all. Coach Addison was a great coach, and the team really respected him. We’re all very sorry to see him go.”"});
        inputHandler.send(new Object[]{"Coach Addison didn’t say much when contacted about his reason for leaving. He said he made the decision that was best for his family and mentioned that they might be relocating from the area entirely. Sources close to the family say that Addison’s father-in-law, who lives in Mississippi, is in poor health and that the need to care for him may have partially prompted the couple’s decision."});
        inputHandler.send(new Object[]{"Whatever the reason, the school board now faces the task of filling the position with a candidate who can revive the program and restore it to its former glory. Many wonder if assistant coaches Tom Caffey or Ryan Young will vie for the position. Opposing coach Taylor Miller, of the Spring Valley Lions, has also been rumored to have expressed an interest in coming to Midfield."});
        inputHandler.send(new Object[]{"“We don’t really care who it is,” said receiver Justin Thomas, who will be a senior next season when the new coach takes over. “We’re a close-knit team with some really good leaders coming up through the ranks. I have no doubt we’ll be able to pull together and do a great job for whomever the school board chooses.”"});

        Thread.sleep(1000);
        siddhiManager.shutdown();

    }

}