/*
 * Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.siddhi.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class NameEntityTypeTransformProcessorTest extends NlpTransformProcessorTest {
    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryTransformProcessorTest.class);

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream NameEntityTypeIn (id string, text string )");
    }

    @Test(expected = org.wso2.siddhi.core.exception.QueryCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.addQuery("from NameEntityTypeIn#transform.nlp:findNameEntityType" +
                "        ( 'PERSON', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionEntityTypeTypeMismatch(){
        logger.info("Test: QueryCreationException at EntityType type mismatch");
        siddhiManager.addQuery("from NameEntityTypeIn#transform.nlp:findNameEntityType" +
                "        ( 124 , false, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionGroupSuccessiveEntitiesTypeMismatch(){
        logger.info("Test: QueryCreationException at GroupSuccessiveEntities type mismatch");
        siddhiManager.addQuery("from NameEntityTypeIn#transform.nlp:findNameEntityType" +
                "        ( 'PERSON' , 'false', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionUndefinedEntityType(){
        logger.info("Test: QueryCreationException at undefined EntityType");
        siddhiManager.addQuery("from NameEntityTypeIn#transform.nlp:findNameEntityType" +
                "        ( 'DEGREE' , false, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test
    public void testFindNameEntityTypePerson() throws Exception{
        testFindNameEntityType("person", false);
        testFindNameEntityType("person", true);
    }

    @Test
    public void testFindNameEntityTypeOrganization() throws Exception{
        testFindNameEntityType("ORGANIZATION", false);
        testFindNameEntityType("ORGANIZATION", true);
    }

    @Test
    public void testFindNameEntityTypeLocation() throws Exception{
        testFindNameEntityType("LOCATION", false);
        testFindNameEntityType("LOCATION", true);
    }

    private void testFindNameEntityType(String entityType, boolean groupSuccessiveEntities) throws Exception{
        logger.info(String.format("Test: EntityType = %s GroupSuccessiveEntities = %b", entityType,
                groupSuccessiveEntities));
        String query = "from NameEntityTypeIn#transform.nlp:findNameEntityType" +
                "        ( '%s' , %b, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n";
        start = System.currentTimeMillis();
        String queryReference = siddhiManager.addQuery(String.format(query, entityType, groupSuccessiveEntities));
        end = System.currentTimeMillis();

        logger.info(String.format("Time to add query: [%f sec]", ((end - start)/1000f)));

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println
                        ("========================================================================================================================================================================================");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event:inEvents){
                    Event[] subEventArray = event.toArray();
                    if (subEventArray != null){
                        for (Event subEvent:subEventArray){
                            System.out.println
                                    ("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            System.out.println("timestamp="+ subEvent.getTimeStamp());
                            System.out.print("data=[");
                            for (Object obj: subEvent.getData()){
                                System.out.print(obj + ",");
                            }
                            System.out.println("]");
                        }
                    }
                }
                System.out.println
                        ("========================================================================================================================================================================================");
            }

        });

        generateEvents();
    }

    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("NameEntityTypeIn");
        for (String[] dataLine : data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }
}