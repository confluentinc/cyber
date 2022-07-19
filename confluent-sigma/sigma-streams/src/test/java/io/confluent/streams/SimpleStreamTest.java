/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */

package io.confluent.streams;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.sigmarules.exceptions.InvalidSigmaRuleException;
import io.confluent.sigmarules.exceptions.SigmaRuleParserException;
import io.confluent.sigmarules.models.DetectionResults;
import io.confluent.sigmarules.rules.SigmaRulesFactory;
import io.confluent.sigmarules.streams.SigmaStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SimpleStreamTest {
    final static Logger logger = LogManager.getLogger(SimpleStreamTest.class);

    private TopologyTestDriver td;
    private Topology topology;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private ObjectMapper objectMapper = new ObjectMapper();


    Properties getProperties() {
        Properties testProperties = new Properties();
        //TODO should be bootstrap.serverS -- use defined variable in place of string
        //testProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        testProperties.setProperty("bootstrap.server", "foo:1234");
        testProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-simple");
        testProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testProperties.setProperty("sigma.rules.topic", "rules");
        testProperties.setProperty("schema.registry", "localhost:8888");
        testProperties.setProperty("output.topic", "test-output");
        testProperties.setProperty("data.topic", "test-input");

        return testProperties;
    }

    @BeforeAll
    void setUp() {
    }

    @After
    public void tearDown() {
        td.close();
    }

    @Test
    public void checkSingleRule()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "   - foo: 'ab*'\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"abc\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"bc\"}");
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void checkContainsAllRule()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "   foo|contains|all:\n"
            + "     - abc\n"
            + "     - 123\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"abcdef123456\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"fooabcdef123456foo\"}");
        results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"ab123\"}");
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void checkListRule()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "   foo:\n"
            + "     - abcd\n"
            + "     - 1234\n"
            + "     - abc123\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"abcd\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"abc123\"}");
        results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"1234\"}");
        results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"empty\"}");
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void checkMapRule()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "     - EventLog: Security\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"EventLog\" : \"Security\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"foo\" : \"abc123\"}");
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void checkMapAndListRule()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "     - EventLog: Security\n"
            + "       EventID:\n"
            + "         - 517\n"
            + "         - 1102\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"EventLog\" : \"Security\", \"EventID\" : \"517\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void checkMultipleRules()
        throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        String testRule = "title: Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "   - foo: 'ab*'\n"
            + "  condition: test";

        String testRule2 = "title: Another Simple Http\n"
            + "logsource:\n"
            + "  product: zeek\n"
            + "  service: http\n"
            + "detection:\n"
            + "  test:\n"
            + "   - foo: 'ab*'\n"
            + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);
        srf.addRule("Another Simple Http", testRule2);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
            Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
            Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        // Should be 2 matches (titles)
        inputTopic.pipeInput("{\"foo\" : \"abc\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        logger.info("title: " + results.getSigmaMetaData().getTitle());
        assertTrue(results.getSigmaMetaData().getTitle().equals("Another Simple Http") ||
            results.getSigmaMetaData().getTitle().equals("Simple Http"));

        results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        logger.info("title: " + results.getSigmaMetaData().getTitle());
        assertTrue(results.getSigmaMetaData().getTitle().equals("Another Simple Http") ||
            results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());

        // Should be empty
        inputTopic.pipeInput("{\"foo\" : \"bc\"}");
        assertTrue(outputTopic.isEmpty());

    }

    @Test
    public void checkNestedMapUsingListRule()
            throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        // Not able to test field mappings, so adding the rule in the format that field mappings normalize to
        String testRule = "title: Simple Http\n"
                + "logsource:\n"
                + "  product: zeek\n"
                + "  service: http\n"
                + "detection:\n"
                + "  test:\n"
                + "     - $.Event.System.EventLog: Security\n"
                + "       $.Event.System.EventID:\n"
                + "         - 517\n"
                + "         - 1102\n"
                + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
                Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
                Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"Event\": {\"System\": {\"EventLog\" : \"Security\", \"EventID\" : \"517\"}}}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void checkObjectWithPeriodDelimiterUsingListRule()
            throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        // Not able to test field mappings, so adding the rule in the format that field mappings normalize to
        String testRule = "title: Simple Http\n"
                + "logsource:\n"
                + "  product: zeek\n"
                + "  service: http\n"
                + "detection:\n"
                + "  test:\n"
                + "     - Event.System.EventLog: Security\n"
                + "       Event.System.EventID:\n"
                + "         - 517\n"
                + "         - 1102\n"
                + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();;
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
                Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
                Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"Event.System.EventLog\" : \"Security\", \"Event.System.EventID\" : \"517\"}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void checkObjectWithoutDetectionField()
            throws IOException, InvalidSigmaRuleException, SigmaRuleParserException {

        // Not able to test field mappings, so adding the rule in the format that field mappings normalize to
        String testRule = "title: Simple Http\n"
                + "logsource:\n"
                + "  product: zeek\n"
                + "  service: http\n"
                + "detection:\n"
                + "  test:\n"
                + "     - $.Event.System.EventLog: Security\n"
                + "       $.Event.System.EventID:\n"
                + "         - 517\n"
                + "         - 1102\n"
                + "  condition: test";

        SigmaRulesFactory srf = new SigmaRulesFactory();
        srf.setFiltersFromProperties(getProperties());
        srf.addRule("Simple Http", testRule);

        SigmaStream stream = new SigmaStream(getProperties(), srf);
        topology = stream.createTopology();
        td = new TopologyTestDriver(topology, getProperties());

        inputTopic = td.createInputTopic("test-input", Serdes.String().serializer(),
                Serdes.String().serializer());
        outputTopic = td.createOutputTopic("test-output", Serdes.String().deserializer(),
                Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("{\"Event\": {\"System\": {\"EventLog\" : \"Security\"}}}");
        DetectionResults results = objectMapper.readValue(outputTopic.readValue(), DetectionResults.class);
        assertTrue(results.getSigmaMetaData().getTitle().equals("Simple Http"));
        assertTrue(outputTopic.isEmpty());
    }

}
