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

package io.confluent.sigmarules.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.models.AggregateValues;
import io.confluent.sigmarules.models.DetectionResults;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.parsers.AggregateParser;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AggregateTopology {
    final static Logger logger = LogManager.getLogger(AggregateTopology.class);

    private SigmaRuleCheck ruleCheck = new SigmaRuleCheck();

    public void createAggregateTopology(KStream<String, JsonNode> sigmaStream, SigmaRule rule,
        String outputTopic, Configuration jsonPathConf) {

        final Serde<AggregateResults> aggregateSerde = AggregateResults.getJsonSerde();
        Long windowTimeMS = rule.getDetectionsManager().getWindowTimeMS();
        logger.info("window time: " + windowTimeMS);

        AggregateValues aggregateValues = rule.getConditionsManager().getAggregateCondition().getAggregateValues();

        sigmaStream.filter((k, sourceData) -> ruleCheck.isValid(rule, sourceData, jsonPathConf))
            .selectKey((k, v) -> updateKey(aggregateValues))
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(windowTimeMS),
                Duration.ofMillis(windowTimeMS)))
            .aggregate(
                () -> new AggregateResults(),
                (key, source, totals) -> updateValues(aggregateValues, source, totals),
                Materialized.with(Serdes.String(), aggregateSerde)
            )
            .toStream()
            .filter((k, results) -> doStreamFiltering(aggregateValues, results))
            .map((Windowed<String> key, AggregateResults value) ->
                new KeyValue<>("", buildResults(rule, value.getSourceData())))
            .to(outputTopic, Produced.with(Serdes.String(), DetectionResults.getJsonSerde()));
    }

    private String updateKey(AggregateValues aggregateValues) {
        if (aggregateValues.getGroupBy() == null || aggregateValues.getGroupBy().isEmpty()) {
            return "Counter";
        } else {
            return aggregateValues.getGroupBy();
        }
    }

    private AggregateResults updateValues(AggregateValues aggregateValues, JsonNode source,
        AggregateResults balance) {

        AggregateResults results = new AggregateResults();
        results.setResults(balance.getResults());
        results.setSourceData(source);

        Long newValue = 1L;
        String distinctValue = aggregateValues.getDistinctValue();
        if (distinctValue == null || distinctValue.isEmpty()) {
            distinctValue = "CurrentCount";
        }

        if (results.getResults().containsKey(distinctValue)) {
            newValue = results.getResults().get(distinctValue) + 1;
        }
        results.getResults().put(distinctValue, newValue);
        logger.info(" update " + distinctValue + " to " + newValue);

        return results;
    }

    private Boolean doStreamFiltering(AggregateValues aggregateValues, AggregateResults tableValues) {
        Long operationValue = Long.parseLong(aggregateValues.getOperationValue());
        Boolean matchFound = false;

        for (Map.Entry<String, Long> results : tableValues.getResults().entrySet()) {
            switch (aggregateValues.getOperation()) {
                case AggregateParser.EQUALS:
                    if (results.getValue() == operationValue) {
                        matchFound = true;
                    }
                    break;
                case AggregateParser.GREATER_THAN:
                    if (results.getValue() > operationValue) {
                        matchFound = true;
                    }
                    break;
                case AggregateParser.GREATER_THAN_EQUAL:
                    if (results.getValue() >= operationValue) {
                        matchFound = true;
                    }
                    break;
                case AggregateParser.LESS_THAN:
                    if (results.getValue() < operationValue) {
                        matchFound = true;
                    }
                    break;
                case AggregateParser.LESS_THAN_EQUAL:
                    if (results.getValue() <= operationValue) {
                        matchFound = true;
                    }
                    break;
                default:
                    System.out.println("Unhandled operation");
                    break;
            }

            if (matchFound == true) {
                logger.info("********************");
                logger.info("Found a match Key: " + results.getKey() + " Value: " + results.getValue() +
                        " Comp Value " + operationValue);
                return true;
            }
        }

        return false;
    }

    private DetectionResults buildResults(SigmaRule rule, JsonNode sourceData) {
        DetectionResults results = new DetectionResults();
        results.setSourceData(sourceData);

        // check rule factory conditions manager for aggregate condition
        // and set it metadata
        if (rule != null) {
            results.getSigmaMetaData().setId(rule.getId());
            results.getSigmaMetaData().setTitle(rule.getTitle());
        }

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        results.setTimeStamp(timestamp.getTime());

        return results;
    }

}
