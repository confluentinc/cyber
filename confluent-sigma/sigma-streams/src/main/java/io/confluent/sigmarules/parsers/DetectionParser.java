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

package io.confluent.sigmarules.parsers;

import io.confluent.sigmarules.exceptions.InvalidSigmaRuleException;
import io.confluent.sigmarules.fieldmapping.FieldMapper;
import io.confluent.sigmarules.models.OperatorType;
import io.confluent.sigmarules.models.SigmaDetection;
import io.confluent.sigmarules.models.SigmaDetections;
import io.confluent.sigmarules.rules.ConditionsManager;
import io.confluent.sigmarules.rules.DetectionsManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DetectionParser {
    final static Logger logger = LogManager.getLogger(DetectionParser.class);

    static final String OPEN_BRACKET = "{";
    static final String CLOSE_BRACKET = "}";
    static final String OPEN_ARRAY = "[";
    static final String CLOSE_ARRAY = "]";
    static final String EQUALS = "=";
    static final String SEPERATOR = "|";
    static final String COMMA_SEP = ",";

    private FieldMapper fieldMapper = null;

    public DetectionParser() {}

    public DetectionParser(FieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
    }

    public DetectionsManager parseDetections(ParsedSigmaRule sigmaRule)
        throws InvalidSigmaRuleException {
        DetectionsManager detectionsManager = new DetectionsManager();

        // loop through list of detections - search identifier are the keys
        // the values can be either lists or maps (key / value pairs)
        // See https://github.com/SigmaHQ/sigma/wiki/Specification#detection
        for (Map.Entry<String, Object> entry : sigmaRule.getDetection().entrySet()) {
            String detectionName = entry.getKey();
            Object searchIdentifiers = entry.getValue();

            if (detectionName.equals("condition") || detectionName.equals("timeframe") ||
                detectionName.equals("fields")) {
                // handle separately
            } else {
                detectionsManager.addDetections(detectionName, parseDetection(searchIdentifiers));
            }
        }

        if (sigmaRule.getDetection().containsKey("timeframe")) {
            detectionsManager.convertWindowTime(sigmaRule.getDetection().get("timeframe").toString());
        }

        return detectionsManager;
    }

    private SigmaDetections parseDetection(Object searchIdentifiers) throws InvalidSigmaRuleException {
        SigmaDetections parsedDetections = new SigmaDetections();

        // check if the search identifier is a list or a map
        if (searchIdentifiers instanceof LinkedHashMap) {
            LinkedHashMap<String, Object> searchIdMap = (LinkedHashMap<String, Object>) searchIdentifiers;
            for (Map.Entry<String, Object> searchId : searchIdMap.entrySet()) {
                if (searchId.getValue() instanceof ArrayList) {
                    SigmaDetection detectionModel = new SigmaDetection();
                    parseName(detectionModel, searchId.getKey());

                    List<String> searchIdValues = (ArrayList<String>) searchId.getValue();
                    for (String v : searchIdValues) {
                        parseValue(detectionModel, v);
                    }

                    parsedDetections.addDetection(detectionModel);
                } else if (searchId.getValue() instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> searchIdInnerMap = (LinkedHashMap<String, Object>) searchId.getValue();
                    for (Map.Entry<String, Object> searchIdInner : searchIdInnerMap.entrySet()) {
                        SigmaDetection detectionModel = new SigmaDetection();
                        parseName(detectionModel, searchIdInner.getKey());

                        if (searchIdInner.getValue() instanceof ArrayList) {
                            List<String> searchIdValues = (ArrayList<String>) searchIdInner.getValue();
                            for (String v : searchIdValues) {
                                parseValue(detectionModel, v);
                            }
                        } else {
                            parseValue(detectionModel, searchIdInner.getValue().toString());
                        }

                        parsedDetections.addDetection(detectionModel);
                    }
                } else { // key is the detection name
                    SigmaDetection detectionModel = new SigmaDetection();
                    parseName(detectionModel, searchId.getKey());
                    parseValue(detectionModel, searchId.getValue().toString());

                    parsedDetections.addDetection(detectionModel);
                }
            }
        } else if (searchIdentifiers instanceof ArrayList) {
            List<String> searchArray = (ArrayList<String>)searchIdentifiers;
            for (Object searchMap : searchArray) {
                LinkedHashMap<String, Object> searchIdMap = (LinkedHashMap<String, Object>)searchMap;
                for (Map.Entry<String, Object> searchId : searchIdMap.entrySet()) {
                    SigmaDetection detectionModel = new SigmaDetection();
                    parseName(detectionModel, searchId.getKey());
                    parseValue(detectionModel, searchId.getValue().toString());

                    parsedDetections.addDetection(detectionModel);
                }
            }
        } else {
            logger.error("unknown type: " + searchIdentifiers.getClass() + " value: " + searchIdentifiers);
        }
        return parsedDetections;
    }

    private void parseName(SigmaDetection detectionModel, String name) {
        String parsedName = StringUtils.substringBefore(name, SEPERATOR);

        detectionModel.setSigmaName(parsedName);
        detectionModel.setName(parsedName);

        // override name with mapped name
        if (fieldMapper != null) {
            // TODO: mapped fields can be an array - only taking first value for now
            List<String> mappedField = fieldMapper.getSigmaFields().getSigmaField(parsedName);
            if (mappedField.isEmpty() == false) {
                System.out.println("mappedField: " + mappedField.get(0));
                detectionModel.setName(mappedField.get(0));
            }
        }

        // handles the case where the operator is piped with the name (ex. field|endswith)
        if (StringUtils.contains(name, SEPERATOR))
            detectionModel.setOperator(OperatorType.getEnum(StringUtils.substringAfter(name, SEPERATOR)));
    }


    private void parseValue(SigmaDetection detectionModel, String value) throws InvalidSigmaRuleException {
        if (detectionModel.getOperator() != null) {
            detectionModel.addValue(buildStringWithOperator(value, detectionModel.getOperator()));
        }
        else {
            detectionModel.addValue(sigmaWildcardToRegex(value));
        }
    }

    // TODO We need to handle escaping in sigma
    private String buildStringWithOperator(String value, OperatorType operator) throws InvalidSigmaRuleException {

        // Sigma spec isn't clear on what to do with wildcard characters when they are in vlaues with a "transformation"
        // which we are calling operator
        if (operator != null) {
            switch (operator) {
                case STARTS_WITH:
                case BEGINS_WITH:
                    return sigmaWildcardToRegex(value) + ".*";
                case CONTAINS:
                    return ".*" + sigmaWildcardToRegex(value) + ".*";
                case ENDS_WITH:
                    return ".*" + sigmaWildcardToRegex(value);
                case REGEX:
                    if (!validRegex(value))
                        throw new InvalidSigmaRuleException("Regular expression operator specified " +
                                "but pattern did not compile for value = " + value);
                    return value;
            }
        }

        return sigmaWildcardToRegex(value);
    }

    private boolean validRegex(String regex) {
        try {
            // check if pattern is already a regex and do nothing
            Pattern.compile(regex);
            return true;
          } catch (PatternSyntaxException e) {
           return false;
          }
    }

    /**
     * This function takes a sigma expression which allows the typical search wildcards and converts it into a java regex
     * pattern.  If there are no sigma wildcards then nothing will change
     * @param value sigma pattern value
     * @return java regex pattern
     */
    private String sigmaWildcardToRegex(String value) {
        StringBuilder out = new StringBuilder();
        for(int i = 0; i < value.length(); ++i) {
            final char c = value.charAt(i);
            switch(c) {
                case '*': out.append(".*"); break;
                case '?': out.append('.'); break;
                case '.': out.append("\\."); break;
                case '\\': out.append("\\\\"); break;
                default: out.append(c);
            }
        }
        return out.toString();
    }
}
