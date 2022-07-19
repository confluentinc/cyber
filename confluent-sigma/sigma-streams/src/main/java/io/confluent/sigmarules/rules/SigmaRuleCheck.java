/*
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
 *
 */

package io.confluent.sigmarules.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import io.confluent.sigmarules.models.DetectionsManager;
import io.confluent.sigmarules.models.SigmaCondition;
import io.confluent.sigmarules.models.SigmaDetection;
import io.confluent.sigmarules.models.SigmaDetections;
import io.confluent.sigmarules.models.SigmaRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SigmaRuleCheck {
  final static Logger logger = LogManager.getLogger(SigmaRuleCheck.class);

  public Boolean isValid(SigmaRule rule, JsonNode data, Configuration jsonPathConf) {
    if (rule != null)
      return checkConditions(rule, data, jsonPathConf);

    logger.error("Invalid Rule");
    return false;
  }

  private Boolean checkConditions(SigmaRule rule, JsonNode sourceData, Configuration jsonPathConf) {
    for (SigmaCondition c : rule.getConditionsManager().getConditions()) {
      if (!c.getAggregateCondition()) {
        if (checkCondition(c, rule.getDetectionsManager(), sourceData, jsonPathConf)) {
          logger.debug("source data: " + sourceData.toString());
          return true;
        }
      }
    }

    return false;
  }

  private Boolean checkCondition(SigmaCondition condition, DetectionsManager detections, JsonNode sourceData, Configuration jsonPathConf) {
    if (!condition.getAggregateCondition()) {
      Boolean pairedResult = false;
      if (condition.getPairedCondition() != null) {
        pairedResult = checkCondition(condition.getPairedCondition(), detections, sourceData, jsonPathConf);

        // if paired condition is true and operator is OR, no need to check
        if (pairedResult && condition.getOperator().equals("OR")) {
          return true;
        } else if (condition.getOperator().equals("OR")) {
          return checkParentConditions(condition, detections, sourceData, jsonPathConf);
        } else { //AND case
          Boolean myResult = checkParentConditions(condition, detections, sourceData, jsonPathConf);
          return pairedResult && myResult;
        }
      } else {
        return checkParentConditions(condition, detections, sourceData, jsonPathConf);
      }
    }

    return false;
  }

  private Boolean checkParentConditions(SigmaCondition condition, DetectionsManager detections,
      JsonNode sourceData, Configuration jsonPathConf) {
    // detections grouped together are combined for a status
    Boolean validDetections = false;
    SigmaDetections myDetections = detections.getDetectionsByName(condition.getConditionName());

    if (myDetections != null) {
      for (SigmaDetection d : myDetections.getDetections()) {
        String name = d.getName();

        if (name.charAt(0) == '$') {
          JsonNode jsonPathSourceValues = JsonPath.using(jsonPathConf).parse(sourceData.toString()).read(name, JsonNode.class);
          if (jsonPathSourceValues == null) break;
          validDetections = beginDetectionProcessing(jsonPathSourceValues, d, condition);
        } else if (sourceData.has(name)) {
          JsonNode sourceValues = sourceData.get(name);
          validDetections = beginDetectionProcessing(sourceValues, d, condition);
        } else {
          // does not contain all detection names
          return false;
        }
      }

      if (validDetections) {
        return true;
      }
    } else {
      logger.info("No detections for condition: " + condition.getConditionName() +
          " source: " + sourceData.toString());
    }

    return false;
  }

  private Boolean beginDetectionProcessing(JsonNode sourceValues, SigmaDetection d, SigmaCondition condition) {
    Boolean validDetections = false;
    if (sourceValues.isArray()) {
      for (final JsonNode sourceValue : sourceValues) {
        if (!checkValue(condition, d, sourceValue.asText())) {
          return false;
        } else {
          validDetections = true;
        }
      }
    } else {
      // this is a string
      String sourceValue = sourceValues.asText();
      if (!checkValue(condition, d, sourceValue)) {
        return false;
      } else {
        validDetections = true;
      }
    }
    return validDetections;
  }

  private Boolean checkValue(SigmaCondition condition, SigmaDetection model, String sourceValue) {
    if (model.matches(sourceValue, condition.getNotCondition())) {
      return true;
    } else {
      return false;
    }
  }

}
