/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Contains the configuration for a Conduit destination connector.
 */
@Getter
@Setter
public class DestinationConfig {
    /**
     * Creates a new <code>DestinationConfig</code> instance from a map with configuration parameters.
     */
    public static DestinationConfig fromMap(Map<String, String> map) {
        if (Utils.isEmpty(map)) {
            return new DestinationConfig();
        }
        return fromMap(map, DestinationConfig.class);
    }

    protected static <T extends DestinationConfig> T fromMap(Map<String, String> map, Class<T> clazz) {
        Map<String, String> connectorMap = new HashMap<>();

        map.forEach(connectorMap::put);
        T cfg = Utils.mapper.convertValue(connectorMap, clazz);

        return cfg;
    }
}
