/*
 * Copyright 2015 Avanza Bank AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avanza.ymer;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MirroredObjectDefinitionsOverride {

    Logger log = LoggerFactory.getLogger(MirroredObjectDefinitionsOverride.class);

    boolean excludeFromInitialLoad(MirroredObjectDefinition<?> definition);
    boolean writeBackPatchedDocuments(MirroredObjectDefinition<?> definition);
    boolean loadDocumentsRouted(MirroredObjectDefinition<?> definition);
    PersistInstanceIdDefinition<?> persistInstanceId(MirroredObjectDefinition<?> definition);

    static MirroredObjectDefinitionsOverride noOverride() {
        return new MirroredObjectDefinitionsOverrideNone();
    }

    static MirroredObjectDefinitionsOverride fromSystemProperties() {
        return new MirroredObjectDefinitionsOverrideSystemProperties();
    }

    class MirroredObjectDefinitionsOverrideNone implements MirroredObjectDefinitionsOverride {
        @Override
        public boolean excludeFromInitialLoad(MirroredObjectDefinition<?> definition) {
            return definition.excludeFromInitialLoad();
        }

        @Override
        public boolean writeBackPatchedDocuments(MirroredObjectDefinition<?> definition) {
            return definition.writeBackPatchedDocuments();
        }

        @Override
        public boolean loadDocumentsRouted(MirroredObjectDefinition<?> definition) {
            return definition.loadDocumentsRouted();
        }

        @Override
        public PersistInstanceIdDefinition<?> persistInstanceId(MirroredObjectDefinition<?> definition) {
            return definition.getPersistInstanceId();
        }
    }

    class MirroredObjectDefinitionsOverrideSystemProperties implements MirroredObjectDefinitionsOverride {
        @Override
        public boolean excludeFromInitialLoad(MirroredObjectDefinition<?> definition) {
            return getProperty(definition, "excludeFromInitialLoad")
                    .orElse(definition.excludeFromInitialLoad());
        }

        @Override
        public boolean writeBackPatchedDocuments(MirroredObjectDefinition<?> definition) {
            return getProperty(definition, "writeBackPatchedDocuments")
                    .orElse(definition.writeBackPatchedDocuments());
        }

        @Override
        public boolean loadDocumentsRouted(MirroredObjectDefinition<?> definition) {
            return getProperty(definition, "loadDocumentsRouted")
                    .orElse(definition.loadDocumentsRouted());
        }

        @Override
        public PersistInstanceIdDefinition<?> persistInstanceId(MirroredObjectDefinition<?> definition) {
            PersistInstanceIdDefinition<?> persistInstanceId = PersistInstanceIdDefinition.from(definition.getPersistInstanceId());
            getProperty(definition, "persistInstanceId").ifPresent(persistInstanceId::enabled);
            getProperty(definition, "triggerInstanceIdCalculationOnStartup").ifPresent(persistInstanceId::triggerCalculationOnStartup);
            getIntProperty(definition, "triggerInstanceIdCalculationWithDelay")
                    .map(Duration::ofSeconds)
                    .ifPresent(persistInstanceId::triggerCalculationWithDelay);
            return persistInstanceId;
        }

        private Optional<Boolean> getProperty(MirroredObjectDefinition<?> definition, String setting) {
            return Optional.ofNullable(System.getProperty(getPropertyName(definition, setting)))
                    .filter(s -> s.equals("true") || s.equals("false"))
                    .map("true"::equals);
        }

        private Optional<Integer> getIntProperty(MirroredObjectDefinition<?> definition, String setting) {
            return Optional.ofNullable(System.getProperty(getPropertyName(definition, setting)))
                    .flatMap(s -> {
                        try {
                            return Optional.of(Integer.valueOf(s));
                        } catch (NumberFormatException e) {
                            log.warn("Could not parse setting {} with value [{}] as int", setting, s);
                            return Optional.empty();
                        }
                    });
        }

        public static String getPropertyName(MirroredObjectDefinition<?> definition, String setting) {
            return "ymer." + definition.getMirroredType().getCanonicalName() + "." + setting;
        }
    }

}
