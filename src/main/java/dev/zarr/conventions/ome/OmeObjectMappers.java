package dev.zarr.conventions.ome;


import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.DeserializationProblemHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

final class OmeObjectMappers {
    private OmeObjectMappers() {
    }

    static ObjectMapper makeV2Mapper() {
        return dev.zarr.zarrjava.v2.Node.makeObjectMapper()
                .rebuild()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .addHandler(new UnknownOmePropertyWarningHandler())
                .build();
    }

    static ObjectMapper makeV3Mapper() {
        return dev.zarr.zarrjava.v3.Node.makeObjectMapper()
                .rebuild()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .addHandler(new UnknownOmePropertyWarningHandler())
                .build();
    }

    private static final class UnknownOmePropertyLoggingHandler extends DeserializationProblemHandler {
        private static final Logger LOGGER = Logger.getLogger(UnknownOmePropertyLoggingHandler.class.getName());
        private static final Set<String> UNKNOWN_FIELDS = ConcurrentHashMap.newKeySet();

        @Override
        public boolean handleUnknownProperty(
                DeserializationContext ctxt,
                JsonParser p,
                ValueDeserializer<?> deserializer,
                Object beanOrClass,
                String propertyName
        ) {
            String target = (beanOrClass instanceof Class)
                    ? ((Class<?>) beanOrClass).getName()
                    : beanOrClass.getClass().getName();
            String key = target + "#" + propertyName;
            if (UNKNOWN_FIELDS.add(key)) {
                LOGGER.info(
                        "Ignoring unknown OME metadata field '" + propertyName + "' for " + target);
            }
            p.skipChildren();
            return true;
        }
    }
}
