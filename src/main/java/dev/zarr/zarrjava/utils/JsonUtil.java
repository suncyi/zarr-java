package dev.zarr.zarrjava.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.commons.io.FileUtils;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.*;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.ext.javatime.deser.LocalDateDeserializer;
import tools.jackson.databind.ext.javatime.deser.LocalDateTimeDeserializer;
import tools.jackson.databind.ext.javatime.deser.LocalTimeDeserializer;
import tools.jackson.databind.ext.javatime.ser.LocalDateSerializer;
import tools.jackson.databind.ext.javatime.ser.LocalDateTimeSerializer;
import tools.jackson.databind.ext.javatime.ser.LocalTimeSerializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.type.TypeFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * @author suncy
 * @date 2024/12/5 10:10
 */
public class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = baseObjectMapperBuilder().build();
    private static final ObjectMapper PRETTY_OBJECT_MAPPER;
    private static final TypeFactory TYPE_FACTORY = TypeFactory.createDefaultInstance();
    private static final ObjectMapper OBJECT_MAPPER_LOWER ;


    static {
        PRETTY_OBJECT_MAPPER = baseObjectMapperBuilder().enable(SerializationFeature.INDENT_OUTPUT).build();
        OBJECT_MAPPER_LOWER =
                baseObjectMapperBuilder().propertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
                        .build();

    }

    private JsonUtil() {
    }

    private static JsonMapper.Builder baseObjectMapperBuilder() {
        // 3.x 中 ObjectMapper 完全兼容，无需迁移
        String dateTimePattern = "yyyy-MM-dd HH:mm:ss";
        return JsonMapper.builder()
                .changeDefaultPropertyInclusion(incl -> incl.withValueInclusion(JsonInclude.Include.NON_NULL))
                .enable(DateTimeFeature.WRITE_DATES_WITH_ZONE_ID)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .defaultDateFormat(new SimpleDateFormat(dateTimePattern))
                .defaultTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

                .addModule(createSimpleJavaTimeModule());
    }

    public static SimpleModule createSimpleJavaTimeModule() {
        String dateTimePattern = "yyyy-MM-dd HH:mm:ss";
        String datePattern = "yyyy-MM-dd";
        String timePattern = "HH:mm:ss";

        SimpleModule module = new SimpleModule("JavaTimeModule");
        // 对于 LocalDateTime 等类型，使用 SimpleModule 来注册
        module.addSerializer(LocalDateTime.class,
                new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(dateTimePattern)));
        module.addSerializer(LocalDate.class,
                new LocalDateSerializer(DateTimeFormatter.ofPattern(datePattern)));
        module.addSerializer(LocalTime.class,
                new LocalTimeSerializer(DateTimeFormatter.ofPattern(timePattern)));

        module.addDeserializer(LocalDateTime.class,
                new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(dateTimePattern)));
        module.addDeserializer(LocalDate.class,
                new LocalDateDeserializer(DateTimeFormatter.ofPattern(datePattern)));
        module.addDeserializer(LocalTime.class,
                new LocalTimeDeserializer(DateTimeFormatter.ofPattern(timePattern)));

        return module;
    }

    public static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("转化为json失败", e);
        }
    }

    public static String toPrettyJson(Object object) {
        try {
            return PRETTY_OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("转化为json失败", e);
        }
    }

    /*public static byte[] toJsonBytes(@NonNull Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new RuntimeException("转化为json字节数组失败", e);
        }
    }*/

    public static <E> List<E> fromJsonToList(String jsonStr, Class<E> elementClass) {
        return fromJson(jsonStr, TYPE_FACTORY.constructCollectionType(List.class, elementClass));
    }

    public static <E> Set<E> fromJsonToSet(String jsonStr, Class<E> elementClass) {
        return fromJson(jsonStr, TYPE_FACTORY.constructCollectionType(Set.class, elementClass));
    }

    public static <K, V> Map<K, V> fromJsonToMap(String jsonStr, Class<K> keyClass, Class<V> valueClass) {
        return fromJson(jsonStr, TYPE_FACTORY.constructMapType(Map.class, keyClass, valueClass));
    }

    public static <T> T fromJson(String jsonStr, Class<T> clazz) {
        requireNonBlankJson(jsonStr);
        try {
            return OBJECT_MAPPER.readValue(jsonStr, clazz);
        } catch (Exception e) {
            throw new RuntimeException("从json " + jsonStr + " 转化为" + clazz.getName() + "对象失败", e);
        }
    }

    public static <T> T fromJson(String jsonStr, JavaType javaType) {
        requireNonBlankJson(jsonStr);
        try {
            return OBJECT_MAPPER.readValue(jsonStr, javaType);
        } catch (Exception e) {
            throw new RuntimeException("从json " + jsonStr + " 转化为" + javaType + "对象失败", e);
        }
    }

    public static <T> T fromJson(String jsonStr, TypeReference<T> valueTypeRef) {
        requireNonBlankJson(jsonStr);
        try {
            return OBJECT_MAPPER.readValue(jsonStr, valueTypeRef);
        } catch (Exception e) {
            throw new RuntimeException("从json " + jsonStr + " 转化为" + valueTypeRef + "对象失败", e);
        }
    }

    public static <T> T fromJsonBytes(byte[] bytes, TypeReference<T> valueTypeRef) {
        if (bytes == null) {
            throw new NullPointerException("bytes is marked non-null but is null");
        }
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        } catch (Exception e) {
            throw new RuntimeException("从json bytes " + new String(bytes) + " 转化为" + valueTypeRef + "对象失败", e);
        }
    }

    public static ObjectNode createObjectNode() {
        return OBJECT_MAPPER.createObjectNode();
    }

    public static ArrayNode createArrayNode() {
        return OBJECT_MAPPER.createArrayNode();
    }

    private static void requireNonBlankJson(String jsonStr) throws IllegalArgumentException {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            throw new IllegalArgumentException("待解析json串不能为空, jsonStr: <" + jsonStr + ">");
        }
    }

    /**
     * @param jsonStr
     * @param jsonPath /data/result 对应 {“data":{”result“:""}}
     * @return
     */
    public static String readSingleNodeVal(String jsonStr, String jsonPath) {
        requireNonBlankJson(jsonStr);
        try {
            JsonNode rootJsonNode = OBJECT_MAPPER.readTree(jsonStr);
            JsonNode jsonNode = rootJsonNode.at(jsonPath);
            return jsonNode.asText();
//            return jsonNode.toString();
        } catch (Exception e) {
            throw new RuntimeException("从json " + jsonStr + " 读取key " + jsonPath + " 失败", e);
        }
    }

    public static String readSingleSubNode(String jsonStr, String jsonPath) {
        requireNonBlankJson(jsonStr);
        try {
            JsonNode rootJsonNode = OBJECT_MAPPER.readTree(jsonStr);
            JsonNode jsonNode = rootJsonNode.at(jsonPath);
//            return jsonNode.asText();
            return jsonNode.toString();
        } catch (Exception e) {
            throw new RuntimeException("从json " + jsonStr + " 读取key " + jsonPath + " 失败", e);
        }
    }


    public static void main(String[] args) throws IOException {
//        Map<String, Object> map = Map.of("sdsdsf", 234, "ttttt", "sdfsdf");
//        String json = toJson(map);
//        System.out.println(json);
//        map = fromJson(json, new TypeReference<Map<String, Object>>() {
//        });
//        System.out.println(map);

        String json2 = FileUtils.readFileToString(new File("D:\\code\\dh\\cacs-client\\src\\test\\resources\\forecast" +
                ".json"), "UTF-8");
        String s = JsonUtil.readSingleNodeVal(json2, "/code");
        System.out.println(s);
    }

}
