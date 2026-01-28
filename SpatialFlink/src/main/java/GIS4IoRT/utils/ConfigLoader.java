package GIS4IoRT.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

// Dynamic Configuration Bootstrap Utility

public class ConfigLoader {
    public static ParameterTool load(String[] args, Object defaultConfig) {
        try {
            ParameterTool argsParams = ParameterTool.fromArgs(args);
            ObjectMapper mapper = new ObjectMapper();

            if (argsParams.has("configBase64")) {
                String base64Encoded = argsParams.get("configBase64");

                try {
                    byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded);
                    String json = new String(decodedBytes, StandardCharsets.UTF_8);

                    System.out.println("Recieved JSON configuration: " + json);
                    mapper.readerForUpdating(defaultConfig).readValue(json);


                } catch (Exception e) {
                    System.err.println("Config decoding error: " + e.getMessage());
                    throw new RuntimeException("Invalid parameter configBase64", e);
                }
            }

            Map<String, Object> map = mapper.convertValue(defaultConfig, new TypeReference<Map<String, Object>>() {
            });

            Map<String, String> stringMap = new HashMap<>();
            map.forEach((k, v) -> stringMap.put(k, String.valueOf(v)));

            ParameterTool finalParams = ParameterTool.fromMap(stringMap).mergeWith(argsParams);
            mapper.updateValue(defaultConfig, finalParams.toMap());
            return finalParams;
        } catch (Exception e) {
            throw new RuntimeException("Critical error loading configuration", e);
        }
    }
}
