package util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class JsonUtils {
  public static final ObjectMapper objectMapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());

  public static Map<String, Object> parseAsMap(String json) throws IOException {
    return objectMapper.readValue(json, new TypeReference<Map<String, Object>>(){});
  }

  public static String writeAsString(Object json) {
    try {
      return objectMapper.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      log.error("[JsonUtils.writeAsString] " + e.getMessage(), e);
    }

    return StringUtils.EMPTY;
  }
}
