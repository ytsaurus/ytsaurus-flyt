package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.io.InputStream;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.platform.commons.util.Preconditions;

@UtilityClass
public class TestResourceUtils {
    @SneakyThrows
    public static String readResource(String name) {
        return new String(readResourceBytes(name)).trim();
    }

    @SneakyThrows
    public static byte[] readResourceBytes(String name) {
        ClassLoader classLoader = TestResourceUtils.class.getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(name)) {
            Preconditions.notNull(stream, "Test resource not found: " + name);
            return stream.readAllBytes();
        }
    }

    @SneakyThrows
    public static <T> T readResourceJsonOrDefault(String name, Class<T> clazz, T defaultValue) {
        ClassLoader classLoader = TestResourceUtils.class.getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(name)) {
            if (stream == null) {
                return defaultValue;
            }
            String data = new String(stream.readAllBytes()).trim();
            return new ObjectMapper().readValue(data, clazz);
        }
    }

    @SneakyThrows
    public static boolean hasResource(String name) {
        ClassLoader classLoader = TestResourceUtils.class.getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(name)) {
            return stream != null;
        }
    }
}
