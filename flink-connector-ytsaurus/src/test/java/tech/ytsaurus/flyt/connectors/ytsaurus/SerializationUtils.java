package tech.ytsaurus.flyt.connectors.ytsaurus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class SerializationUtils {

    public static byte[] serialize(Object obj) throws Exception {
        try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
            try (ObjectOutputStream o = new ObjectOutputStream(b)) {
                o.writeObject(obj);
                return b.toByteArray();
            }
        }
    }

    public static Object deserialize(byte[] bytes) throws Exception {
        try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream o = new ObjectInputStream(b)) {
                return o.readObject();
            }
        }
    }
}
