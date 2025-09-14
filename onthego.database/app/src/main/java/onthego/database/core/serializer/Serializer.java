package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serializer<T> {
    void write(DataOutputStream out, T obj) throws IOException;
    T read(DataInputStream in) throws IOException;
    long estimateSize(T obj);
}
