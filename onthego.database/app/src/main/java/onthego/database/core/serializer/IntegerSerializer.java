package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerSerializer implements Serializer<Integer> {
    @Override
    public void write(DataOutputStream out, Integer obj) throws IOException {
        if (obj == null) out.writeInt(0);
        else out.writeInt(obj);
    }

    @Override
    public Integer read(DataInputStream in) throws IOException {
        return in.readInt();
    }

    @Override
    public long estimateSize(Integer obj) {
        return Integer.BYTES;
    }
}
