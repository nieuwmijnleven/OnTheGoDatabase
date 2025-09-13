package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongSerializer implements Serializer<Long> {
    @Override
    public void write(DataOutputStream out, Long obj) throws IOException {
        if (obj == null) out.writeLong(0);
        else out.writeLong(obj);
    }

    @Override
    public Long read(DataInputStream in) throws IOException {
        return in.readLong();
    }

    @Override
    public long estimateSize(Long obj) {
        return Long.BYTES;
    }
}
