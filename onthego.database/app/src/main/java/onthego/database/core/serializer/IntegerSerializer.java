package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerSerializer implements Serializer<Object> {
    @Override
    public void write(DataOutputStream out, Object obj) throws IOException {
        //if (obj == null) throw new IOException("Cannot serialize null value.");
        if (obj == null) out.writeInt(0);
        else out.writeInt((Integer)obj);
    }

    @Override
    public Object read(DataInputStream in) throws IOException {
        return in.readInt();
    }

    @Override
    public int estimateSize(Object obj) {
        return Integer.BYTES;
    }
}
