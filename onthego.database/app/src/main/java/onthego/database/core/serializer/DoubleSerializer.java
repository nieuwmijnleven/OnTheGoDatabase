package onthego.database.core.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleSerializer implements Serializer<Object> {
    @Override
    public void write(DataOutputStream out, Object obj) throws IOException {
        //if (obj == null) throw new IOException("Cannot serialize null value.");
        if (obj == null) out.writeDouble(0.0);
        else out.writeDouble((Double)obj);
    }

    @Override
    public Object read(DataInputStream in) throws IOException {
        return in.readDouble();
    }

    @Override
    public int estimateSize(Object obj) {
        return Double.BYTES;
    }
}
