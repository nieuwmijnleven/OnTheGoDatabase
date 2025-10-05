package onthego.database.core.serializer;

import onthego.database.core.table.StandardTableUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StringSerializer implements Serializer<Object> {
    @Override
    public void write(DataOutputStream out, Object obj) throws IOException {
        //if (obj == null) throw new IOException("Cannot serialize null value.");
        if (obj == null) out.writeUTF("0");
        else out.writeUTF((String)obj);
    }

    @Override
    public Object read(DataInputStream in) throws IOException {
        return in.readUTF();
    }

    @Override
    public int estimateSize(Object obj) {
        String payload = (obj != null) ? (String)obj : "0";
        return Short.BYTES + StandardTableUtil.getUTFSize(payload);
    }
}
