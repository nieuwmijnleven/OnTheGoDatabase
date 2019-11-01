package onthego.database.core.table;

import java.nio.ByteBuffer;

public class StandardTableUtil {

	private StandardTableUtil() {}
	
	public static int readUnsignedShort(ByteBuffer byteBuffer) {
        int high = byteBuffer.get();
        int low = byteBuffer.get();
        return (high << 8) + (low << 0);
    }
	
	public static int readUnsignedShort(ByteBuffer byteBuffer, int position) {
        byteBuffer.position(position);
        return readUnsignedShort(byteBuffer);
    }
	
	public static int readUnsignedShort(byte[] record, int position) {
		return readUnsignedShort(ByteBuffer.wrap(record), position);
    }
	
	public static void writeUnsignedShort(ByteBuffer byteBuffer, int value) {
		byteBuffer.put((byte)((value >>> 8) & 0xFF));
		byteBuffer.put((byte)((value >>> 0) & 0xFF));
    }
	
	public static void writeUnsignedShort(ByteBuffer byteBuffer, int position, int value) {
		byteBuffer.position(position);
		writeUnsignedShort(byteBuffer, value);
    }
	
	public static void writeUnsignedShort(byte[] record, int position, int value) {
		writeUnsignedShort(ByteBuffer.wrap(record), position, value);
    }
	
	public static int getUTFSize(String str) {
		int strlen = str.length();
        int utflen = 0;
        int c;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }
        
        return utflen;
	}
	
	public static String readUTF(ByteBuffer byteBuffer) {
        int utflen = readUnsignedShort(byteBuffer);
        
        char[] chararr = new char[utflen];

        int c, char2, char3;
        final int endIndex = byteBuffer.position() + utflen;
        int chararr_count=0;

        while (byteBuffer.position() < endIndex) {
            c = (int) byteBuffer.get() & 0xff;
            if (c > 127) break;
            chararr[chararr_count++]=(char)c;
        }

        while (byteBuffer.position() < endIndex) {
            c = (int) byteBuffer.get() & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
//                    startIndex++;
                    chararr[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
//                    startIndex += 2;
                	byteBuffer.get();
                    if (byteBuffer.position() > endIndex)
                        throw new IllegalArgumentException(
                            "malformed input: partial character at end");
                    char2 = (int) byteBuffer.get(byteBuffer.position() - 1);
                    if ((char2 & 0xC0) != 0x80)
                        throw new IllegalArgumentException(
                            "malformed input around byte " + byteBuffer.position());
                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
                                                    (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                	byteBuffer.get();
                	byteBuffer.get();
//                    startIndex += 3;
                    if (byteBuffer.position() > endIndex)
                        throw new IllegalArgumentException(
                            "malformed input: partial character at end");
                    char2 = (int) byteBuffer.get(byteBuffer.position() - 2);
                    char3 = (int) byteBuffer.get(byteBuffer.position() - 1);
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new IllegalArgumentException(
                            "malformed input around byte " + (byteBuffer.position() - 1));
                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                    ((char2 & 0x3F) << 6)  |
                                                    ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new IllegalArgumentException(
                        "malformed input around byte " + byteBuffer.position());
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
	
	public static String readUTF(ByteBuffer byteBuffer, int position) {
		byteBuffer.position(position);
		return readUTF(byteBuffer);
	}
	
	public static String readUTF(byte[] record, int position) {
        return readUTF(ByteBuffer.wrap(record), position);
    }
	
	public static void writeUTF(ByteBuffer byteBuffer, String str) {
        int utflen = getUTFSize(str);
        writeUnsignedShort(byteBuffer, utflen);

        int c;
        int i=0;
        for (i=0; i< str.length(); i++) {
           c = str.charAt(i);
           if (!((c >= 0x0001) && (c <= 0x007F))) break;
           byteBuffer.put((byte)c);
        }

        for (;i < str.length(); i++){
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
            	byteBuffer.put((byte)c);

            } else if (c > 0x07FF) {
            	byteBuffer.put((byte)(0xE0 | ((c >> 12) & 0x0F)));
            	byteBuffer.put((byte)(0x80 | ((c >>  6) & 0x3F)));
            	byteBuffer.put((byte)(0x80 | ((c >>  0) & 0x3F)));
            } else {
            	byteBuffer.put((byte)(0xC0 | ((c >>  6) & 0x1F)));
            	byteBuffer.put((byte)(0x80 | ((c >>  0) & 0x3F)));
            }
        }
    }
	
	public static void writeUTF(ByteBuffer byteBuffer, int position, String str) {
		byteBuffer.position(position);
		writeUTF(byteBuffer, str);
	}
	
	public static void writeUTF(byte[] record, int position, String str) {
		writeUTF(ByteBuffer.wrap(record), position, str);
    }
	
	public static String readColumnData(byte[] record, int n) {
		return readUTF(record, readUnsignedShort(record, (n + 1) * Short.BYTES));
	}
	
	public static byte[] writeColumnData(byte[] record, int n, String data) {
		int prevColumnDataOffset = readUnsignedShort(record, (n + 1) * 2);
		int prevColumnDataLength = readUnsignedShort(record, prevColumnDataOffset);
		int columnDataLength = getUTFSize(data);
		
		if (prevColumnDataLength >= columnDataLength) {
			writeUTF(record, prevColumnDataOffset, data);
			return record;
		} else {
			int diff = columnDataLength - prevColumnDataLength;
			byte[] newRecord = new byte[record.length + diff];
			
			int columnCount = readUnsignedShort(record, 0);
			writeUnsignedShort(newRecord, 0, columnCount);
			
			int payloadOffset = Short.BYTES * (1 + columnCount);
			for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
				String value = null;
				if (columnIndex != n) {
					value = readColumnData(record, columnIndex);
				} else {
					value = data;
				}
				
				writeUnsignedShort(newRecord, (columnIndex + 1) * Short.BYTES, payloadOffset);
				writeUTF(newRecord, payloadOffset, value);
				payloadOffset += StandardTableUtil.getUTFSize(value) + Short.BYTES;
			}
			return newRecord;
		}
	}
}
