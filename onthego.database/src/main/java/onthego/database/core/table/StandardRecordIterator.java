package onthego.database.core.table;

import java.util.Iterator;

public class StandardRecordIterator implements Iterator<String> {
	
	private byte[] record;
	
	private int columnCount;
	
	private int columnIndex;
	
	public StandardRecordIterator(byte[] record, int columnCount) {
		this.record = record;
		this.columnCount = columnCount;
		this.columnIndex = 0;
	}

	@Override
	public boolean hasNext() {
		if (columnIndex < columnCount) {
			return true;
		}
		return false;
	}

	@Override
	public String next() {
		if (!hasNext()) {
			throw new IndexOutOfBoundsException();
		}
		return StandardTableUtil.readColumnData(record, columnIndex++);
	}

}
