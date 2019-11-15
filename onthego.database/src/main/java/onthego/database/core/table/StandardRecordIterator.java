package onthego.database.core.table;

import java.util.Iterator;
import java.util.List;

public class StandardRecordIterator implements Iterator<String> {
	
	private byte[] record;
	
	private List<Integer> selectColumnIndex;
	
	private final int columnCount;
	
	private int columnIndex;

	public StandardRecordIterator(byte[] record, List<Integer> selectColumnIndex) {
		if (record == null) {
			throw new IllegalArgumentException();
		}
		
		this.record = record;
		this.selectColumnIndex = selectColumnIndex;
		this.columnCount = selectColumnIndex.size();
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
		return StandardTableUtil.readColumnData(record, selectColumnIndex.get(columnIndex++));
	}
}
