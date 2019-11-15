package onthego.database.core.table;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class StandardRecordIterator implements Iterator<String> {
	
	private final byte[] record;
	
	private final List<Integer> columnRealIndexList;
	
	private final int columnCount;
	
	private int columnIndex;

	public StandardRecordIterator(byte[] record, List<Integer> columnRealIndexList) {
		Objects.requireNonNull(record);
		Objects.requireNonNull(columnRealIndexList);
		
		this.record = record;
		this.columnRealIndexList = columnRealIndexList;
		this.columnCount = columnRealIndexList.size();
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
		return StandardTableUtil.readColumnData(record, mapToColumnRealIndex(columnIndex++));
	}

	private Integer mapToColumnRealIndex(int columnIndex) {
		return columnRealIndexList.get(columnIndex);
	}
}
