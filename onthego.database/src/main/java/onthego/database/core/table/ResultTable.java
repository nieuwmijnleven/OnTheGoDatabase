package onthego.database.core.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import onthego.database.core.table.meta.Column;
import onthego.database.core.tablespace.manager.TablespaceManager;

public final class ResultTable implements Table {
	
	private final String tableName;
	
	private final List<Column> columnList;
	
	private final List<byte[]> records;
	
	private final Map<String,Integer> columnIndexMap;
	
	public ResultTable(String tableName, List<Column> columnList, List<byte[]> records) {
		this.tableName = tableName;
		this.columnList = columnList;
		this.records = records;
		this.columnIndexMap = new HashMap<>();
		
		initialize();
	}

	private void initialize() {
		for (int i = 0; i < this.columnList.size(); ++i) {
			Column column = this.columnList.get(i);
			this.columnIndexMap.put(column.getName(), i);
		}
	}
	
	@Override
	public void begin() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rollback(boolean all) {
		throw new UnsupportedOperationException();

	}

	@Override
	public void commit(boolean all) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Table select(Filtration filtration) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long insert(Map<Column, String> values) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Cursor getCursor() {
		return new ResultTableCursor();
	}

	@Override
	public int update(Filtration filtration) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int delete(Filtration filtration) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getTableName() {
		return this.tableName;
	}

	@Override
	public TablespaceManager getTablespaceManager() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Column> getColumnList() {
		return Collections.unmodifiableList(this.columnList);
	}

	@Override
	public int getColumnCount() {
		return this.columnList.size();
	}
	
	private class ResultTableCursor implements Cursor {
		
		private Iterator<byte[]> iterator = records.iterator();
		
		private byte[] record;

		@Override
		public String getTableName() {
			return ResultTable.this.getTableName();
		}

		@Override
		public boolean next() {
			if (iterator.hasNext()) {
				record = iterator.next();
				return true;
			}
			return false;
		}

		@Override
		public String getColumn(String columnName) {
			if (!columnIndexMap.containsKey(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
			return StandardTableUtil.readColumnData(record, columnIndexMap.get(columnName));
		}

		@Override
		public byte[] getRawRecord() {
			return Arrays.copyOf(record, record.length);
		}

		@Override
		public Iterator<String> getRecord() {
			return new StandardRecordIterator(record, getColumnCount());
		}

		@Override
		public String update(String columnName, String newValue) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void delete() {
			throw new UnsupportedOperationException();
		}
	}
}
