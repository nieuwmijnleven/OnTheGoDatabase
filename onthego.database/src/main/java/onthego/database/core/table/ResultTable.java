package onthego.database.core.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.tablespace.manager.TablespaceManager;

public final class ResultTable implements Table {
	
	private final String tableName;
	
	private final List<ColumnType> columnTypeList;
	
	private final List<byte[]> records;
	
	private final Map<String,Integer> columnTypeIndexMap;
	
	public ResultTable(String tableName, List<ColumnType> columnList, List<byte[]> records) {
		this.tableName = tableName;
		this.columnTypeList = columnList;
		this.records = records;
		this.columnTypeIndexMap = new HashMap<>();
		
		initialize();
	}

	private void initialize() {
		for (int i = 0; i < this.columnTypeList.size(); ++i) {
			ColumnType column = this.columnTypeList.get(i);
			this.columnTypeIndexMap.put(column.getName(), i);
		}
	}
	
	@Override
	public void close() {}
	
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
	public Table select(List<ColumnType> selectColumn, Filtration filtration) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long insert(Map<ColumnType, String> values) {
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
	public List<ColumnType> getColumnList() {
		return Collections.unmodifiableList(this.columnTypeList);
	}

	@Override
	public int getColumnCount() {
		return this.columnTypeList.size();
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
		public int getColumnCount() {
			return ResultTable.this.getColumnCount();
		}
		
		@Override
		public ColumnType getColumnType(int columnIdx) {
			isValidColumnIndex(columnIdx);
			return columnTypeList.get(columnIdx);
		}
		
		@Override
		public ColumnType getColumnType(String columnName) {
			isValidColumnName(columnName);
			return columnTypeList.get(columnTypeIndexMap.get(columnName));
		}
		
		@Override
		public String getColumn(int columnIdx) {
			isValidColumnIndex(columnIdx);
			return StandardTableUtil.readColumnData(record, columnIdx);
		}

		@Override
		public String getColumn(String columnName) {
			isValidColumnName(columnName);
			return StandardTableUtil.readColumnData(record, columnTypeIndexMap.get(columnName));
		}
		
		private void isValidColumnName(String columnName) {
			if (!columnTypeIndexMap.containsKey(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
		}
		
		private void isValidColumnIndex(int columnIdx) {
			if (columnIdx < 0 || columnIdx >= columnTypeList.size()) {
				throw new IllegalArgumentException(columnIdx + " is not a valid column index.");
			}
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
