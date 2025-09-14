package onthego.database.core.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.tablespace.manager.TablespaceManager;

public final class ResultTable implements Table {
	
	private final String tableName;
	
	private final List<ColumnMeta> columnTypeList;
	
	private final List<Integer> columnRealIndexList;
	
	private final Map<String,Integer> columnTypeIndexMap = new HashMap<>();
	
	private final List<byte[]> records;
	
	public ResultTable(String tableName, List<ColumnMeta> columnList, List<Integer> columnRealIndexList, List<byte[]> records) {
		this.tableName = tableName;
		this.columnTypeList = columnList;
		this.records = records;
		this.columnRealIndexList = columnRealIndexList;
		
		initColumnTypeIndexMap();
	}

	private void initColumnTypeIndexMap() {
		for (int i = 0; i < this.columnTypeList.size(); ++i) {
			ColumnMeta column = this.columnTypeList.get(i);
			this.columnTypeIndexMap.put(column.getName(), columnRealIndexList.get(i));
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
	public Table select(List<ColumnMeta> selectColumn, Filtration filtration) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long insert(Map<ColumnMeta, String> values) {
		throw new UnsupportedOperationException();
	}
	
	public Cursor getCursor(List<ColumnMeta> selectColumn) {
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
	
	private int getColumnIndex(String columnName) {
		return columnTypeIndexMap.get(columnName);
	}

	@Override
	public List<ColumnMeta> getColumnList() {
		return Collections.unmodifiableList(this.columnTypeList);
	}

	@Override
	public int getColumnCount() {
		return this.columnTypeList.size();
	}
	
	private class ResultTableCursor implements Cursor {
		
		private final Iterator<byte[]> iterator = records.iterator();
		
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
		public ColumnMeta getColumnType(int columnIdx) {
			if (!isValidColumnIndex(columnIdx)) {
				throw new IllegalArgumentException(columnIdx + " is not a valid column index.");
			}
			return columnTypeList.get(columnIdx);
		}
		
		@Override
		public ColumnMeta getColumnType(String columnName) {
			if (!isValidColumnName(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
			return columnTypeList.get(getColumnIndex(columnName));
		}
		
		@Override
		public String getColumn(int columnIdx) {
			return getColumn(columnTypeList.get(columnIdx).getName());
		}

		@Override
		public String getColumn(String columnName) {
			if (!isValidColumnName(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
			return StandardTableUtil.readColumnData(record, getColumnIndex(columnName));
		}
		
		private boolean isValidColumnName(String columnName) {
            return columnTypeIndexMap.containsKey(columnName);
        }
		
		private boolean isValidColumnIndex(int columnIdx) {
            return columnIdx >= 0 && columnIdx < columnTypeList.size();
        }

		@Override
		public byte[] getRawRecord() {
			return Arrays.copyOf(record, record.length);
		}

		@Override
		public Iterator<String> getRecord() {
			return new StandardRecordIterator(record, columnRealIndexList);
		} 

		@Override
		public String update(String columnName, String newValue) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void delete() {
			throw new UnsupportedOperationException();
		}

        @Override
        public void close() {

        }
    }
}
