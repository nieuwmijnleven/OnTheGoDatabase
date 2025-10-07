package onthego.database.core.table;

import onthego.database.core.database.DatabaseException;
import onthego.database.core.index.BPlusTreeIndex;
import onthego.database.core.index.BTreeRecordInfo;
import onthego.database.core.serializer.LongSerializer;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.table.meta.Type;
import onthego.database.core.tablespace.manager.StandardTablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManager;
import onthego.database.core.tablespace.meta.StandardTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StandardTable implements Table {

	private final String tableName;
	
	private final TablespaceManager tsManager;
	
	private final BPlusTreeIndex<Object> clusteredIndex;
	
	private Stack<List<RecordTrackableUndo>> transactionStack;
	
	// To create a standard table
	private StandardTable(String path, String tableName, TableMetaInfo tableMetaInfo) throws IOException {
		TablespaceHeader tsHeader = new StandardTablespaceHeader.Builder()
										.chunkSize(16)
										.tableMetaInfo(tableMetaInfo)
										.build();
		this.tsManager = StandardTablespaceManager.create(path + File.separator + tableName + ".db", tsHeader);

        Optional<ColumnMeta> keyColumn = tableMetaInfo.getColumnList().stream().filter(ColumnMeta::isKey).findFirst();
        if (keyColumn.isPresent()) {
            Type type = keyColumn.get().getType();
            this.clusteredIndex = new BPlusTreeIndex<>(128, type.getSerializer(), type.getComparator(), tsManager);
        } else {
            this.clusteredIndex = new BPlusTreeIndex<>(128, new LongSerializer(), Comparator.comparing(Long.class::cast), tsManager);
        }

		this.tableName = tableName;
		this.transactionStack = new Stack<>();
	}
	
	// To load a standard table
	private StandardTable(String path, String tableName) throws IOException {
		this.tsManager = StandardTablespaceManager.load(path + File.separator + tableName + ".db");
		this.clusteredIndex = new BPlusTreeIndex<>(128, tsManager);
		this.tableName = tableName;
		this.transactionStack = new Stack<>();
	}
	
	public static Table create(String path, String tableName, TableMetaInfo tableMetaInfo) throws IOException {
		return new StandardTable(path, tableName, tableMetaInfo);
	}
	
	public static Table load(String path, String tableName) throws IOException {
		return new StandardTable(path, tableName);
	}
	
	private void addToTransactionStack(RecordTrackableUndo undo) {
		if (!transactionStack.isEmpty()) {
			List<RecordTrackableUndo> transactionList = transactionStack.peek();
			transactionList.add(0, undo);
		}
	}
	
	@Override
	public void close() {
		tsManager.close();
	}
	
	@Override
	public void begin() {
		transactionStack.push(new LinkedList<>());
	}
	
	@Override
	public void rollback(boolean all) {
		if (transactionStack.isEmpty()) {
			throw new IllegalStateException("There is no BEGIN for ROLLBACK");
		}

        UndoRecordTracker<Long> recordTracker = UndoRecordTracker.create();
		do {
			List<RecordTrackableUndo> transactionList = transactionStack.pop();
			for (RecordTrackableUndo undo : transactionList) {
                undo.setRecordPosTracker(recordTracker);
				undo.execute();
			}
		} while (all && !transactionStack.isEmpty());
	}
	
	@Override
	public void commit(boolean all) {
		if (transactionStack.isEmpty()) {
			throw new IllegalStateException("There is no BEGIN for COMMIT");
		}
		
		if (all) {
			this.transactionStack = new Stack<>();
		} else {
			List<RecordTrackableUndo> transactionList = transactionStack.pop();
			if (!transactionStack.isEmpty()) {
				List<RecordTrackableUndo> higherTransactionList = transactionStack.peek();
				higherTransactionList.addAll(0, transactionList);
			}
		}
	}

	private long insertRecord(Object key, byte[] payload) {
		long recordPos = tsManager.allocate(payload.length);
		tsManager.writeBlock(recordPos, payload);
		tsManager.increaseRecordCount();

        key = (key != null) ? key : recordPos;
        clusteredIndex.insert(key, recordPos);
        return recordPos;
	}
	
	private void updateRecord(long recordPos, byte[] newRecord) {
		tsManager.writeBlock(recordPos, newRecord);
	}

	private void deleteRecord(Object key, long recordPos) {
        clusteredIndex.delete(key);
        tsManager.free(recordPos);
        tsManager.decreaseRecordCount();
	}
	
	@Override
	public Table select(List<ColumnMeta> selectColumns, Filtration filtration) throws DatabaseException {
		List<byte[]> filteredRecords = new ArrayList<>();
	 	Cursor cursor = getCursor();
		while (cursor.next()) {
			if (filtration.filter(new Cursor[]{cursor})) {
				filteredRecords.add(cursor.getRawRecord());
			}
		}
		return new ResultTable(getTableName(), selectColumns, getColumnRealIndexList(selectColumns), filteredRecords);
	}
	
	private List<Integer> getColumnRealIndexList(List<ColumnMeta> selectColumns) {
		if (selectColumns.isEmpty()) {
			return IntStream.range(0, getColumnList().size())
							.boxed().collect(Collectors.toList());
		} else {
			List<Integer> selectColumnRealIndexList = new ArrayList<>();
			for (ColumnMeta column : selectColumns) {
				selectColumnRealIndexList.add(getColumnIndex(column.getName()));
			}
			return selectColumnRealIndexList;
		}
	}

    //<column_count><offset_of_payloads><length_of_columndata1><columndata1><...>
    //writeUTF(): write size of colums and column data.
	@Override
	public long insert(Map<ColumnMeta,String> values) {
		int recordSize = Short.BYTES * (1 + values.size());
		for (String value : values.values()) {
			recordSize += Short.BYTES + StandardTableUtil.getUTFSize(value);
		}
		
		ByteBuffer byteBuffer = ByteBuffer.allocate(recordSize);
		StandardTableUtil.writeUnsignedShort(byteBuffer, getColumnCount());

        Object key = null;
		int offset = Short.BYTES * (1 + getColumnCount());
		for (ColumnMeta column : getColumnList()) {
			StandardTableUtil.writeUnsignedShort(byteBuffer, offset);
			
			byteBuffer.mark();
			byteBuffer.position(offset);
			
			String value = (values.get(column) != null) ? values.get(column) : "";
            if (column.isKey()) {
                key = column.getType().cast(value);
            }
			//System.out.println("value = " + value);
			StandardTableUtil.writeUTF(byteBuffer, value);
			
			byteBuffer.reset();
			offset += Short.BYTES + StandardTableUtil.getUTFSize(value);
		}

		long recordPos = insertRecord(key, byteBuffer.array());
		addToTransactionStack(new UndoInsert(key, recordPos));
		return recordPos;
	}
	
	@Override
	public Cursor getCursor(List<ColumnMeta> selectColumn) {
		return new StandardTableCursor(selectColumn);
	}
	
	@Override
	public Cursor getCursor() {
		return getCursor(getColumnList());
	}
	
	@Override
	public int update(Filtration filtration) throws DatabaseException {
		int updated = 0;
		Cursor cursor = getCursor();
		while (cursor.next()) {
			if (filtration.filter(new Cursor[]{cursor})) {
				filtration.update(cursor);
				++updated;
			}
		}
		return updated;
	}
	
	@Override
	public int delete(Filtration filtration) throws DatabaseException {
		int deleted = 0;
		Cursor cursor = getCursor();
		while (cursor.next()) {
			if (filtration.filter(new Cursor[]{cursor})) {
				cursor.delete();
				++deleted;
			}
		}
		return deleted;
	}

	@Override
	public String getTableName() {
		return this.tableName;
	}
	
	@Override
	public TablespaceManager getTablespaceManager() {
		return this.tsManager;
	}
	
	@Override
	public List<ColumnMeta> getColumnList() {
		return tsManager.getHeader().getTableMetaInfo().getColumnList();
	}

	@Override
	public int getColumnCount() {
		return tsManager.getHeader().getTableMetaInfo().getColumnCount();
	}
	
	private int getColumnIndex(String name) {
		return tsManager.getHeader().getTableMetaInfo().getColumnIndex(name);
	}
	
	private final class UndoInsert implements RecordTrackableUndo {

        private final Object key;

		private final long recordPos;

        private RecordTracker<Long> recordTracker;

		public UndoInsert(Object key, long recordPos) {
			this.key = key;
            this.recordPos = recordPos;
		}

        @Override
        public void setRecordPosTracker(RecordTracker<Long> recordTracker) {
            this.recordTracker = recordTracker;
        }

        @Override
		public void execute() {
            if (recordTracker == null) {
                throw new RecordTrackerException("RecordTracker must be set before executing undo.");
            }

            Object determinedKey = (key != null) ? key : recordPos;
            recordTracker.getNewPosition(recordPos).ifPresentOrElse( newPosition -> deleteRecord(determinedKey, newPosition),
                    () -> deleteRecord(determinedKey, recordPos)
            );
		}
	}
	
	private final class UndoUpdate implements RecordTrackableUndo {
		private final long recordPos;
		
		private final byte[] record;
		
		private final int columnIndex;
		
		private final String oldValue;

        private RecordTracker<Long> recordTracker;
		
		public UndoUpdate(long recordPos, byte[] record, int columnIndex, String oldValue) {
			this.recordPos = recordPos;
			this.record = record;
			this.columnIndex = columnIndex;
			this.oldValue = oldValue;
		}

        @Override
        public void setRecordPosTracker(RecordTracker<Long> recordTracker) {
            this.recordTracker = recordTracker;
        }

        @Override
		public void execute() {
            if (recordTracker == null) {
                throw new RecordTrackerException("RecordTracker must be set before executing undo.");
            }

			StandardTableUtil.writeColumnData(record, columnIndex, oldValue);

            recordTracker.getNewPosition(recordPos).ifPresentOrElse( newPosition -> tsManager.writeBlock(newPosition, record),
                    () -> tsManager.writeBlock(recordPos, record)
            );
		}
	}
	
	private final class UndoDelete implements RecordTrackableUndo {
        private final Object key;
        private final long recordPos;
		private final byte[] record;
        private RecordTracker<Long> recordTracker;

		public UndoDelete(Object key, long recordPos, byte[] record) {
            this.key = key;
            this.recordPos = recordPos;
			this.record = record;
		}

        @Override
        public void setRecordPosTracker(RecordTracker<Long> recordTracker) {
            this.recordTracker = recordTracker;
        }

        @Override
		public void execute() {
            if (recordTracker == null) {
                throw new RecordTrackerException("RecordTracker must be set before executing undo.");
            }

			long newRecordPos = insertRecord(key, record);
            recordTracker.setNewPosition(recordPos, newRecordPos);
		}
	}

	private final class StandardTableCursor implements Cursor {
		
		private final Iterator<BTreeRecordInfo<Object>> tableIndexIterator = clusteredIndex.iterator();
		
		private final List<ColumnMeta> selectColumn;

        private final ColumnMeta keyColumn;

        private Object key;

        private long recordPos;

		private byte[] record;

		public StandardTableCursor(List<ColumnMeta> selectColumn) {
			this.selectColumn = selectColumn;
            this.keyColumn = selectColumn.stream().filter(ColumnMeta::isKey).findFirst().orElse(null);
		}

		@Override
		public String getTableName() {
			return StandardTable.this.getTableName();
		}

		@Override
		public boolean next() {
			if (tableIndexIterator.hasNext()) {
                var recordInfo = tableIndexIterator.next();
                this.key = recordInfo.key();
				this.recordPos = recordInfo.recordPos();
                //System.out.printf("key = %s, recordPos = %d\n", recordInfo.key(), recordInfo.recordPos());
				this.record = tsManager.readBlock(recordPos);
				return true;
			}
			return false;
		}
		
		@Override
		public int getColumnCount() {
			return selectColumn.size();
		}
		
		@Override
		public ColumnMeta getColumnType(int columnIdx) {
			return selectColumn.get(columnIdx);
		}
		
		@Override
		public ColumnMeta getColumnType(String columnName) {
			if (!isValidColumnName(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
			return getColumnList().get(getColumnIndex(columnName));
		}
		
		@Override
		public String getColumn(int columnIdx) {
			if (!isValidColumnIndex(columnIdx)) {
				throw new IllegalArgumentException(columnIdx + " is not a valid column index.");
			}
			
			String columnName = selectColumn.get(columnIdx).getName();
			return StandardTableUtil.readColumnData(record, getColumnIndex(columnName));
		}

		@Override
		public String getColumn(String columnName) {
			if (!isValidColumnName(columnName)) {
				throw new IllegalArgumentException(columnName + " is not a valid column name");
			}
			return StandardTableUtil.readColumnData(record, getColumnIndex(columnName));
		}
		
		private boolean isValidColumnName(String columnName) {
			return selectColumn.stream().anyMatch(column -> column.getName().equalsIgnoreCase(columnName));
		}
		
		private boolean isValidColumnIndex(int columnIdx) {
            return columnIdx >= 0 && columnIdx < selectColumn.size();
        }
		
		@Override
		public byte[] getRawRecord() {
			return Arrays.copyOf(record, record.length);
		}

		@Override
		public Iterator<String> getRecord() {
			return new StandardRecordIterator(record, mapToColumnRealIndex());
		}

		private List<Integer> mapToColumnRealIndex() {
			return selectColumn.stream()
							   .map(column -> getColumnIndex(column.getName()))
							   .collect(Collectors.toList());
		}

		@Override
		public String update(String columnName, String newValue) {
			int columnIndex = getColumnIndex(columnName);
			
			String oldValue = StandardTableUtil.readColumnData(record, columnIndex);
			if (oldValue != null && oldValue.equals(newValue)) {
				return oldValue;
			}
			
			byte[] newRecord = StandardTableUtil.writeColumnData(record, columnIndex, newValue);
			if (newRecord.length != record.length) {
                deleteRecord(this.key, this.recordPos);
				addToTransactionStack(new UndoDelete(key, this.recordPos, record));
				this.recordPos = insertRecord(this.key, newRecord);
				this.record = newRecord;
				addToTransactionStack(new UndoInsert(key, recordPos));
			} else {
				updateRecord(this.recordPos, newRecord);
			}
			
			addToTransactionStack(new UndoUpdate(recordPos, record, columnIndex, oldValue));
			return oldValue;
		}
		
		@Override
		public void delete() {
            deleteRecord(this.key, this.recordPos);
            addToTransactionStack(new UndoDelete(key, this.recordPos, record));
		}
    }
}
