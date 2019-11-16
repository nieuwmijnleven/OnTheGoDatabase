package onthego.database.core.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import onthego.database.core.index.BTreeIndex;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.tablespace.manager.SingleTablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManager;
import onthego.database.core.tablespace.meta.SingleTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public class StandardTable implements Table {
	
	public static final String TABLESPACE_DIRECTORY_PATH = "./";
	
	private String tableName;
	
	private TablespaceManager tsManager;
	
	private BTreeIndex<Long> clusteredIndex;
	
	private Map<ColumnMeta, BTreeIndex<String>> columnIndexMap;
	
	private Stack<List<Undo>> transactionStack;
	
	// To create a standard table
	private StandardTable(String path, String tableName, TableMetaInfo tableMetaInfo) throws IOException {
		TablespaceHeader tsHeader = new SingleTablespaceHeader.Builder()
										.chunkSize(16)
										.tableMetaInfo(tableMetaInfo)
										.build();
		this.tsManager = SingleTablespaceManager.create(path + File.separator + tableName + ".db", tsHeader);
		this.clusteredIndex = new BTreeIndex<>(128, tsManager);
		this.tableName = tableName;
		this.transactionStack = new Stack<>();
	}
	
	// To load a standard table
	private StandardTable(String path, String tableName) throws IOException {
		this.tsManager = SingleTablespaceManager.load(path + File.separator + tableName + ".db");
		this.clusteredIndex = new BTreeIndex<>(128, tsManager);
		this.tableName = tableName;
		this.transactionStack = new Stack<>();
	}
	
	public static Table create(String path, String tableName, TableMetaInfo tableMetaInfo) throws IOException {
		return new StandardTable(path, tableName, tableMetaInfo);
	}
	
	public static Table load(String path, String tableName) throws IOException {
		return new StandardTable(path, tableName);
	}
	
	private void addToTransactionStack(Undo undo) {
		if (!transactionStack.isEmpty()) {
			List<Undo> transactionList = transactionStack.peek();
			transactionList.add(0, undo);
		}
	}
	
	@Override
	public void close() {
		tsManager.close();
	}
	
	@Override
	public void begin() {
		transactionStack.push(new LinkedList<Undo>());
	}
	
	@Override
	public void rollback(boolean all) {
		if (transactionStack.isEmpty()) {
			throw new IllegalStateException("There is no BEGIN for ROLLBACK");
		}
		
		do {
			List<Undo> transactionList = transactionStack.pop();
			for (Undo undo : transactionList) {
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
			List<Undo> transactionList = transactionStack.pop();
			if (!transactionStack.isEmpty()) {
				List<Undo> higherTransactionList = transactionStack.peek();
				higherTransactionList.addAll(0, transactionList);
			}
		}
	}

	private long insertRecord(byte[] payload) {
		long recordPos = tsManager.allocate(payload.length);
		tsManager.writeBlock(recordPos, payload);
		tsManager.increaseRecordCount();
		clusteredIndex.insert(recordPos, recordPos);
		return recordPos;
	}
	
	private void updateRecord(long recordPos, byte[] newRecord) {
		tsManager.writeBlock(recordPos, newRecord);
	}

	private void deleteRecord(long recordPos) {
		tsManager.free(recordPos);
		tsManager.decreaseRecordCount();
		clusteredIndex.delete(recordPos);
	}
	
	@Override
	public Table select(List<ColumnMeta> selectColumns, Filtration filtration) {
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
		if (selectColumns.size() == 0) {
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
	
	@Override
	public long insert(Map<ColumnMeta,String> values) {
		int recordSize = Short.BYTES * (1 + values.size());
		for (String value : values.values()) {
			recordSize += Short.BYTES + StandardTableUtil.getUTFSize(value);
		}
		
		ByteBuffer byteBuffer = ByteBuffer.allocate(recordSize);
		StandardTableUtil.writeUnsignedShort(byteBuffer, getColumnCount());
		
		int offset = Short.BYTES * (1 + getColumnCount());
		for (ColumnMeta column : getColumnList()) {
			StandardTableUtil.writeUnsignedShort(byteBuffer, offset);
			
			byteBuffer.mark();
			byteBuffer.position(offset);
			
			String value = (values.get(column) != null) ? values.get(column) : "";
			//System.out.println("value = " + value);
			StandardTableUtil.writeUTF(byteBuffer, value);
			
			byteBuffer.reset();
			offset += Short.BYTES + StandardTableUtil.getUTFSize(value);
		}
		
		long recordPos = insertRecord(byteBuffer.array());
		addToTransactionStack(new UndoInsert(recordPos));
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
	public int update(Filtration filtration) {
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
	public int delete(Filtration filtration) {
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
	
	private final class UndoInsert implements Undo {
		private long recordPos;
		
		public UndoInsert(long recordPos) {
			this.recordPos = recordPos;
		}

		@Override
		public void execute() {
			deleteRecord(this.recordPos);
		}
	}
	
	private final class UndoUpdate implements Undo {
		private long recordPos;
		
		private byte[] record;
		
		private int columnIndex;
		
		private String oldValue;
		
		public UndoUpdate(long recordPos, byte[] record, int columnIndex, String oldValue) {
			this.recordPos = recordPos;
			this.record = record;
			this.columnIndex = columnIndex;
			this.oldValue = oldValue;
		}

		@Override
		public void execute() {
			StandardTableUtil.writeColumnData(record, columnIndex, oldValue);
			tsManager.writeBlock(recordPos, record);
		}
	}
	
	private final class UndoDelete implements Undo {
		private byte[] record;
		
		public UndoDelete(byte[] record) {
			this.record = record;
		}

		@Override
		public void execute() {
			insertRecord(record);
		}
	}
	
	private final class StandardTableCursor implements Cursor {
		
		private final Iterator<Long> tableIndexIterator = clusteredIndex.iterator();
		
		private final List<ColumnMeta> selectColumn;
		
		private long recordPos;
		
		private byte[] record;
		
		public StandardTableCursor(List<ColumnMeta> selectColumn) {
			this.selectColumn = selectColumn;
		}

		@Override
		public String getTableName() {
			return StandardTable.this.getTableName();
		}

		@Override
		public boolean next() {
			if (tableIndexIterator.hasNext()) {
				this.recordPos = tableIndexIterator.next();
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
			if (columnIdx < 0 || columnIdx >= selectColumn.size()) {
				return false;
			}
			return true;
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
			if (oldValue == newValue) {
				return oldValue;
			}
			
			byte[] newRecord = StandardTableUtil.writeColumnData(record, columnIndex, newValue);
			if (newRecord.length != record.length) {
				deleteRecord(this.recordPos);
				addToTransactionStack(new UndoDelete(record));
				
				this.recordPos = insertRecord(newRecord);
				this.record = newRecord;
				addToTransactionStack(new UndoInsert(recordPos));
			} else {
				updateRecord(this.recordPos, newRecord);
			}
			
			addToTransactionStack(new UndoUpdate(recordPos, record, columnIndex, oldValue));
			return oldValue;
		}
		
		@Override
		public void delete() {
			deleteRecord(this.recordPos);
			addToTransactionStack(new UndoDelete(record));
		}
	}
}
