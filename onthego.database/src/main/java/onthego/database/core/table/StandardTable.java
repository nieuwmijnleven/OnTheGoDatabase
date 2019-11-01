package onthego.database.core.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import onthego.database.core.index.BTreeIndex;
import onthego.database.core.table.meta.Column;
import onthego.database.core.tablespace.manager.SingleTablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManager;
import onthego.database.core.tablespace.meta.SingleTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public class StandardTable {
	
	public static final String TABLESPACE_DIRECTORY_PATH = "./";
	
	private String tableName;
	
	private TablespaceManager tsManager;
	
	private BTreeIndex<Long> clusteredIndex;
	
	private Map<Column, BTreeIndex<String>> columnIndexMap;
	
	private Stack<List<Undo>> transactionStack;
	
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
		
		private long recordPos;
		
		private byte[] record;
		
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
		public String getColumn(String columnName) {
			return StandardTableUtil.readColumnData(record, getColumnIndex(columnName));
		}
		
		@Override
		public byte[] getRawRecord() {
			return Arrays.copyOf(record, record.length);
		}

		@Override
		public Iterator<String> getRecord() {
			return new StandardRecordInterator(record, getColumnList().size());
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
				this.recordPos = insertRecord(newRecord);
				this.record = newRecord;
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
	
	public static StandardTable create(String path, String tableName, TableMetaInfo tableMetaInfo) throws IOException {
		return new StandardTable(path, tableName, tableMetaInfo);
	}
	
	public static StandardTable load(String path, String tableName) throws IOException {
		return new StandardTable(path, tableName);
	}
	
	private void addToTransactionStack(Undo undo) {
		if (!transactionStack.isEmpty()) {
			List<Undo> transactionList = transactionStack.peek();
			transactionList.add(0, undo);
		}
	}
	
	public void begin() {
		transactionStack.push(new LinkedList<Undo>());
	}
	
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
	
	public void commit(boolean all) {
		if (transactionStack.isEmpty()) {
			throw new IllegalStateException("There is no BEGIN for ROLLBACK");
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
		clusteredIndex.insert(recordPos, recordPos);
		return recordPos;
	}

	private void deleteRecord(long recordPos) {
		tsManager.free(recordPos);
		clusteredIndex.delete(recordPos);
	}
	
	public long insert(Map<Column,String> values) {
		int recordSize = Short.BYTES * (1 + values.size());
		for (String value : values.values()) {
			recordSize += Short.BYTES + StandardTableUtil.getUTFSize(value);
		}
		
		ByteBuffer byteBuffer = ByteBuffer.allocate(recordSize);
		StandardTableUtil.writeUnsignedShort(byteBuffer, getColumnCount());
		
		int offset = Short.BYTES * (1 + getColumnCount());
		for (Column column : getColumnList()) {
			StandardTableUtil.writeUnsignedShort(byteBuffer, offset);
			
			byteBuffer.mark();
			byteBuffer.position(offset);
			StandardTableUtil.writeUTF(byteBuffer, values.get(column));
			byteBuffer.reset();
			
			offset += Short.BYTES + StandardTableUtil.getUTFSize(values.get(column));
		}
		
		long recordPos = insertRecord(byteBuffer.array());
		addToTransactionStack(new UndoInsert(recordPos));
		return recordPos;
	}
	
	public Cursor getCursor() {
		return new StandardTableCursor();
	}
	
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

	public String getTableName() {
		return this.tableName;
	}
	
	public TablespaceManager getTablespaceManager() {
		return this.tsManager;
	}
	
	public List<Column> getColumnList() {
		return tsManager.getHeader().getTableMetaInfo().getColumnList();
	}

	public int getColumnCount() {
		return tsManager.getHeader().getTableMetaInfo().getColumnCount();
	}
	
	private int getColumnIndex(String name) {
		return tsManager.getHeader().getTableMetaInfo().getColumnIndex(name);
	}
}
