package onthego.database.core.database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import onthego.database.core.sqlprocessor.SQLProcessor;
import onthego.database.core.sqlprocessor.SQLProcessorException;
import onthego.database.core.sqlprocessor.SQLResult;
import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.expression.ExpressionEvaluationException;
import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;
import onthego.database.core.table.Filtration;
import onthego.database.core.table.Filtration.DefaultFilter;
import onthego.database.core.table.StandardTable;
import onthego.database.core.table.Table;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.tablespace.meta.TableMetaInfo;

public final class Database {

	private Path location = Paths.get(".");
	
	private int transactionLevel = 0;
	
	private int affectedRowCount = 0;
	
	private Map<String,Table> tables = new HashMap<>();
	
	public Database(String path) throws DatabaseException {
		open(Paths.get(path));
	}
	
	private void open(Path path) throws DatabaseException {
		tables.clear();
		
		this.location = path;
		try {
			loadTables();
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
	}
	
	public void close() {
		for (Table table : tables.values()) {
			table.close();
		}
		
		tables.clear();
	}

	
	private void loadTables() throws IOException {
		Files.list(this.location)
		.filter(Files::isRegularFile)
		.map(filePath -> filePath.getName(filePath.getNameCount() - 1).toString())
		.filter(fileName -> fileName.substring(fileName.lastIndexOf(".") + 1).equals("db"))
		.map(fileName -> fileName.substring(0, fileName.lastIndexOf(".")))
		.forEach(tableName -> {
			try {
				Table table = StandardTable.load(location.toString(), tableName);
				tables.put(tableName, table);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}
	
	public void createDatabase(String databaseName) throws DatabaseException {
		try {
			this.location = Files.createDirectory(Paths.get(databaseName));
		} catch (IOException e) {
			throw new DatabaseException(e);
		}
	}
	
	public void createTable(String tableName, List<ColumnMeta> columns) throws DatabaseException {
		try {
			TableMetaInfo tableMetaInfo = new TableMetaInfo(tableName, columns);
			Table table = StandardTable.create(location.toString(), tableName, tableMetaInfo);
			tables.put(tableName, table);
			
			for (int i = 0; i < transactionLevel; ++i) {
				table.begin();
			}
		} catch (IOException e) {
			throw new DatabaseException(e);
		}
	}
	
	public void dropTable(String tableName) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		/*if (hasTransaction()) {
			Table removed = tables.get(tableName);
			removed.rollback(true);
		}*/
		
		tables.remove(tableName);
		try {
			if (!Files.deleteIfExists(location.resolve(tableName + ".db"))) {
				throw new DatabaseException(tableName + " table is not in the database directory.");
			}
		} catch (IOException e) {
			throw new DatabaseException(e);
		}
	}
	
	private boolean hasTransaction() {
		return transactionLevel > 0;
	}

	public void begin() {
		for (Table table : tables.values()) {
			table.begin();
		}
		++transactionLevel;
	}
	
	public void commit() {
		if (transactionLevel <= 0) {
			throw new DatabaseException("There is no transaction.");
		}
		
		for (Table table : tables.values()) {
			table.commit(false);
		}
		--transactionLevel;
	}
	
	public void rollback() {
		if (transactionLevel <= 0) {
			throw new DatabaseException("There is no transaction.");
		}
		
		for (Table table : tables.values()) {
			table.rollback(false);
		}
		--transactionLevel;
	}
	
	public Table select(String tableName, List<ColumnMeta> columns, Expression where) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		Table resultTable = table.select(mapToRealTableColumn(columns, table), new DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				try {
					BooleanValue result = (BooleanValue)where.evaluate(cursor);
					return result.getValue();
				} catch(ExpressionEvaluationException e) {
					throw new DatabaseException(e);
				}
			}
		});
		return resultTable;
	}
	
	private Cursor[] cursors() {
		Cursor[] cursors = new Cursor[tables.size()];
		int i = 0;
		for (Table table : tables.values()) {
			cursors[i++] = table.getCursor();
		}
		return cursors;
	}
	
	public int insert(String tableName, final List<ColumnMeta> columns, List<Expression> values) {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		
		List<ColumnMeta> tableColumns = new ArrayList<>();
		if (columns.size() == 0 && values.size() == table.getColumnCount()) { //in case of selecting all columns such as "select * from table"
			tableColumns = table.getColumnList();
		} else if (columns.size() != values.size()) {
			throw new DatabaseException("The number of columns is not consistent with that of values");
		} else {
			tableColumns = mapToRealTableColumn(columns, table);
		}
		
		table.insert(createRecordDataMap(tableColumns, values, table.getCursor()));
		return (affectedRowCount = 1);
	}

	private List<ColumnMeta> mapToRealTableColumn(final List<ColumnMeta> columns, Table table) {
		List<ColumnMeta> tableColumns;
		tableColumns = table.getColumnList().stream()
					.filter(tableColumn -> columns.stream()
							.anyMatch(selectColumn -> tableColumn.getName().equalsIgnoreCase(selectColumn.getName())))
							.collect(Collectors.toList());
		return tableColumns;
	}

	private Map<ColumnMeta, String> createRecordDataMap(List<ColumnMeta> tableColumns, List<Expression> values, Cursor cursor) {
		Map<ColumnMeta,String> newRecord = new HashMap<>();
		try {
			for (int i = 0; i < tableColumns.size(); ++i) {
				newRecord.put(tableColumns.get(i), Value.evaluate(values.get(i), new Cursor[]{cursor}));
			}
		} catch(ExpressionEvaluationException e) {
			throw new DatabaseException("Fail to evaluate expressions.");
		}
		return newRecord;
	}

	public int update(String tableName, List<ColumnMeta> columns, List<Expression> values, Expression where) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		
		if (columns.size() != values.size()) {
			throw new DatabaseException("The number of columns is not consistent with that of values");
		}
		
		affectedRowCount = table.update(new DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				try {
					BooleanValue result = (BooleanValue)where.evaluate(cursor);
					return result.getValue();
				} catch(ExpressionEvaluationException e) {
					throw new DatabaseException(e);
				}
			}

			@Override
			public void update(Cursor cursor) {
				try {
//					cursor.update(columns.get(0).getName(), values.get(0).evaluate(new Cursor[]{cursor}).toString());
					cursor.update(columns.get(0).getName(), Value.evaluate(values.get(0), new Cursor[]{cursor}));
				} catch (ExpressionEvaluationException e) {
					throw new DatabaseException(e);
				}
			}
		});
		
		return affectedRowCount;
	}
	
	public int delete(String tableName, Expression where) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		affectedRowCount = table.delete(new Filtration.DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				try {
					BooleanValue result = (BooleanValue)where.evaluate(cursor);
					return result.getValue();
				} catch(ExpressionEvaluationException e) {
					throw new DatabaseException(e);
				}
			}
		});
		
		return affectedRowCount;
	} 
	
	public Table execute(String query) {
		SQLProcessor processor = new SQLProcessor(query);
		SQLResult result;
		try {
			result = processor.process();
		} catch (SQLProcessorException e) {
			throw new DatabaseException(e);
		}
		
		switch(result.getCommand()) {
		case CREATE_DATABASE:
			doCreateDatabase(result);
			break;
		case CREATE_TABLE:
			doCreateTable(result);
			break;
		case DROP_TABLE:
			doDropTable(result);
			break;
		case USE_DATABASE:
			doUseTable(result);
			break;
		case BEGIN_TRANSACTION:
			begin();
			break;
		case COMMIT:
			commit();
			break;
		case ROLLBACK:
			rollback();
			break;
		case SELECT:
			return doSelect(result);
		case INSERT:
			doInsert(result);
			break;
		case UPDATE:
			doUpdate(result);
			break;
		case DELETE:
			doDelete(result);
			break;
		default:
			throw new DatabaseException("An unsupported Opeartion.");
		}
		
		return null;
	}

	private void doDelete(SQLResult result) {
		affectedRowCount = delete(result.getTable(), result.getWhere());
	}

	private void doUpdate(SQLResult result) {
		affectedRowCount = update(result.getTable(), result.getColumns(), result.getValues(), result.getWhere());
	}

	private void doInsert(SQLResult result) {
		affectedRowCount = insert(result.getTable(), result.getColumns(), result.getValues());
	}

	private Table doSelect(SQLResult result) {
		return select(result.getTable(), result.getColumns(), result.getWhere());
	}

	private void doUseTable(SQLResult result) {
		open(Paths.get(result.getDatabase()));
	}

	private void doDropTable(SQLResult result) {
		dropTable(result.getTable());
	}

	private void doCreateTable(SQLResult result) {
		createTable(result.getTable(), result.getColumns());
	}

	private void doCreateDatabase(SQLResult result) {
		createDatabase(result.getDatabase());
	}
	
	public int getAffectedRowCount() {
		return affectedRowCount;
	}
}
