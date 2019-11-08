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
import onthego.database.core.table.Cursor;
import onthego.database.core.table.Filtration;
import onthego.database.core.table.Filtration.DefaultFilter;
import onthego.database.core.table.StandardTable;
import onthego.database.core.table.Table;
import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.tablespace.meta.TableMetaInfo;

public final class Database {

	private Path location = Paths.get(".");
	
	private int transactionLevel = 0;
	
	private int affectedRowCount = 0;
	
	private Map<String,Table> tables = new HashMap<>();
	
	public Database(String path) throws DatabaseException {
		this(Paths.get(path));
	}
	
	public Database(Path path) throws DatabaseException {
		useDatabase(path);
	}

	private void useDatabase(Path path) throws DatabaseException {
		tables.clear();
		
		this.location = path;
		try {
			loadTables();
		} catch (Exception e) {
			throw new DatabaseException(e);
		}
	}
	
	private void loadTables() throws IOException {
		Files.list(this.location)
		.filter(Files::isRegularFile)
		.map(filePath -> filePath.getName(filePath.getNameCount() - 1).toString())
		.filter(fileName -> fileName.substring(fileName.lastIndexOf(".") + 1).equals("db"))
		.map(fileName -> fileName.substring(fileName.lastIndexOf(".") + 1))
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
	
	public void createTable(String tableName, List<ColumnType> columns) throws DatabaseException {
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
	
	public Table select(String tableName, List<ColumnType> columns, Expression where) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		Table resultTable = table.select(new DefaultFilter() {
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
	
	public int insert(String tableName, final List<ColumnType> columns, List<Expression> values) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		
		List<ColumnType> tableColumns = new ArrayList<>();
		if (columns.size() == 0 && values.size() == table.getColumnCount()) {
			tableColumns = table.getColumnList();
		} else if (columns.size() != values.size()) {
			throw new DatabaseException("the number of columns is not consistent with that of values");
		} else {
			tableColumns = table.getColumnList().stream()
			.filter(tableColumn -> columns.stream()
					.anyMatch(column -> tableColumn.getName().equals(column.getName())))
			.collect(Collectors.toList());
		}
		
		try {
			Map<ColumnType,String> newRecord = new HashMap<>();
			for (int i = 0; i < tableColumns.size(); ++i) {
				newRecord.put(tableColumns.get(i), values.get(i).evaluate(cursors()).toString());
			}
			table.insert(newRecord);
		} catch (ExpressionEvaluationException e) {
			throw new DatabaseException(e);
		}
		
		return (affectedRowCount = 1);
	}
	
	public int update(String tableName, List<ColumnType> columns, List<Expression> values, Expression where) throws DatabaseException {
		if (!tables.containsKey(tableName)) {
			throw new DatabaseException(tableName + " table is not in the database.");
		}
		
		Table table = tables.get(tableName);
		
		if (columns.size() != values.size()) {
			throw new DatabaseException("The number of columns is not consistent with that of values");
		}
		
		affectedRowCount = table.update(new Filtration.DefaultFilter() {
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
					cursor.update(columns.get(0).getName(), values.get(0).evaluate(new Cursor[]{cursor}).toString());
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
	
	public Table execute(String query) throws DatabaseException {
		SQLProcessor processor = new SQLProcessor(query);
		SQLResult result;
		try {
			result = processor.process();
		} catch (SQLProcessorException e) {
			throw new DatabaseException(e);
		}
		
		switch(result.getCommand()) {
		case CREATE_DATABASE:
			createDatabase(result.getDatabase());
			break;
		case CREATE_TABLE:
			createTable(result.getTable(), result.getColumns());
			break;
		case DROP_TABLE:
			dropTable(result.getTable());
			break;
		case USE_DATABASE:
			useDatabase(Paths.get(result.getDatabase()));
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
			return select(result.getTable(), result.getColumns(), result.getWhere());
		case INSERT:
			affectedRowCount = insert(result.getTable(), result.getColumns(), result.getValues());
			break;
		case UPDATE:
			affectedRowCount = update(result.getTable(), result.getColumns(), result.getValues(), result.getWhere());
			break;
		case DELETE:
			affectedRowCount = delete(result.getTable(), result.getWhere());
			break;
		default:
			throw new DatabaseException("An unsupported Opeartion.");
		}
		
		return null;
	}
}
