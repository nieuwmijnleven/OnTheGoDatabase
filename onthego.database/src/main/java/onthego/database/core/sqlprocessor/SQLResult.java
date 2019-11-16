package onthego.database.core.sqlprocessor;

import java.util.Collections;
import java.util.List;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.table.meta.ColumnMeta;

public class SQLResult {
	
	public enum CommandType { CREATE_DATABASE, CREATE_TABLE, DROP_TABLE, USE_DATABASE, BEGIN_TRANSACTION, COMMIT, ROLLBACK, SELECT, INSERT, UPDATE, DELETE };
	
	private CommandType command;
	
	private String database;
	
	private String table;
	
	private List<ColumnMeta> columns;
	
	private List<Expression> values;
	
	private Expression where;
	
	public static class SQLResultBuilder {
		
		private CommandType command;
		
		private String database;
		
		private String table;
		
		private List<ColumnMeta> columns = Collections.emptyList();
		
		private List<Expression> values = Collections.emptyList();
		
		private Expression where = Expression.NULL_EXPRESSION;
		
		public SQLResultBuilder() {}

		public SQLResultBuilder command(CommandType command) {
			this.command = command;
			return this;
		}

		public SQLResultBuilder database(String database) {
			this.database = database;
			return this;
		}

		public SQLResultBuilder table(String table) {
			this.table = table;
			return this;
		}

		public SQLResultBuilder columns(List<ColumnMeta> columns) {
			this.columns = columns;
			return this;
		}

		public SQLResultBuilder values(List<Expression> values) {
			this.values = values;
			return this;
		}

		public SQLResultBuilder where(Expression where) {
			this.where = where;
			return this;
		}
		
		public SQLResult build() {
			return new SQLResult(this);
		}
	}
	
	public static SQLResultBuilder builder() {
		return new SQLResultBuilder();
	}
	
	private SQLResult(SQLResultBuilder builder) {
		this.command = builder.command;
		this.database = builder.database;
		this.table = builder.table;
		this.columns = builder.columns;
		this.values = builder.values;
		this.where = builder.where;
	}

	public CommandType getCommand() {
		return command;
	}
	
	public String getDatabase() {
		return database;
	}
	
	public String getTable() {
		return table;
	}

	public List<ColumnMeta> getColumns() {
		return columns;
	}
	
	public List<Expression> getValues() {
		return values;
	}

	public Expression getWhere() {
		return where;
	}	
}
