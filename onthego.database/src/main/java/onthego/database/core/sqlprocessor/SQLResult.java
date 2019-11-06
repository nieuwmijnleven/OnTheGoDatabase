package onthego.database.core.sqlprocessor;

import java.util.Collections;
import java.util.List;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.table.meta.ColumnType;

public class SQLResult {
	
	public enum CommandType { CREATE_DATABASE, CREATE_TABLE, DROP_TABLE, USE_DATABASE, BEGIN_TRANSACTION, COMMIT, ROLLBACK, SELECT, INSERT, UPDATE, DELETE };
	
	private CommandType command;
	
	private String table;
	
	private List<ColumnType> columns;
	
	private List<Expression> values;
	
	private List<Expression> expressions;
	
	public SQLResult(CommandType command, String table, List<ColumnType> columns, List<Expression> values,
			List<Expression> expressions) {
		this.command = command;
		this.table = table;
		this.columns = columns;
		this.values = values;
		this.expressions = expressions;
	}

	public SQLResult(CommandType command, String table, List<ColumnType> columns, List<Expression> values) {
		this(command, table, columns, values, Collections.emptyList());
	}
	
	public SQLResult(CommandType command, String table, List<ColumnType> columns) {
		this(command, table, columns, Collections.emptyList());
	}
	
	public SQLResult(CommandType command, String table) {
		this(command, table, Collections.emptyList());
	}
	
	public SQLResult(CommandType command) {
		this(command, "");
	}

	public String getTable() {
		return table;
	}

	public CommandType getCommand() {
		return command;
	}

	public List<ColumnType> getColumns() {
		return columns;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public String toString() {
		return "SQLResult [table=" + table + ", command=" + command + ", columns=" + columns + ", expression="
				+ expressions + "]";
	}
}
