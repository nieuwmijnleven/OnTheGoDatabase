package onthego.database.core.sqlprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.scanner.TokenManager;
import onthego.database.core.table.meta.CharType;
import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.table.meta.IntegerType;
import onthego.database.core.table.meta.NullType;
import onthego.database.core.table.meta.NumericType;
import onthego.database.core.table.meta.VarcharType;

/*
 * SQLProcessor - yield columns, tables, and expression(where condition)
 */

public class SQLProcessor {
	
	static {
		TokenManager.createManagedToken("COMMA", "','");
		TokenManager.createManagedToken("EQUAL", "'='");
		TokenManager.createManagedToken("LP", "'('");
		TokenManager.createManagedToken("RP", "')'");
		TokenManager.createManagedToken("DOT", "'.'");
		TokenManager.createManagedToken("STAR", "'*'");
		TokenManager.createManagedToken("SLASH", "'/'");
		
		TokenManager.createManagedToken("BEGIN", "'BEGIN'");
		TokenManager.createManagedToken("COMMIT", "'COMMIT'");
		TokenManager.createManagedToken("ROLLBACK", "'ROLLBACK'");
		
		TokenManager.createManagedToken("CREATE", "'CREATE'");
		TokenManager.createManagedToken("DATABASE", "'DATABASE'");
		TokenManager.createManagedToken("DROP", "'DROP'");
		TokenManager.createManagedToken("TABLE", "'TABLE'");
		
		TokenManager.createManagedToken("SELECT", "'SELECT'");
		TokenManager.createManagedToken("INSERT", "'INSERT'");
		TokenManager.createManagedToken("UPDATE", "'UPDATE'");
		TokenManager.createManagedToken("DELETE", "'DELETE'");
		TokenManager.createManagedToken("FROM", "'FROM'");
		TokenManager.createManagedToken("INTO", "'INTO'");
		TokenManager.createManagedToken("SET", "'SET'");
		
		TokenManager.createManagedToken("USE", "'USE'");
		TokenManager.createManagedToken("VALUES", "'VALUES'");
		TokenManager.createManagedToken("WHERE", "'WHERE'");
		TokenManager.createManagedToken("TRANSACTION", "'TRANSACTION'");
		
		TokenManager.createManagedToken("PRIMARY", "'PRIMARY'");
		TokenManager.createManagedToken("KEY", "'KEY'");
		
		TokenManager.createManagedToken("INTEGER", "'INTEGER'");
		TokenManager.createManagedToken("NUMERIC", "'NUMERIC'");
		TokenManager.createManagedToken("CHAR", "'CHAR'");
		TokenManager.createManagedToken("VARCHAR", "'VARCHAR'");
		TokenManager.createManagedToken("NULL", "'NULL'");
		
		TokenManager.createManagedToken("OR", "'OR'");
		TokenManager.createManagedToken("AND", "'AND'");
		TokenManager.createManagedToken("LIKE", "'LIKE'");
		TokenManager.createManagedToken("NOT", "'NOT'");
		
		TokenManager.createManagedToken("ADDITIVE", "\\+|-");
		TokenManager.createManagedToken("STRING", "(\".*?\")|('.*?')");
		TokenManager.createManagedToken("RELOP", "[<>][=>]?");
		
		TokenManager.createManagedToken("NUMBER", "[0-9]+(\\.[0-9]+)?");
		TokenManager.createManagedToken("BOOLEAN", "(true|false)");
		
		TokenManager.createManagedToken("IDENTIFIER", "[a-z_][a-z_0-9]*");
	}
	 
	private String query;
	
	private SQLScanner scanner;
	
	public SQLProcessor(String query) {
		this.query = query;
		this.scanner = new SQLScanner(query);
	}
	
	public SQLResult process() throws SQLProcessorException {
		try {
			scanner.next();
			return doProcess();
		} catch (SQLProcessorException e) {
			throw new SQLProcessorException(e);
		}
	}

	private SQLResult doProcess() throws SQLProcessorException {
		if (scanner.match(TokenManager.getToken("CREATE"))) {
			scanner.next();
			if (scanner.match(TokenManager.getToken("DATABASE"))) {
				scanner.next();
				if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
					throw new SQLProcessorException("A database name is required.");
				}
				String database = scanner.getCurrentLexeme();
				scanner.next();
				
//				return new SQLResult(SQLResult.CommandType.CREATE_DATABASE, scanner.getCurrentLexeme());
				return SQLResult.builder()
								.command(SQLResult.CommandType.CREATE_DATABASE)
								.database(database).build();
										
			} else {
				scanner.next(TokenManager.getToken("TABLE"));
				if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
					throw new SQLProcessorException("A table name is required.");
				}
				String table = scanner.getCurrentLexeme();
				scanner.next();
				
				scanner.next(TokenManager.getToken("LP"));
				List<ColumnType> columns = columnTypeList(); 
				scanner.next(TokenManager.getToken("RP"));
				
//				return new SQLResult(SQLResult.CommandType.CREATE_TABLE, table, columns);
				return SQLResult.builder()
								.command(SQLResult.CommandType.CREATE_TABLE)
								.table(table)
								.columns(columns).build();
			}
		} else if (scanner.match(TokenManager.getToken("DROP"))) {
			scanner.next();
			
			scanner.next(TokenManager.getToken("TABLE"));
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A table name is required.");
			} 
			String table = scanner.getCurrentLexeme();
			scanner.next();
			
//			return new SQLResult(SQLResult.CommandType.DROP_TABLE, table);
			return SQLResult.builder()
							.command(SQLResult.CommandType.DROP_TABLE)
							.table(table).build();
		} else if (scanner.match(TokenManager.getToken("USE"))) {
			scanner.next();
			
			scanner.next(TokenManager.getToken("DATABASE"));
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A database name is required.");
			}
			String database = scanner.getCurrentLexeme();
			scanner.next();
			
//			return new SQLResult(SQLResult.CommandType.USE_DATABASE, database);
			return SQLResult.builder()
							.command(SQLResult.CommandType.USE_DATABASE)
							.database(database).build();
		} else if (scanner.match(TokenManager.getToken("BEGIN"))) {
			scanner.next();
			if (scanner.match(TokenManager.getToken("TRANSACTION"))) {
				scanner.next();
			}
//			return new SQLResult(SQLResult.CommandType.BEGIN_TRANSACTION);
			return SQLResult.builder()
							.command(SQLResult.CommandType.BEGIN_TRANSACTION).build();
		} else if (scanner.match(TokenManager.getToken("COMMIT"))) {
			scanner.next();
//			return new SQLResult(SQLResult.CommandType.COMMIT);
			return SQLResult.builder()
							.command(SQLResult.CommandType.COMMIT).build();
		} else if (scanner.match(TokenManager.getToken("ROLLBACK"))) {
			scanner.next();
//			return new SQLResult(SQLResult.CommandType.ROLLBACK);
			return SQLResult.builder()
							.command(SQLResult.CommandType.ROLLBACK).build();
		} else if (scanner.match(TokenManager.getToken("SELECT"))) {
			scanner.next();
			
			List<ColumnType> columns = columnList();
			
			scanner.next(TokenManager.getToken("FROM"));
			String table = scanner.getCurrentLexeme();
			scanner.next();
			
			Expression where = Expression.TRUE_EXPRESSION;
			if (scanner.match(TokenManager.getToken("WHERE"))) {
				scanner.next();
				where = new SQLParser(scanner).parse();
			}
			
//			return new SQLResult(SQLResult.CommandType.SELECT, table, columns, Collections.emptyList(), expressions);
			return SQLResult.builder()
							.command(SQLResult.CommandType.SELECT)
							.table(table)
							.columns(columns)
							.where(where)
							.build();
		} else if (scanner.match(TokenManager.getToken("INSERT"))) {
			scanner.next();
			
			scanner.next(TokenManager.getToken("INTO"));
			String table = scanner.getCurrentLexeme();
			scanner.next();
			
			List<ColumnType> columns = new ArrayList<>();
			if (scanner.match(TokenManager.getToken("LP"))) {
				scanner.next();
				columns = columnList(); 
				scanner.next(TokenManager.getToken("RP"));
			}
			
			scanner.next(TokenManager.getToken("VALUES"));
			scanner.next(TokenManager.getToken("LP"));
			List<Expression> values = valueList();
			scanner.next(TokenManager.getToken("RP"));
			
//			return new SQLResult(SQLResult.CommandType.INSERT, table, columns, values);
			return SQLResult.builder()
							.command(SQLResult.CommandType.INSERT)
							.table(table)
							.columns(columns)
							.values(values)
							.build();
		} else if (scanner.match(TokenManager.getToken("UPDATE"))) {
			scanner.next();
			
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A table name is required.");
			}
			String table = scanner.getCurrentLexeme();
			scanner.next();			
			
			scanner.next(TokenManager.getToken("SET"));
			
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A column name is required.");
			}
			ColumnType column = new ColumnType(scanner.getCurrentLexeme(), new NullType());
			scanner.next();	
			
			scanner.next(TokenManager.getToken("EQUAL"));
			
			List<Expression> values = valueList();
			
			Expression where = Expression.TRUE_EXPRESSION;
			if (scanner.match(TokenManager.getToken("WHERE"))) {
				scanner.next();
				
				where = new SQLParser(scanner).parse();
			}
			
//			return new SQLResult(SQLResult.CommandType.UPDATE, table, Collections.singletonList(column), value, expressions);
			return SQLResult.builder()
							.command(SQLResult.CommandType.UPDATE)
							.table(table)
							.columns(Collections.singletonList(column))
							.values(values)
							.where(where)
							.build();
		} else if (scanner.match(TokenManager.getToken("DELETE"))) {
			scanner.next();
			
			scanner.next(TokenManager.getToken("FROM"));
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A table name is required.");
			}
			String table = scanner.getCurrentLexeme();
			scanner.next();			
			
			Expression where = Expression.TRUE_EXPRESSION;
			if (scanner.match(TokenManager.getToken("WHERE"))) {
				scanner.next();
				
				where = new SQLParser(scanner).parse();
			}
			
//			return new SQLResult(SQLResult.CommandType.DELETE, table, Collections.emptyList(), Collections.emptyList(), expressions);
			return SQLResult.builder()
							.command(SQLResult.CommandType.DELETE)
							.table(table)
							.where(where)
							.build();
		}  
		
		throw new SQLProcessorException("An unrecognizable query => " + this.query);
	}
	
	private Expression parse() throws SQLParserException {
		return new SQLParser(scanner).parse();
	}

	private List<Expression> valueList() throws SQLProcessorException {
		List<Expression> expressions = new ArrayList<>();
		
		expressions.add(parse());
		while (scanner.match(TokenManager.getToken("COMMA"))) {
			scanner.next();
			expressions.add(parse());
		}
		
		return expressions;
	}

	private List<ColumnType> columnList() throws SQLScannerException {
		if (scanner.match(TokenManager.getToken("STAR"))) {
			scanner.next();
			return Collections.emptyList();
		}
		
		List<ColumnType> columns = new ArrayList<>();
		while (scanner.match(TokenManager.getToken("IDENTIFIER"))) {
			columns.add(new ColumnType(scanner.getCurrentLexeme(), new NullType()));
			scanner.next();
			
			if (!scanner.match(TokenManager.getToken("COMMA"))) {
				break;
			}
			scanner.next();
		}
		return columns;
	}

	private List<ColumnType> columnTypeList() throws SQLProcessorException {
		List<ColumnType> columnTypeList = new ArrayList<>();
		
		while (true) {
			if (!scanner.match(TokenManager.getToken("IDENTIFIER"))) {
				throw new SQLProcessorException("A column name is required.");
			}
			ColumnType columnType = new ColumnType(scanner.getCurrentLexeme());
			scanner.next();
			
			if (scanner.match(TokenManager.getToken("CHAR"))) {
				scanner.next();
				
				scanner.next(TokenManager.getToken("LP"));
				if (!scanner.match(TokenManager.getToken("NUMBER"))) {
					throw new SQLProcessorException("A number is required.");
				}
				columnType.setType(new CharType(Integer.parseInt(scanner.getCurrentLexeme())));
				scanner.next();
				
				scanner.next(TokenManager.getToken("RP"));
			} else if (scanner.match(TokenManager.getToken("VARCHAR"))) {
				scanner.next();
				
				scanner.next(TokenManager.getToken("LP"));
				if (!scanner.match(TokenManager.getToken("NUMBER"))) {
					throw new SQLProcessorException("A number is required.");
				}
				columnType.setType(new VarcharType(Integer.parseInt(scanner.getCurrentLexeme())));
				scanner.next();
				
				scanner.next(TokenManager.getToken("RP"));
			} else if (scanner.match(TokenManager.getToken("INTEGER"))) {
				scanner.next();
				
				scanner.next(TokenManager.getToken("LP"));
				if (!scanner.match(TokenManager.getToken("NUMBER"))) {
					throw new SQLProcessorException("A number is required.");
				}
				columnType.setType(new IntegerType(Integer.parseInt(scanner.getCurrentLexeme())));
				scanner.next();
				
				scanner.next(TokenManager.getToken("RP"));
			} else if (scanner.match(TokenManager.getToken("NUMERIC"))) {
				scanner.next();
				
				scanner.next(TokenManager.getToken("LP"));
				if (!scanner.match(TokenManager.getToken("NUMBER"))) {
					throw new SQLProcessorException("A number is required.");
				}
				int integer = Integer.parseInt(scanner.getCurrentLexeme());
				scanner.next();
				
				scanner.next(TokenManager.getToken("COMMA"));
				
				if (!scanner.match(TokenManager.getToken("NUMBER"))) {
					throw new SQLProcessorException("A number is required.");
				}
				int decimal = Integer.parseInt(scanner.getCurrentLexeme());
				scanner.next();
				
				columnType.setType(new NumericType(integer, decimal));
				scanner.next(TokenManager.getToken("RP"));
			} else {
				throw new SQLProcessorException(scanner.getCurrentLexeme() + " is not a valid type.");
			}
			
			columnTypeList.add(columnType);
			
			if (!scanner.match(TokenManager.getToken("COMMA"))) {
				break;
			}
			scanner.next();
		}
		
		return columnTypeList;
	}
}
