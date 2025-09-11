package onthego.database.core.sqlprocessor;

import onthego.database.core.sqlprocessor.expression.ArithmeticExpression;
import onthego.database.core.sqlprocessor.expression.AtomicExpression;
import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.expression.Expression.ArithmeticOperator;
import onthego.database.core.sqlprocessor.expression.Expression.RelationalOperator;
import onthego.database.core.sqlprocessor.expression.LikeExpression;
import onthego.database.core.sqlprocessor.expression.LogicalExpression;
import onthego.database.core.sqlprocessor.expression.NOTExpression;
import onthego.database.core.sqlprocessor.expression.RelationalExpression;
import onthego.database.core.sqlprocessor.scanner.TokenManager;
import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.IdValue;
import onthego.database.core.sqlprocessor.value.NullValue;
import onthego.database.core.sqlprocessor.value.NumberValue;
import onthego.database.core.sqlprocessor.value.StringValue;
import onthego.database.core.sqlprocessor.value.Value;

public class SQLParser {
	
	private final SQLScanner scanner;
	
	public SQLParser(SQLScanner scanner) {
		this.scanner = scanner;
	}
	
	public SQLParser(String whereCond) {
		this.scanner = new SQLScanner(whereCond);
	}
	
	public Expression parse() throws SQLParserException {
		try {
			return doParse();
		} catch (SQLScannerException e) {
			throw new SQLParserException(e.getMessage());
		} 
	}
	
	private Expression doParse() throws SQLScannerException {
		Expression lhs = parseAND();
		while (scanner.match(TokenManager.getToken("OR"))) {
			scanner.next();
			lhs = new LogicalExpression(Expression.OR, lhs, parseAND());
		}
		return lhs;
	}

	private Expression parseAND() throws SQLScannerException {
		Expression lhs = parseREL();
		while (scanner.match(TokenManager.getToken("AND"))) {
			scanner.next();
			lhs = new LogicalExpression(Expression.AND, lhs, parseREL());
		}
		return lhs;
	}

	private Expression parseREL() throws SQLScannerException {
		Expression lhs = parseADDSUB();
		
		while (true) {
			if (scanner.match(TokenManager.getToken("RELOP"))) {
				String lexeme = scanner.getCurrentLexeme();
				scanner.next();
				
				RelationalOperator op;
				if (lexeme.length() == 1) {
					op = lexeme.charAt(0) == '<' ? Expression.LT : Expression.GT;
				} else {
					if (lexeme.charAt(0) == '<' && lexeme.charAt(1) == '>') {
						op = Expression.NE;
					} else {
						op = lexeme.charAt(0) == '<' ? Expression.LTE : Expression.GTE;
					}
				}
				lhs = new RelationalExpression(op, lhs, parseADDSUB());
			} else if (scanner.match(TokenManager.getToken("EQUAL"))) {
				scanner.next();
				lhs = new RelationalExpression(Expression.EQ, lhs, parseADDSUB());
			} else if (scanner.match(TokenManager.getToken("LIKE"))) {
				scanner.next();
				lhs = new LikeExpression(lhs, parseADDSUB());
			} else {
				break;
			}
		}
		
		return lhs;
	}
		
	private Expression parseADDSUB() throws SQLScannerException {
		Expression lhs = parseMULDIV();
		while (scanner.match(TokenManager.getToken("ADDITIVE"))) {
			String lexeme = scanner.getCurrentLexeme();
			scanner.next();
			
			ArithmeticOperator op = lexeme.charAt(0) == '+' ? Expression.ADD : Expression.SUB;
			lhs = new ArithmeticExpression(op, lhs, parseMULDIV());
		}
		return lhs;
	}

	private Expression parseMULDIV() throws SQLScannerException {
		Expression lhs = parseTERM();
		while (scanner.match(TokenManager.getToken("STAR")) 
			|| scanner.match(TokenManager.getToken("SLASH"))) {
			String lexeme = scanner.getCurrentLexeme();
			scanner.next();
			
			ArithmeticOperator op = lexeme.charAt(0) == '*' ? Expression.MUL : Expression.DIV;
			lhs = new ArithmeticExpression(op, lhs, parseTERM());
		}
		return lhs;
	}

	private Expression parseTERM() throws SQLScannerException {
		if (scanner.match(TokenManager.getToken("NOT"))) {
			scanner.next();
			
			return new NOTExpression(doParse());
		}
		
		if (scanner.match(TokenManager.getToken("LP"))) {
			scanner.next();
			
			Expression expression = doParse();
			scanner.next(TokenManager.getToken("RP"));
			return expression;
		}
		
		return parseFactor();
	}

	private Expression parseFactor() throws SQLScannerException {
		Value value;
		
		if (scanner.match(TokenManager.getToken("STRING"))) {
			value = new StringValue(scanner.getCurrentLexeme());
			scanner.next();
		} else if (scanner.match(TokenManager.getToken("NUMBER"))) {
			value = new NumberValue(scanner.getCurrentLexeme());
			scanner.next();
		} else if (scanner.match(TokenManager.getToken("NULL"))) {
			value = new NullValue();
			scanner.next();
		} else if (scanner.match(TokenManager.getToken("BOOLEAN"))) {
			value = new BooleanValue(scanner.getCurrentLexeme());
			scanner.next();
		} else {
			String tableName = null;
			String columnName = scanner.getCurrentLexeme();
			scanner.next(TokenManager.getToken("IDENTIFIER"));
			
			if (scanner.match(TokenManager.getToken("DOT"))) {
				scanner.next();
				tableName = columnName;
				columnName = scanner.getCurrentLexeme();
				scanner.next(TokenManager.getToken("IDENTIFIER"));
			}
			
			value = new IdValue(tableName, columnName);
		}
		
		return new AtomicExpression(value);
	}
}
