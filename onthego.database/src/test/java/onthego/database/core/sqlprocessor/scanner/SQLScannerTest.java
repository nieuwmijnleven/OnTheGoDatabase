package onthego.database.core.sqlprocessor.scanner;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import onthego.database.core.sqlprocessor.SQLScanner;
import onthego.database.core.sqlprocessor.SQLScannerException;

public class SQLScannerTest {
	
	private static String statement = "select select_column1, select_column2 "
									 + "from table "
									 + "where where_cond = 100";
	@Before
	public void setUp() throws Exception {
		TokenManager.clear();
		
		TokenManager.createManagedToken("COMMA", "','");
		TokenManager.createManagedToken("EQUAL", "'='");
		TokenManager.createManagedToken("SELECT", "select");
		TokenManager.createManagedToken("FROM", "from");
		TokenManager.createManagedToken("WHERE", "where");
		TokenManager.createManagedToken("IDENTIFIER", "[a-z_][a-z_0-9]*");
		
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("COMMA").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("EQUAL").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("SELECT").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("FROM").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("WHERE").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("IDENTIFIER").getType());
	}
	
//	@After
//	public void tearDown() throws Exception {
//		TokenManager.clear();
//	}

	@Test
	public void testNext() throws SQLScannerException {
		TokenManager.createManagedToken("NUMBER", "[0-9]+");
		assertEquals(Token.Type.REGEX, TokenManager.getToken("NUMBER").getType());
		
		SQLScanner scanner = new SQLScanner(statement);
		assertEquals("select", scanner.next().lexeme());
		assertEquals("select_column1", scanner.next().lexeme());
		assertEquals(",", scanner.next().lexeme());
		assertEquals("select_column2", scanner.next().lexeme());
		assertEquals("from", scanner.next().lexeme());
		assertEquals("table", scanner.next().lexeme());
		assertEquals("where", scanner.next().lexeme());
		assertEquals("where_cond", scanner.next().lexeme());
		assertEquals("=", scanner.next().lexeme());
		assertEquals("100", scanner.next().lexeme());
		assertEquals(Token.NULL_TOKEN, scanner.next());
	}
	
	@Test
	public void testNextFail() throws SQLScannerException {
		TokenManager.removeManagedToken("NUMBER");
		
		SQLScanner scanner = new SQLScanner(statement);
		assertEquals("select", scanner.next().lexeme());
		assertEquals("select_column1", scanner.next().lexeme());
		assertEquals(",", scanner.next().lexeme());
		assertEquals("select_column2", scanner.next().lexeme());
		assertEquals("from", scanner.next().lexeme());
		assertEquals("table", scanner.next().lexeme());
		assertEquals("where", scanner.next().lexeme());
		assertEquals("where_cond", scanner.next().lexeme());
		assertEquals("=", scanner.next().lexeme());
		
		try {
			scanner.next();
		} catch(SQLScannerException se) {
			assertEquals(se.getMessage(), "Line : 1, Postion: 68\n" + 
						"select select_column1, select_column2 from table where where_cond = 100\n" + 
						"____________________________________________________________________^\n");
		}
	}
	
	@Test
	public void testNextIf() throws SQLScannerException {
		TokenManager.createManagedToken("NUMBER", "[0-9]+");
		assertEquals(Token.Type.REGEX, TokenManager.getToken("NUMBER").getType());
		
		SQLScanner scanner = new SQLScanner(statement);
		assertEquals("select", scanner.next(Token.BEGIN_TOKEN).lexeme());
		assertEquals("select_column1", scanner.next(TokenManager.getToken("SELECT")).lexeme());
		assertEquals(",", scanner.next(TokenManager.getToken("IDENTIFIER")).lexeme());
		assertEquals("select_column2", scanner.next(TokenManager.getToken("COMMA")).lexeme());
		assertEquals("from", scanner.next(TokenManager.getToken("IDENTIFIER")).lexeme());
		
		try {
			scanner.next(TokenManager.getToken("SELECT"));
		} catch (SQLScannerException se) {
			assertEquals(se.getMessage(), "The token(select) is required.");
			assertEquals("table", scanner.next(TokenManager.getToken("FROM")).lexeme());
		}
		
		assertEquals("where", scanner.next(TokenManager.getToken("IDENTIFIER")).lexeme());
		assertEquals("where_cond", scanner.next(TokenManager.getToken("WHERE")).lexeme());
		assertEquals("=", scanner.next(TokenManager.getToken("IDENTIFIER")).lexeme());
		assertEquals("100", scanner.next(TokenManager.getToken("EQUAL")).lexeme());
		assertEquals(Token.NULL_TOKEN, scanner.next(TokenManager.getToken("NUMBER")));
	}
}
