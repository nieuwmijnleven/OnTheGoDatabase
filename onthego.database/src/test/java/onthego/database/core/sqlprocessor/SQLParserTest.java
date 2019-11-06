package onthego.database.core.sqlprocessor;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.expression.ExpressionEvaluationException;
import onthego.database.core.sqlprocessor.scanner.Token;
import onthego.database.core.sqlprocessor.scanner.TokenManager;
import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;

public class SQLParserTest {

	@Before
	public void setUp() throws Exception {
		TokenManager.createManagedToken("COMMA", "','");
		TokenManager.createManagedToken("EQUAL", "'='");
		TokenManager.createManagedToken("LP", "'('");
		TokenManager.createManagedToken("RP", "')'");
		TokenManager.createManagedToken("DOT", "'.'");
		TokenManager.createManagedToken("STAR", "'*'");
		TokenManager.createManagedToken("SLASH", "'/'");
		TokenManager.createManagedToken("AND", "AND");
		TokenManager.createManagedToken("LIKE", "LIKE");
		TokenManager.createManagedToken("NOT", "NOT");
		TokenManager.createManagedToken("NULL", "NULL");
		TokenManager.createManagedToken("OR", "OR");
		TokenManager.createManagedToken("ADDITIVE", "\\+|-");
		TokenManager.createManagedToken("STRING", "(\".*?\")|('.*?')");
		TokenManager.createManagedToken("RELOP", "[<>][=>]?");
		TokenManager.createManagedToken("NUMBER", "[0-9]+(\\.[0-9]+)?");
		TokenManager.createManagedToken("BOOLEAN", "(true|false)");
		TokenManager.createManagedToken("IDENTIFIER", "[a-z_][a-z_0-9]*");
		
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("COMMA").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("EQUAL").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("LP").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("RP").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("DOT").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("STAR").getType());
		assertEquals(Token.Type.SIMPLE, TokenManager.getToken("SLASH").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("AND").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("LIKE").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("NOT").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("NULL").getType());
		assertEquals(Token.Type.WORD, TokenManager.getToken("OR").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("ADDITIVE").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("STRING").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("RELOP").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("NUMBER").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("BOOLEAN").getType());
		assertEquals(Token.Type.REGEX, TokenManager.getToken("IDENTIFIER").getType());
	}

	@After
	public void tearDown() throws Exception {
		TokenManager.clear();
	}
	
	private void testDoParse(String whereCond, boolean result) throws Exception {
		SQLScanner scanner = new SQLScanner(whereCond);
		scanner.next();
		
		SQLParser parser = new SQLParser(scanner);
		Expression resExpr = parser.parse();
		Value resValue = resExpr.evaluate(null);
		
		assertEquals(Value.Type.BOOLEAN, resValue.getType());
		assertEquals(result, ((BooleanValue)resValue).getValue());
	}

	@Test
	public void testParse() throws Exception {
		String whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
						 + "AND \"dustin\" = \"dustin\" OR true = false";
		testDoParse(whereCond, true);
		
		whereCond = "50 * 4 + 25 * 2 <> (25 + 25) * 10 / 2 "
				 + "AND \"dustin\" = \"dustin\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin\" <> \"dustin\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin\" = \"dustin\" OR true <> false";
		testDoParse(whereCond, true);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin\" = \"dustin\" AND true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" = \"dustin2\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"%dust\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"dust%\" OR true = false";
		testDoParse(whereCond, true);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"%dust%\" OR true = false";
		testDoParse(whereCond, true);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"%st\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"st%\" OR true = false";
		testDoParse(whereCond, false);
		
		whereCond = "50 * 4 + 25 * 2 = (25 + 25) * 10 / 2 "
				 + "AND \"dustin1\" LIKE \"%st%\" OR true = false";
		testDoParse(whereCond, true);
	}
}
