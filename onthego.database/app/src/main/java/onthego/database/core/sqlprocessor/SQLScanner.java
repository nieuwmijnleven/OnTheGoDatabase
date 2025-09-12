package onthego.database.core.sqlprocessor;

import java.util.Iterator;
import java.util.Scanner;

import onthego.database.core.sqlprocessor.scanner.Token;
import onthego.database.core.sqlprocessor.scanner.TokenManager;

public class SQLScanner {
	
	private final Scanner scanner;
	
	private Token currentToken = Token.BEGIN_TOKEN;
	
	private String currentLine;
	
	private int currentLinePos = 0;
	
	private int currentLineNumber = 0;

	public SQLScanner(String input) {
		this.scanner = new Scanner(input);
		readLine();
	}
	
	public Token next() throws SQLScannerException {
		if (currentToken != Token.NULL_TOKEN) {
			if (currentLine.length() == currentLinePos) {
				if (!readLine()) {
					return Token.NULL_TOKEN;
				}
			}
			
			while (Character.isWhitespace(currentLine.charAt(currentLinePos))) {
				if (currentLine.length() == ++currentLinePos) {
					if (!readLine()) {
						return Token.NULL_TOKEN;
					}
				}
			}
			
			currentToken = Token.NULL_TOKEN;
			for (Iterator<Token> it = TokenManager.iterator(); it.hasNext();) {
				Token token = it.next();
				if (token.match(currentLine, currentLinePos)) {
//					System.out.println("token = " + token);
					currentToken = token;
					currentLinePos += token.lexeme().length();
					break;
				}
			}
			
			if (currentToken == Token.NULL_TOKEN) {
				throw new SQLScannerException(getFailInfo());
			}
		}
		
		return currentToken;
	}

	public String getFailInfo() {
		StringBuilder message = new StringBuilder();
		message.append("Line : ").append(currentLineNumber)
		.append(", Postion: ").append(currentLinePos).append("\n")
		.append(currentLine).append("\n");
		for (int i = 0; i < currentLinePos; ++i) {
			message.append("_");
		}
		message.append("^").append("\n");
		return message.toString();
	}
	
	public Token next(Token token) throws SQLScannerException {
		if (!match(token)) {
			throw new SQLScannerException("The token(" + token.getPattern() + ") is required.\n" + getFailInfo());
		}
		return next();
	}
	
	public boolean match(Token token) {
		return currentToken == token;
	}
	
	public Token getCurrentToken() {
		return this.currentToken;
	}
	
	public String getCurrentLexeme() {
		return this.getCurrentToken().lexeme();
	}
	
	private boolean readLine() {
		if (scanner.hasNextLine()) {
			currentLine = scanner.nextLine();
			currentLineNumber++;
			currentLinePos = 0;
			return true;
		}
		return false;
	}
}
