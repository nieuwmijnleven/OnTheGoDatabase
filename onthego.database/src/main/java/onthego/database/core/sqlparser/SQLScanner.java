package onthego.database.core.sqlparser;

import java.util.Iterator;
import java.util.Optional;
import java.util.Scanner;

public class SQLScanner {
	
	private Scanner scanner;
	
	private Token currentToken = Token.NULL_TOKEN;
	
	private Token previousToken;
	
	private String currentLine;
	
	private int currentLinePos = 0;
	
	private int currentLineNumber = 0;

	public SQLScanner(String input) {
		this.scanner = new Scanner(input);
		readLine();
	}
	
	public Token next() throws SQLScannerException {
		if (currentToken != previousToken) {
			if (currentLine.length() == currentLinePos) {
				if (!readLine()) {
					return Token.NULL_TOKEN;
				}
			}
			
			while (Character.isWhitespace(currentLine.charAt(currentLinePos++))) {
				if (currentLine.length() == currentLinePos) {
					if (!readLine()) {
						return Token.NULL_TOKEN;
					}
				}
			}
			
			for (Iterator<Token> it = TokenManager.iterator(); it.hasNext();) {
				Token token = it.next();
				if (token.match(currentLine, currentLinePos)) {
					currentToken = token;
					currentLinePos += token.lexeme().length();
					break;
				}
			}
			
			previousToken = currentToken;
			if (previousToken == currentToken) {
				throw new SQLScannerException("Cannot recognize a token from " + currentLine.substring(currentLinePos));
			}
		}
		
		return currentToken;
	}
	
	public boolean match(Token token) {
		return currentToken == token;
	}
	
	public Optional<String> advanceIfMatch(Token token) throws SQLScannerException {
		if (match(token)) {
			String lexeme = currentToken.lexeme();
			next();
			return Optional.of(lexeme);
		}
		return Optional.empty();
	}
	
	public String advance(Token token) throws SQLScannerException {
		String lexeme = advanceIfMatch(token).orElseThrow(() -> new SQLScannerException("The token(" + token + ") is required."));
		return lexeme;
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
