package onthego.database.core.sqlprocessor.scanner;

public class SimpleToken extends Token {
	
	public SimpleToken(String patternStr) {
		super(Token.Type.SIMPLE, patternStr);
	}

	@Override
	public boolean match(String input, int pos) {
		return input.substring(pos, pos + getPattern().length()).equalsIgnoreCase(getPattern());
	}

	@Override
	public String lexeme() {
		return getPattern();
	}
}
