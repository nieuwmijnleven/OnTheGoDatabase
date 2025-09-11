package onthego.database.core.sqlprocessor.scanner;

public class WordToken extends Token {

	public WordToken(String patternStr) {
		super(Token.Type.WORD, patternStr);
	}

	@Override
	public boolean match(String input, int pos) {
		if ((input.length() - pos) < getPattern().length()) {
			return false;
		}
		
		if (!input.substring(pos, pos + getPattern().length()).equalsIgnoreCase(getPattern())) {
			return false;
		}
		
		return (input.length() == pos + getPattern().length())
				|| !Character.isJavaIdentifierPart(input.charAt(pos + getPattern().length()));
	}

	@Override
	public String lexeme() {
		return getPattern();
	}
}
