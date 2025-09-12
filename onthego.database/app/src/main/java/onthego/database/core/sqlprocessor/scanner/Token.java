package onthego.database.core.sqlprocessor.scanner;

public abstract class Token {
	
	public enum Type { SIMPLE, WORD, REGEX, NULL }
	
	private final Type type;
	
	private final String patternStr;
	
	protected Token(Type type, String patternStr) {
		this.type = type;
		this.patternStr = patternStr;
	}
	
	public abstract boolean match(String input, int pos);
	
	public abstract String lexeme();
	
	public Type getType() {
		return this.type;
	}
	
	public String getPattern() {
		return this.patternStr;
	}

	@Override
	public String toString() {
		return "Token [type='" + type + "', pattern='" + patternStr + "', lexeme='" + lexeme() + "']";
	}

	public static final class DefaultToken extends Token {
		
		public DefaultToken() {
			super(Token.Type.NULL, null);
		}

		@Override
		public boolean match(String input, int pos) {
			return false;
		}

		@Override
		public String lexeme() {
			return "";
		}
	}
	
	public static final Token NULL_TOKEN = new DefaultToken();
	
	public static final Token BEGIN_TOKEN = new DefaultToken();
}
