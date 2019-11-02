package onthego.database.core.sqlparser;

public interface Token {
	
	boolean match(String input, int pos);
	
	String lexeme();
	
	public static class NullToken implements Token {
		@Override
		public boolean match(String input, int pos) {
			return false;
		}

		@Override
		public String lexeme() {
			return "";
		}
	}
	
	public static final Token NULL_TOKEN = new NullToken();
}
