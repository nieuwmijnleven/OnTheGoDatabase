package onthego.database.core.sqlprocessor.scanner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexToken extends Token {
	
	private final Pattern pattern;
	
	private Matcher matcher;

	public RegexToken(String patternStr) {
		super(Token.Type.REGEX, patternStr);
		this.pattern = Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
	}

	@Override
	public boolean match(String input, int pos) {
		matcher = pattern.matcher(input.substring(pos));
		return matcher.lookingAt();
	}

	@Override
	public String lexeme() {
		return matcher.group();
	}
}
