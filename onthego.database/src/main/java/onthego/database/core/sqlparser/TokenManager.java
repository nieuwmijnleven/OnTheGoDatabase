package onthego.database.core.sqlparser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class TokenManager {
	
	private static final List<Token> tokens = new ArrayList<>();
	
	private static final Pattern regexMetaCharacters = Pattern.compile("\\\\\\[\\]{}$\\^*+?|()");

	private TokenManager() {}
	
	public static Token createManagedToken(String patternString) {
		Token token = createToken(patternString);
		tokens.add(token);
		return token;
	}
	
	public static Token createToken(String patternString) {
		if (!patternString.startsWith("'") && containRegexMetaCharacters(patternString)) {
			return new RegexToken(patternString);
		} else {
			int start = 0;
			int end = patternString.length();
			if (patternString.startsWith("'") && patternString.endsWith("'")) {
				++start;
				--end;
			}
			
			if (Character.isJavaIdentifierPart(patternString.charAt(end - 1))) {
				return new WordToken(patternString.substring(start, end));
			} else {
				return new SimpleToken(patternString.substring(start, end));
			}
		}
	}
	
	public static Iterator<Token> iterator() {
		return tokens.iterator();
	}
	
	private static boolean containRegexMetaCharacters(String patternString) {
		return regexMetaCharacters.matcher(patternString).find();
	}
}
