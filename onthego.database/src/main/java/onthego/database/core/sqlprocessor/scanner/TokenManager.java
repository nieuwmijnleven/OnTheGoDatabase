package onthego.database.core.sqlprocessor.scanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TokenManager {
	
	private static final List<Token> tokens = new ArrayList<>();
	
	private static final Map<String,Token> tokenMap = new HashMap<>();
	
	private static final Pattern regexMetaCharacters = Pattern.compile("[\\\\\\[\\]{}$\\^*+?|()]");

	private static List<Token> savedTokens = new ArrayList<>();
	
	private static Map<String,Token> savedTokenMap = new HashMap<>();
	
	private TokenManager() {}
	
	public static Token createManagedToken(String tokenName, String patternString) {
		if (tokenMap.containsKey(tokenName)) {
			throw new AlreadyDefinedTokenException(tokenName + " token is already defined.");
		}
		
		Token token = createToken(patternString);
		tokens.add(token);
		tokenMap.put(tokenName, token);
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
	
	public static Token removeManagedToken(String tokenName) {
		if (!tokenMap.containsKey(tokenName)) {
			return Token.NULL_TOKEN;
		}
		
		Token token = getToken(tokenName);
		tokens.remove(token);
		tokenMap.remove(tokenName);
		return token;
	}
	
	public static Token getToken(String tokenName) {
		if (!tokenMap.containsKey(tokenName)) {
			throw new UndefinedTokenException("There is no token(" + tokenName + ")");
		}
		
		return tokenMap.get(tokenName);
	}
	
	public static Iterator<Token> iterator() {
		return tokens.iterator();
	}
	
	public static void saveManagedTokens() {
		savedTokens = new ArrayList<>(tokens);
		savedTokenMap = new HashMap<>(tokenMap);
	}
	
	public static void restoreManagedTokens() {
		clear();
		tokens.addAll(savedTokens);
		tokenMap.putAll(savedTokenMap);
	}
	
	public static void clear() {
		tokens.clear();
		tokenMap.clear();
	}
	
	private static boolean containRegexMetaCharacters(String patternString) {
//		System.out.println(patternString + " = " + regexMetaCharacters.matcher(patternString).find());
		return regexMetaCharacters.matcher(patternString).find();
	}
}
