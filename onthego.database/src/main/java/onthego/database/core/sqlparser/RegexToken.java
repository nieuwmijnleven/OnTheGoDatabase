package onthego.database.core.sqlparser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexToken implements Token {
	
	private final String patternString;
	
	private final Pattern pattern;
	
	private Matcher matcher;

	public RegexToken(String patternString) {
		this.patternString = patternString;
		this.pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patternString == null) ? 0 : patternString.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RegexToken other = (RegexToken) obj;
		if (patternString == null) {
			if (other.patternString != null)
				return false;
		} else if (!patternString.equals(other.patternString))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return this.patternString;
	}
}
