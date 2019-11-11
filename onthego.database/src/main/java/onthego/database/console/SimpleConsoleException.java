package onthego.database.console;

public class SimpleConsoleException extends Exception {

	public SimpleConsoleException() {}

	public SimpleConsoleException(String message) {
		super(message);
	}

	public SimpleConsoleException(Throwable cause) {
		super(cause);
	}

	public SimpleConsoleException(String message, Throwable cause) {
		super(message, cause);
	}

	public SimpleConsoleException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
