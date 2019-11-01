package onthego.database.core.exception;

public class MarginalPayloadSpaceException extends RuntimeException {

	public MarginalPayloadSpaceException() {}

	public MarginalPayloadSpaceException(String message) {
		super(message);
	}

	public MarginalPayloadSpaceException(Throwable cause) {
		super(cause);
	}

	public MarginalPayloadSpaceException(String message, Throwable cause) {
		super(message, cause);
	}

	public MarginalPayloadSpaceException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
