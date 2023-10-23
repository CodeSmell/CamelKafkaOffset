package codesmell.exception;

@SuppressWarnings("serial")
public class NonRetryException extends RuntimeException {

    Object originalMessage;
    
    public NonRetryException(String msg) {
        super(msg);
    }

    public NonRetryException(String msg, Throwable cause) {
        super(msg, cause);
    }
    
}
