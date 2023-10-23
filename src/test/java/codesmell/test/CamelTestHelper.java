package codesmell.test;

import org.apache.camel.Exchange;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class CamelTestHelper {

    /**
     * assert the Camel exchange did NOT have an unhandled exception
     * @param exchange
     * @return the Exception which should be null
     */
    public static Exception assertUnhandledExceptionIsEmpty(Exchange exchange) {
        assertFalse(exchange.isFailed());
        Exception unhandledException = exchange.getException();
        assertNull(unhandledException);
        
        return unhandledException;
    }
    
    /**
     * assert the Camel exchange had an unhandled exception
     * @param exchange
     * @return the Exception which should NOT be null
     */
    public static Exception assertUnhandledExceptionIsFound(Exchange exchange) {
        assertTrue(exchange.isFailed());
        Exception unhandledException = exchange.getException();
        assertNotNull(unhandledException);

        return unhandledException;
    }
    
    /**
     * assert the Camel exchange did NOT have a handled exception
     * @param exchange
     * @return the Exception which should be null
     */
    public static Exception assertHandledExceptionIsEmpty(Exchange exchange) {
        Exception handledException = (Exception) exchange.getProperty(Exchange.EXCEPTION_CAUGHT);
        assertNull(handledException);
        
        return handledException;
    }
    
    /**
     * assert the Camel exchange had a handled exception
     * @param exchange
     * @return the Exception which should NOT be null
     */
    public static Exception assertHandledExceptionIsFound(Exchange exchange) {
        Exception handledException = (Exception) exchange.getProperty(Exchange.EXCEPTION_CAUGHT);
        assertNotNull(handledException);
        
        return handledException;
    }
}
