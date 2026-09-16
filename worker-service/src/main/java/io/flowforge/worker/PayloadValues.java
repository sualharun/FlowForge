package io.flowforge.worker;

import java.math.BigDecimal;
import java.util.Map;

final class PayloadValues {
    private PayloadValues() {}

    static long nonNegativeInteger(Map<String, Object> payload, String key, long fallback) {
        Object value = payload.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            try {
                long parsed = new BigDecimal(number.toString()).longValueExact();
                if (parsed >= 0) {
                    return parsed;
                }
            } catch (NumberFormatException | ArithmeticException ignored) {
                // Report a deterministic payload error below instead of truncating a fraction.
            }
        }
        throw new TaskExecutionException(key + " must be a non-negative integer", false);
    }
}
