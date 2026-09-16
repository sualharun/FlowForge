package io.flowforge.shared;

public final class RetryPolicy {
    private RetryPolicy() {}
    /** attemptNumber=1 is the initial execution; maxRetries counts additional executions. */
    public static boolean shouldRetry(int attemptNumber, int maxRetries, boolean retryable) {
        return retryable && attemptNumber <= maxRetries;
    }
    public static long delayMs(long initial, double multiplier, int failedAttemptNumber) {
        return (long) Math.min(3600000, initial * Math.pow(multiplier, Math.max(0, failedAttemptNumber - 1)));
    }
}
