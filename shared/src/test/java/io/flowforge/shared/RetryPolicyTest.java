package io.flowforge.shared;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RetryPolicyTest {
    @Test void doublesDelayAfterEachFailedAttempt() {
        assertThat(java.util.stream.IntStream.rangeClosed(1,4).mapToLong(i->RetryPolicy.delayMs(1000,2,i)).toArray())
                .containsExactly(1000,2000,4000,8000);
    }
    @Test void capsBackoffAndCountsRetriesAfterInitialAttempt() {
        assertThat(RetryPolicy.delayMs(3600000,10,20)).isEqualTo(3600000);
        assertThat(RetryPolicy.shouldRetry(1,0,true)).isFalse();
        assertThat(RetryPolicy.shouldRetry(3,3,true)).isTrue();
        assertThat(RetryPolicy.shouldRetry(4,3,true)).isFalse();
        assertThat(RetryPolicy.shouldRetry(1,3,false)).isFalse();
    }
}
