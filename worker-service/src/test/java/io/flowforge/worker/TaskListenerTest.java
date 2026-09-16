package io.flowforge.worker;

import static io.flowforge.worker.TaskHandlersTest.job;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.flowforge.shared.TaskJob;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class TaskListenerTest {
    @Test
    void listenerPassesOnlyExecutionIdToAuthoritativeClaim() throws Exception {
        var json = JsonMapper.builder().findAndAddModules().build();
        var runtime = mock(WorkerRuntime.class);
        var listener = new TaskListener(json, runtime);
        var ack = mock(Acknowledgment.class);
        TaskJob message = job("UNTRUSTED", Map.of("amount", "tampered"), 1, 999);
        listener.onTask(json.writeValueAsString(message), ack);
        verify(runtime).execute(message.executionId(), ack);
        verifyNoInteractions(ack);
    }

    @Test
    void invalidDispatchIsNotAcknowledged() {
        var json = JsonMapper.builder().findAndAddModules().build();
        var runtime = mock(WorkerRuntime.class);
        var listener = new TaskListener(json, runtime);
        var ack = mock(Acknowledgment.class);
        assertThatThrownBy(() -> listener.onTask("{}", ack)).isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(runtime, ack);
    }
}
