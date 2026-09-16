package io.flowforge.api;

import io.flowforge.shared.EngineStore;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(WorkflowController.class)
class WorkflowControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean EngineStore store;
    @Test void submissionReturnsCreatedAndLocation() throws Exception {
        UUID id=UUID.randomUUID();when(store.submit(any())).thenReturn(Map.of("id",id,"status","PENDING"));
        mvc.perform(post("/api/workflows").contentType("application/json").content("""
          {"name":"Order","concurrencyLimit":2,"tasks":[{"name":"start","taskType":"DELAY","payload":{"durationMs":10}}]}
          """)).andExpect(status().isCreated()).andExpect(header().string("Location","/api/workflows/"+id)).andExpect(jsonPath("$.status").value("PENDING"));
    }
    @Test void validationRejectsInvalidTimeoutAndEmptyNameBeforePersistence() throws Exception {
        mvc.perform(post("/api/workflows").contentType("application/json").content("""
          {"name":"","tasks":[{"name":"start","taskType":"DELAY","timeoutMs":0}]}
          """)).andExpect(status().isBadRequest()).andExpect(jsonPath("$.detail").exists());
        verifyNoInteractions(store);
    }
    @Test void missingWorkflowProduces404Problem() throws Exception {
        when(store.workflow(any())).thenThrow(new MissingResourceException("Workflow not found","workflow","id"));
        mvc.perform(get("/api/workflows/"+UUID.randomUUID())).andExpect(status().isNotFound()).andExpect(jsonPath("$.detail").value("Workflow not found"));
    }
    @Test void malformedPayloadAndUuidAre400() throws Exception {
        mvc.perform(post("/api/workflows").contentType("application/json").content("{" )).andExpect(status().isBadRequest());
        mvc.perform(get("/api/workflows/not-a-uuid")).andExpect(status().isBadRequest());
    }
    @Test void paginationHasExplicitLimits() throws Exception {
        mvc.perform(get("/api/workflows?limit=0")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/workflows?offset=-1")).andExpect(status().isBadRequest());
        verifyNoInteractions(store);
    }
    @Test void cancellationUsesPostAndReturnsPersistedState() throws Exception {
        UUID id=UUID.randomUUID();when(store.cancel(id)).thenReturn(Map.of("id",id,"status","CANCELLED"));
        mvc.perform(post("/api/workflows/"+id+"/cancel")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CANCELLED"));
        verify(store).cancel(id);
    }
}
