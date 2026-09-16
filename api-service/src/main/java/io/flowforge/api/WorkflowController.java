package io.flowforge.api;

import io.flowforge.shared.EngineStore;
import io.flowforge.shared.WorkflowDefinition;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class WorkflowController {
    private final EngineStore store;
    public WorkflowController(EngineStore store) { this.store=store; }
    @PostMapping("/workflows") public ResponseEntity<Map<String,Object>> submit(@Valid @RequestBody WorkflowDefinition definition) {
        var workflow=store.submit(definition);
        return ResponseEntity.created(URI.create("/api/workflows/"+workflow.get("id"))).body(workflow);
    }
    @GetMapping("/workflows") public List<Map<String,Object>> list(@RequestParam(defaultValue="100") int limit,@RequestParam(defaultValue="0") int offset) {
        return store.workflows(limit(limit),offset(offset));
    }
    @GetMapping("/workflows/{id}") public Map<String,Object> get(@PathVariable UUID id) { return store.workflow(id); }
    @PostMapping("/workflows/{id}/cancel") public Map<String,Object> cancel(@PathVariable UUID id) { return store.cancel(id); }
    @GetMapping("/workflows/{id}/tasks") public List<Map<String,Object>> tasks(@PathVariable UUID id) { return store.tasks(id); }
    @GetMapping("/workflows/{id}/attempts") public List<Map<String,Object>> attempts(@PathVariable UUID id) { return store.attempts(id); }
    @GetMapping("/workflows/{id}/history") public List<Map<String,Object>> history(@PathVariable UUID id,@RequestParam(defaultValue="1000") int limit,@RequestParam(defaultValue="0") int offset) { return store.history(id,limit(limit),offset(offset)); }
    @GetMapping("/workers") public List<Map<String,Object>> workers() { return store.workers(); }
    @GetMapping("/dead-letters") public List<Map<String,Object>> deadLetters(@RequestParam(defaultValue="100") int limit,@RequestParam(defaultValue="0") int offset) { return store.deadLetters(limit(limit),offset(offset)); }
    @GetMapping("/admin/payments") public List<Map<String,Object>> payments(@RequestParam(required=false) UUID workflowId) { return store.payments(workflowId); }
    @GetMapping("/metrics/summary") public Map<String,Object> summary() { return store.summary(); }
    private int limit(int value) { if(value<1 || value>5000) throw new IllegalArgumentException("limit must be 1..5000");return value; }
    private int offset(int value) { if(value<0) throw new IllegalArgumentException("offset cannot be negative");return value; }
}
