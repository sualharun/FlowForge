package io.flowforge.shared;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DagValidatorTest {
    private WorkflowDefinition.TaskDefinition task(String name,String... parents) {
        return new WorkflowDefinition.TaskDefinition(name,"DELAY",Map.of("durationMs",1),List.of(parents),1000L,2,1000L,2d);
    }
    @Test void acceptsDiamondAndDisconnectedRoots() {
        assertThatCode(()->DagValidator.validate(new WorkflowDefinition("Diamond",2,List.of(task("a"),task("b","a"),task("c","a"),task("d","b","c"),task("e"))))).doesNotThrowAnyException();
    }
    @Test void rejectsCycleInDisconnectedComponent() {
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("cycle",2,List.of(task("root"),task("a","b"),task("b","a"))))).hasMessageContaining("cycle");
    }
    @Test void rejectsSelfLoop() {
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("cycle",2,List.of(task("a","a"))))).hasMessageContaining("cycle");
    }
    @Test void rejectsUnknownDependency() {
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("missing",2,List.of(task("a","absent"))))).hasMessageContaining("Unknown dependency");
    }
    @Test void rejectsDuplicateNamesAndEdges() {
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("duplicate",2,List.of(task("a"),task("a"))))).hasMessageContaining("Duplicate task");
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("duplicate",2,List.of(task("a"),task("b","a","a"))))).hasMessageContaining("Duplicate dependencies");
    }
    @Test void validatesLargeGraphWithoutRecursiveTraversal() {
        var tasks=new java.util.ArrayList<WorkflowDefinition.TaskDefinition>();
        for(int i=0;i<1000;i++) tasks.add(i==0 ? task("t0") : task("t"+i,"t"+(i-1)));
        assertThatCode(()->DagValidator.validate(new WorkflowDefinition("large",10,tasks))).doesNotThrowAnyException();
    }
    @Test void rejectsNonFiniteBackoffAndUnknownHandler() {
        var bad=new WorkflowDefinition.TaskDefinition("a","DELAY",Map.of(),List.of(),1000L,2,1000L,Double.NaN);
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("bad",2,List.of(bad)))).hasMessageContaining("configuration");
        var unknown=new WorkflowDefinition.TaskDefinition("a","SHELL",Map.of(),List.of(),1000L,2,1000L,2d);
        assertThatThrownBy(()->DagValidator.validate(new WorkflowDefinition("bad",2,List.of(unknown)))).hasMessageContaining("Unsupported");
    }
}
