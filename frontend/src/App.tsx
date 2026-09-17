import { useEffect, useMemo, useRef, useState } from 'react';
import { api, usePolling } from './api';
import type { DeadLetter, Metrics, Task, Worker, Workflow, WorkflowEvent } from './types';

type View = 'workflows' | 'workers' | 'metrics' | 'dead-letters';
type IconName = 'flow' | 'workers' | 'chart' | 'inbox' | 'plus' | 'arrow' | 'refresh' | 'check' | 'close';

function Icon({ name, size = 20 }: { name: IconName; size?: number }) {
  const paths: Record<IconName, React.ReactNode> = {
    flow: <><rect x="3" y="3" width="6" height="6" rx="1.5" /><rect x="15" y="15" width="6" height="6" rx="1.5" /><path d="M6 9v9h9M9 6h9v9" /></>,
    workers: <><rect x="3" y="3" width="18" height="7" rx="2" /><rect x="3" y="14" width="18" height="7" rx="2" /><path d="M7 6.5h.01M7 17.5h.01M16 6.5h2M16 17.5h2" /></>,
    chart: <><path d="M4 3v17h17M9 15v-4M14 15V6M19 15V9" /></>,
    inbox: <><path d="M4 4h16l2 12v4H2v-4L4 4Z" /><path d="M2 15h6l2 3h4l2-3h6M12 7v4M12 13h.01" /></>,
    plus: <path d="M12 5v14M5 12h14" />,
    arrow: <path d="m14 6-6 6 6 6M8 12h13" />,
    refresh: <><path d="M20 11a8 8 0 0 0-14-5L3 9M3 3v6h6M4 13a8 8 0 0 0 14 5l3-3M15 15h6v6" /></>,
    check: <path d="m5 12 4 4L19 6" />,
    close: <path d="m6 6 12 12M6 18 18 6" />,
  };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{paths[name]}</svg>;
}

const number = (value: number | undefined, digits = 0) => value == null || !Number.isFinite(value) ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: digits });
const percent = (value: number | undefined) => value == null || !Number.isFinite(value) ? '—' : `${number(value * 100, 1)}%`;
const shortId = (id: string) => id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
const pretty = (value: unknown) => typeof value === 'string' ? value : JSON.stringify(value, null, 2) ?? '—';
function date(value: string | null | undefined) {
  return value ? new Date(value).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—';
}
function duration(start: string | null, end: string | null) {
  if (!start) return '—';
  const ms = Math.max(0, (end ? new Date(end).getTime() : Date.now()) - new Date(start).getTime());
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor(ms % 60000 / 1000)}s`;
}

function Badge({ status }: { status: string }) {
  return <span className={`badge status-${status.toLowerCase()}`}><span />{status.replaceAll('_', ' ').toLowerCase()}</span>;
}
function Empty({ title, children, icon = 'flow' }: { title: string; children: React.ReactNode; icon?: IconName }) {
  return <div className="empty-state"><div className="empty-icon"><Icon name={icon} size={26} /></div><h3>{title}</h3><p>{children}</p></div>;
}
function ErrorNotice({ message, refresh }: { message: string | null; refresh: () => void }) {
  if (!message) return null;
  return <div role="alert" className="error-notice"><span><strong>Unable to refresh data.</strong> {message} Previously loaded values may be out of date.</span><button onClick={refresh}>Retry</button></div>;
}
function Loading() { return <div className="loading" role="status"><span className="spinner" />Loading live data…</div>; }
function MetricCard({ label, value, hint, accent }: { label: string; value: string; hint: string; accent?: boolean }) {
  return <article className={`metric-card${accent ? ' accent' : ''}`}><div className="metric-label">{label}</div><div className="metric-value">{value}</div><div className="metric-hint">{hint}</div></article>;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function WorkflowList({ select, submitted }: { select: (id: string) => void; submitted: number }) {
  const { data, loading, error, refresh } = usePolling<Workflow[]>('/workflows');
  const [query, setQuery] = useState('');
  const [status, setStatus] = useState('ALL');
  // The list holds only the most recent page, so filtering it cannot reach an older
  // execution. When the query is a workflow ID that is not on this page, fetch it
  // directly instead of reporting that nothing matched.
  const [lookup, setLookup] = useState<Workflow | null>(null);
  useEffect(() => { if (submitted) refresh(); }, [submitted, refresh]);
  const id = query.trim();
  const onPage = (data ?? []).some(workflow => workflow.id === id);
  useEffect(() => {
    if (!UUID_PATTERN.test(id) || onPage) { setLookup(null); return; }
    let cancelled = false;
    api<Workflow>(`/workflows/${id}`)
      .then(workflow => { if (!cancelled) setLookup(workflow); })
      .catch(() => { if (!cancelled) setLookup(null); });
    return () => { cancelled = true; };
  }, [id, onPage]);
  const searchable = lookup ? [lookup, ...(data ?? [])] : (data ?? []);
  const filtered = searchable.filter(workflow => (status === 'ALL' || workflow.status === status) && `${workflow.name} ${workflow.id}`.toLowerCase().includes(query.toLowerCase()));
  return <section className="panel">
    <div className="panel-heading"><div><h2>Workflow executions <span className="count">{data?.length ?? '—'}</span></h2><p>Every execution, from submission to completion.</p></div><button className="icon-button" title="Refresh workflows" aria-label="Refresh workflows" onClick={refresh}><Icon name="refresh" size={17} /></button></div>
    <ErrorNotice message={error} refresh={refresh} />
    <div className="table-toolbar"><label className="search"><svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><circle cx="10" cy="10" r="6" /><path d="m15 15 5 5" /></svg><input value={query} onChange={event => setQuery(event.target.value)} placeholder="Search name or workflow ID…" aria-label="Search workflows" /></label><select aria-label="Filter workflow status" value={status} onChange={event => setStatus(event.target.value)}><option value="ALL">All statuses</option>{['PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'].map(value => <option key={value} value={value}>{value.charAt(0) + value.slice(1).toLowerCase()}</option>)}</select></div>
    {loading ? <Loading /> : !filtered.length ? <Empty title={data?.length ? 'No matching workflows' : 'Ready for your first workflow'}>{data?.length ? 'Try a different name or status.' : 'Submit a workflow to watch its tasks execute across your workers.'}</Empty> : <div className="table-scroll"><table><thead><tr><th>Workflow</th><th>Status</th><th>Task progress</th><th>Duration</th><th>Created</th><th><span className="sr-only">Details</span></th></tr></thead><tbody>{filtered.map(workflow => <tr key={workflow.id}><td><button className="name-button" onClick={() => select(workflow.id)}>{workflow.name}</button><span className="cell-sub mono" title={workflow.id}>{shortId(workflow.id)}</span></td><td><Badge status={workflow.status} /></td><td><div className="task-progress"><span>{workflow.completedTasks} <span className="muted">/ {workflow.totalTasks}</span></span><div className="progress-track"><span style={{ width: `${workflow.totalTasks ? workflow.completedTasks / workflow.totalTasks * 100 : 0}%` }} /></div></div></td><td className="mono">{duration(workflow.startedAt, workflow.finishedAt)}</td><td className="nowrap muted">{date(workflow.createdAt)}</td><td><button className="row-open" aria-label={`Open ${workflow.name}`} onClick={() => select(workflow.id)}>↗</button></td></tr>)}</tbody></table></div>}
    <div className="panel-footer"><span>{number(filtered.length)} execution{filtered.length === 1 ? '' : 's'}</span><span>Auto-refresh every 4 seconds</span></div>
  </section>;
}

function taskPositions(tasks: Task[]) {
  const byId = new Map(tasks.map(task => [task.id, task]));
  const ranks = new Map<string, number>();
  // Topological layering; the API validates acyclicity before accepting a graph.
  let pending = [...tasks];
  for (let pass = 0; pending.length && pass < tasks.length; pass++) {
    const next: Task[] = [];
    for (const task of pending) {
      const dependencies = task.dependencies.filter(id => byId.has(id));
      if (dependencies.every(id => ranks.has(id))) ranks.set(task.id, dependencies.length ? Math.max(...dependencies.map(id => ranks.get(id)!)) + 1 : 0);
      else next.push(task);
    }
    if (next.length === pending.length) break;
    pending = next;
  }
  const layers = new Map<number, Task[]>();
  for (const task of tasks) {
    const rank = ranks.get(task.id) ?? 0;
    layers.set(rank, [...(layers.get(rank) ?? []), task]);
  }
  const height = Math.max(252, Math.max(0, ...[...layers.values()].map(layer => layer.length)) * 146 + 48);
  const width = Math.max(590, layers.size * 288 + 24);
  const positions = new Map<string, { x: number; y: number }>();
  for (const [rank, layer] of layers) layer.forEach((task, index) => positions.set(task.id, { x: rank * 288 + 32, y: (height - layer.length * 146) / 2 + index * 146 }));
  return { positions, height, width };
}

function Dag({ tasks, selected, select }: { tasks: Task[]; selected: string | null; select: (id: string) => void }) {
  const layout = useMemo(() => taskPositions(tasks), [tasks]);
  if (!tasks.length) return <Empty title="No tasks available">Tasks will appear here when the workflow is loaded.</Empty>;
  // role="group" so the label is actually exposed: aria-label is prohibited on a plain div,
  // and role="img" would hide the task buttons inside the graph.
  return <div className="dag-scroll" role="group" aria-label="Workflow dependency graph"><div className="dag-canvas" style={{ width: layout.width, height: layout.height }}>
    <svg className="dag-edges" width={layout.width} height={layout.height} aria-hidden="true"><defs><marker id="arrowhead" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7" fill="#a0b2bb" /></marker></defs>{tasks.flatMap(task => task.dependencies.map(parentId => {
      const from = layout.positions.get(parentId); const to = layout.positions.get(task.id);
      if (!from || !to) return null;
      const x1 = from.x + 216, y1 = from.y + 57, x2 = to.x - 6, y2 = to.y + 57;
      return <path key={`${parentId}-${task.id}`} d={`M${x1},${y1} C${x1 + 38},${y1} ${x2 - 38},${y2} ${x2},${y2}`} fill="none" stroke="#acbdc4" strokeWidth="1.5" markerEnd="url(#arrowhead)" />;
    }))}</svg>
    {tasks.map(task => { const position = layout.positions.get(task.id)!; return <button key={task.id} className={`dag-node node-${task.status.toLowerCase()}${selected === task.id ? ' selected' : ''}`} style={{ left: position.x, top: position.y }} onClick={() => select(task.id)} aria-pressed={selected === task.id}><div className="node-top"><Badge status={task.status} /><span className="mono">{number(task.attemptCount)}×</span></div><strong title={task.name}>{task.name}</strong><div className="node-bottom"><span>{task.taskType.replaceAll('_', ' ').toLowerCase()}</span><span>{duration(task.startedAt, task.finishedAt)}</span></div></button>; })}
  </div></div>;
}

function TaskInspector({ task, tasks }: { task: Task; tasks: Task[] }) {
  const dependencies = task.dependencies.map(id => tasks.find(candidate => candidate.id === id)?.name ?? id);
  return <div className="task-inspector"><div className="inspector-title"><span className="eyebrow">SELECTED TASK</span><h3>{task.name}</h3><span className="mono muted">{task.id}</span></div><div className="detail-grid"><div><label>Task type</label><span>{task.taskType}</span></div><div><label>Timeout</label><span>{number(task.timeoutMs)} ms</span></div><div><label>Attempts / max retries</label><span>{task.attemptCount} / {task.maxRetries}</span></div><div><label>Retry backoff</label><span>{number(task.initialRetryDelayMs)} ms × {task.backoffMultiplier}</span></div><div><label>Worker</label><span className="mono">{task.workerId || 'Unassigned'}</span></div><div><label>Depends on</label><span>{dependencies.join(', ') || 'No dependencies'}</span></div></div><div className="payload-grid"><details><summary>Input payload</summary><pre>{pretty(task.payload)}</pre></details><details><summary>Task result</summary><pre>{pretty(task.result)}</pre></details></div></div>;
}

function History({ workflowId, tasks }: { workflowId: string; tasks: Task[] }) {
  const { data, error, loading, refresh } = usePolling<WorkflowEvent[]>(`/workflows/${workflowId}/history`);
  return <section className="panel"><div className="panel-heading"><div><h2>Execution history <span className="count">{data?.length ?? '—'}</span></h2><p>Persisted lifecycle events for this workflow.</p></div></div><ErrorNotice message={error} refresh={refresh} />{loading ? <Loading /> : !data?.length ? <Empty title="No events yet">Execution events will appear as the workflow advances.</Empty> : <div className="table-scroll"><table className="history-table"><thead><tr><th>Event</th><th>Task</th><th>Time</th><th>Details</th></tr></thead><tbody>{[...data].sort((a, b) => b.createdAt.localeCompare(a.createdAt)).map(event => <tr key={event.id}><td><span className="event-dot" /><span className="event-type">{event.type.replaceAll('_', ' ').toLowerCase()}</span></td><td>{tasks.find(task => task.id === event.taskId)?.name ?? (event.taskId ? shortId(event.taskId) : 'Workflow')}</td><td className="nowrap muted">{date(event.createdAt)}</td><td><details className="event-details"><summary>View details</summary><pre>{pretty(event.details)}</pre>{event.executionId && <span className="mono cell-sub">Execution: {event.executionId}</span>}</details></td></tr>)}</tbody></table></div>}</section>;
}

function WorkflowDetail({ workflowId, back }: { workflowId: string; back: () => void }) {
  const workflow = usePolling<Workflow>(`/workflows/${workflowId}`);
  const tasks = usePolling<Task[]>(`/workflows/${workflowId}/tasks`);
  const [selected, setSelected] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const selectedTask = tasks.data?.find(task => task.id === selected);
  async function cancel() {
    setCancelling(true); setCancelError(null);
    try { await api(`/workflows/${workflowId}/cancel`, { method: 'POST' }); workflow.refresh(); tasks.refresh(); setConfirmCancel(false); }
    catch (cause) { setCancelError(cause instanceof Error ? cause.message : 'Cancellation failed.'); }
    finally { setCancelling(false); }
  }
  return <><button className="back-button" onClick={back}><Icon name="arrow" size={16} />All workflows</button><ErrorNotice message={workflow.error} refresh={workflow.refresh} />{workflow.loading ? <Loading /> : workflow.data && <><div className="detail-heading"><div><div className="heading-with-badge"><h1>{workflow.data.name}</h1><Badge status={workflow.data.status} /></div><p className="mono muted">{workflow.data.id}</p></div>{['PENDING', 'RUNNING'].includes(workflow.data.status) && <div className="cancel-controls">{confirmCancel ? <><span>Cancel this execution?</span><button className="button danger" disabled={cancelling} onClick={() => void cancel()}>{cancelling ? 'Cancelling…' : 'Confirm cancellation'}</button><button className="button secondary" disabled={cancelling} onClick={() => setConfirmCancel(false)}>Keep running</button></> : <button className="button secondary" onClick={() => setConfirmCancel(true)}>Cancel workflow</button>}</div>}</div>{cancelError && <div role="alert" className="error-notice">{cancelError}</div>}<div className="detail-summary"><div><label>Created</label><span>{date(workflow.data.createdAt)}</span></div><div><label>Duration</label><span>{duration(workflow.data.startedAt, workflow.data.finishedAt)}</span></div><div><label>Task progress</label><span>{workflow.data.completedTasks} / {workflow.data.totalTasks} completed</span></div><div><label>Concurrency limit</label><span>{workflow.data.concurrencyLimit} tasks</span></div></div></>}
    <section className="panel"><div className="panel-heading"><div><h2>Dependency graph</h2><p>Tasks run left to right. Select a node to inspect its execution.</p></div><span className="graph-label">DAG VIEW</span></div><ErrorNotice message={tasks.error} refresh={tasks.refresh} />{tasks.loading ? <Loading /> : <Dag tasks={tasks.data ?? []} selected={selected} select={setSelected} />}{selectedTask && <TaskInspector task={selectedTask} tasks={tasks.data ?? []} />}</section><History workflowId={workflowId} tasks={tasks.data ?? []} /></>;
}

function Workers() {
  const { data, error, loading, refresh } = usePolling<Worker[]>('/workers');
  return <section className="panel"><div className="panel-heading"><div><h2>Worker fleet <span className="count">{data?.length ?? '—'}</span></h2><p>Distributed executors and their latest heartbeat.</p></div><button className="icon-button" onClick={refresh} aria-label="Refresh workers"><Icon name="refresh" size={17} /></button></div><ErrorNotice message={error} refresh={refresh} />{loading ? <Loading /> : !data?.length ? <Empty icon="workers" title="No workers registered">Start a worker service to begin processing ready tasks.</Empty> : <div className="table-scroll"><table><thead><tr><th>Worker ID</th><th>Status</th><th>Last heartbeat</th><th>Active tasks</th><th>Completed tasks</th></tr></thead><tbody>{data.map(worker => <tr key={worker.id}><td className="mono">{worker.id}</td><td><Badge status={worker.status} /></td><td className="muted nowrap">{date(worker.lastHeartbeat)}</td><td>{number(worker.activeTasks)}</td><td>{number(worker.completedTasks)}</td></tr>)}</tbody></table></div>}<div className="panel-footer"><span>Heartbeat state is reported by the backend</span><span>Auto-refresh every 4 seconds</span></div></section>;
}

function DeadLetters({ select }: { select: (id: string) => void }) {
  const { data, error, loading, refresh } = usePolling<DeadLetter[]>('/dead-letters');
  return <section className="panel"><div className="panel-heading"><div><h2>Dead-letter tasks <span className="count">{data?.length ?? '—'}</span></h2><p>Terminal task failures retained for investigation.</p></div><button className="icon-button" onClick={refresh} aria-label="Refresh dead letters"><Icon name="refresh" size={17} /></button></div><ErrorNotice message={error} refresh={refresh} />{loading ? <Loading /> : !data?.length ? <Empty icon="check" title="No dead-letter tasks">Tasks that exhaust their retries or fail permanently will appear here.</Empty> : <div className="table-scroll"><table><thead><tr><th>Task / workflow</th><th>Failure reason</th><th>Recorded</th><th>Payload</th></tr></thead><tbody>{data.map(item => <tr key={item.id}><td><span className="mono" title={item.taskId}>{shortId(item.taskId)}</span><button className="text-button cell-sub" onClick={() => select(item.workflowId)}>Open workflow ↗</button></td><td className="failure-reason">{item.reason}</td><td className="nowrap muted">{date(item.createdAt)}</td><td><details><summary>Inspect</summary><pre>{pretty(item.payload)}</pre><span className="mono cell-sub">Execution: {item.executionId}</span></details></td></tr>)}</tbody></table></div>}</section>;
}

function MetricsView({ data }: { data: Metrics | null }) {
  return <><div className="metrics-grid expanded"><MetricCard label="Workflows submitted" value={number(data?.workflowsSubmitted)} hint="All submitted executions" /><MetricCard label="Workflows completed" value={number(data?.workflowsCompleted)} hint="Successful executions" accent /><MetricCard label="Workflows failed" value={number(data?.workflowsFailed)} hint="Terminal workflow failures" /><MetricCard label="Tasks per minute" value={number(data?.tasksPerMinute, 1)} hint="Recent completion throughput" /><MetricCard label="Task success rate" value={percent(data?.taskSuccessRate)} hint="Successful task executions" /><MetricCard label="Retry rate" value={percent(data?.retryRate)} hint="Reported by the metrics API" /><MetricCard label="Dead-letter count" value={number(data?.deadLetterCount)} hint="Tasks requiring investigation" /><MetricCard label="Queue depth" value={number(data?.queueDepth)} hint="Tasks awaiting execution" /><MetricCard label="Active workers" value={number(data?.activeWorkers)} hint="Healthy execution capacity" /><MetricCard label="p50 task duration" value={data ? `${number(data.p50TaskDurationMs, 1)} ms` : '—'} hint="Median recorded task duration" /><MetricCard label="p95 task duration" value={data ? `${number(data.p95TaskDurationMs, 1)} ms` : '—'} hint="95th percentile duration" /></div><div className="observability-note"><Icon name="chart" /><div><strong>Built for deeper observability</strong><p>Scrape the service metrics endpoints with Prometheus for historical dashboards and alerting.</p></div><a href="/actuator/prometheus" target="_blank" rel="noreferrer">Open API metrics ↗</a></div></>;
}

function sampleWorkflow(retry = false) {
  const task = (name: string, taskType: string, payload: unknown, dependsOn: string[] = []) => ({ name, taskType, payload, dependsOn, timeoutMs: 15000, maxRetries: 3, initialRetryDelayMs: 1000, backoffMultiplier: 2 });
  return JSON.stringify({ name: retry ? 'Order processing · retry demo' : 'Order processing · parallel', concurrencyLimit: 3, tasks: [
    task('validateOrder', 'DATA_TRANSFORM', { data: 'order accepted', operation: 'UPPERCASE' }),
    task('reserveInventory', 'DELAY', { durationMs: 3000 }, ['validateOrder']),
    task('processPayment', 'MOCK_PAYMENT', { amount: 49.9, currency: 'USD', orderId: `demo-${Date.now()}`, ...(retry ? { failUntilAttempt: 2 } : {}) }, ['validateOrder']),
    task('createShipment', 'DELAY', { durationMs: 1500 }, ['reserveInventory', 'processPayment']),
    task('sendConfirmation', 'DATA_TRANSFORM', { data: 'Your order is on its way', operation: 'IDENTITY' }, ['createShipment']),
  ] }, null, 2);
}

function SubmitDialog({ close, created }: { close: () => void; created: (id: string) => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const editor = useRef<HTMLTextAreaElement>(null);
  const [definition, setDefinition] = useState(() => sampleWorkflow());
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  // showModal() runs after React honours autoFocus and moves focus to the dialog's
  // first focusable child, so the editor has to be focused explicitly afterwards.
  // Closing returns focus to whatever opened the dialog rather than dropping it on the body.
  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    dialog.current?.showModal();
    editor.current?.focus();
    return () => opener?.focus?.();
  }, []);
  async function submit(event: React.FormEvent) {
    event.preventDefault(); setError(null);
    let parsed: unknown;
    try { parsed = JSON.parse(definition); } catch { setError('Enter valid JSON. Check commas, quotes, and brackets.'); return; }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) { setError('The workflow definition must be a JSON object.'); return; }
    setSubmitting(true);
    try { const workflow = await api<Workflow>('/workflows', { method: 'POST', body: JSON.stringify(parsed) }); created(workflow.id); }
    catch (cause) { setError(cause instanceof Error ? cause.message : 'Submission failed.'); }
    finally { setSubmitting(false); }
  }
  return <dialog ref={dialog} className="submit-dialog" onCancel={event => { event.preventDefault(); if (!submitting) close(); }} aria-labelledby="submit-title"><form onSubmit={event => void submit(event)}><div className="dialog-heading"><div><span className="eyebrow">NEW EXECUTION</span><h2 id="submit-title">Submit a workflow</h2><p>Define tasks and dependencies. FlowForge handles the execution.</p></div><button type="button" className="icon-button" aria-label="Close dialog" onClick={close} disabled={submitting}><Icon name="close" /></button></div><div className="template-toolbar"><span>Start with an example</span><button type="button" onClick={() => { setDefinition(sampleWorkflow()); setError(null); }} disabled={submitting}>Parallel order processing</button><button type="button" onClick={() => { setDefinition(sampleWorkflow(true)); setError(null); }} disabled={submitting}>With retries</button></div><label className="editor-label" htmlFor="workflow-json">Workflow definition <span>JSON</span></label><textarea ref={editor} id="workflow-json" className="json-editor" value={definition} onChange={event => setDefinition(event.target.value)} spellCheck={false} disabled={submitting} />{error && <div role="alert" className="error-notice">{error}</div>}<div className="dialog-footer"><span>Dependency names must match task names.</span><button type="button" className="button secondary" onClick={close} disabled={submitting}>Cancel</button><button className="button primary" disabled={submitting} type="submit">{submitting ? <><span className="spinner" />Submitting…</> : <><Icon name="plus" size={17} />Submit workflow</>}</button></div></form></dialog>;
}

const pages: { id: View; title: string; subtitle: string; icon: IconName }[] = [
  { id: 'workflows', title: 'Workflows', subtitle: 'Orchestrate work. Follow every step.', icon: 'flow' },
  { id: 'workers', title: 'Workers', subtitle: 'A clear view of your execution fleet.', icon: 'workers' },
  { id: 'metrics', title: 'Metrics', subtitle: 'Throughput, reliability, and execution health.', icon: 'chart' },
  { id: 'dead-letters', title: 'Dead letters', subtitle: 'Investigate the tasks that need your attention.', icon: 'inbox' },
];

export default function App() {
  const [view, setView] = useState<View>('workflows');
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  const [showSubmit, setShowSubmit] = useState(false);
  const [submitted, setSubmitted] = useState(0);
  const metrics = usePolling<Metrics>('/metrics/summary');
  const page = pages.find(item => item.id === view)!;
  function navigate(next: View) { setView(next); setWorkflowId(null); }
  function select(id: string) { setView('workflows'); setWorkflowId(id); }
  return <div className="app-shell"><aside className="sidebar"><a className="brand" href="/" onClick={event => { event.preventDefault(); navigate('workflows'); }}><span className="brand-mark"><Icon name="flow" size={22} /></span>FlowForge<span className="brand-dot">.</span></a><div className="workspace-switch"><span className="workspace-avatar">F</span><div><strong>Control plane</strong><span>Distributed execution</span></div><span className="workspace-chevron" aria-hidden="true">⌄</span></div><div className="nav-label">WORKSPACE</div><nav aria-label="Main navigation">{pages.map(item => { const count = item.id === 'dead-letters' ? metrics.data?.deadLetterCount ?? 0 : 0; return <button key={item.id} className={`nav-item${view === item.id ? ' active' : ''}`} onClick={() => navigate(item.id)} aria-current={view === item.id ? 'page' : undefined} aria-label={count > 0 ? `${item.title}, ${count} needing attention` : undefined}><Icon name={item.icon} size={19} /><span>{item.title}</span>{count > 0 && <span className="nav-count" aria-hidden="true">{count}</span>}</button>; })}</nav><div className="sidebar-bottom"><div className="engine-mark"><span className={`connection-dot${metrics.error ? ' offline' : metrics.data ? '' : ' pending'}`} /><span>{metrics.error ? 'API unavailable' : metrics.data ? 'Connected to API' : 'Connecting to API'}</span></div><p>Persistent by design.<br />Observable at every step.</p><div className="sidebar-version">FLOWFORGE <span>v1.0</span></div></div></aside><div className="main-shell"><header className="topbar"><div className="breadcrumb">Workspace <span>/</span> <strong>{page.title}</strong>{workflowId && <><span>/</span><span className="mono">{shortId(workflowId)}</span></>}</div><div className="live-status"><span className={`connection-dot${metrics.error ? ' offline' : metrics.data ? '' : ' pending'}`} />{metrics.error ? 'Connection interrupted' : metrics.updatedAt ? `Updated ${metrics.updatedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}` : 'Connecting…'}</div></header><main>{workflowId ? <WorkflowDetail key={workflowId} workflowId={workflowId} back={() => setWorkflowId(null)} /> : <><div className="page-heading"><div><span className="eyebrow">EXECUTION CONTROL PLANE</span><h1>{page.title}</h1><p>{page.subtitle}</p></div><button className="button primary" onClick={() => setShowSubmit(true)}><Icon name="plus" size={18} />Submit workflow</button></div><ErrorNotice message={metrics.error} refresh={metrics.refresh} />{view === 'workflows' && <><div className="metrics-grid"><MetricCard label="Total workflows" value={number(metrics.data?.workflowsSubmitted)} hint="Submitted executions" /><MetricCard label="Task success rate" value={percent(metrics.data?.taskSuccessRate)} hint="Successful task executions" accent /><MetricCard label="Active workers" value={number(metrics.data?.activeWorkers)} hint="Healthy execution capacity" /><MetricCard label="Queue depth" value={number(metrics.data?.queueDepth)} hint="Tasks awaiting execution" /></div><WorkflowList select={select} submitted={submitted} /></>}{view === 'workers' && <Workers />}{view === 'metrics' && <MetricsView data={metrics.data} />}{view === 'dead-letters' && <DeadLetters select={select} />}</>}<footer className="main-footer"><span>FlowForge / Distributed workflow engine</span><span>PostgreSQL · Kafka · Redis</span></footer></main></div>{showSubmit && <SubmitDialog close={() => setShowSubmit(false)} created={id => { setShowSubmit(false); setSubmitted(value => value + 1); metrics.refresh(); select(id); }} />}</div>;
}
