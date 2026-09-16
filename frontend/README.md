# FlowForge dashboard

A small React + TypeScript monitoring application with live workflow execution,
dependency graphs, task inspection, execution history, worker heartbeats, metrics,
and dead-letter inspection. No demo data is injected into monitoring views.

## Develop

```sh
npm ci
npm run dev
```

Open http://localhost:5173. Vite proxies `/api` and `/actuator` to the API service
at http://localhost:8080, which is where a directly launched API JVM listens. The
Compose stack publishes the API on 8088 instead, so target it with
`VITE_API_PROXY=http://localhost:8088 npm run dev` (the variable is also read from
a `.env` file). Run `npm run build` to type-check and create `dist/`. Production
hosting must proxy the same paths to the API service.

The dev server binds loopback only, because its `/api` and `/actuator` proxies
would otherwise expose an unauthenticated API to the local network. Pass
`npm run dev -- --host` to opt into that deliberately.

The dashboard polls every four seconds without overlapping requests, cancels
requests on navigation, and pauses polling while the browser tab is hidden. If a
request fails, it retains the last successful snapshot and explicitly marks the
data as potentially stale. Rates from the metrics API are fractions (`0..1`).

Use **Submit workflow** to edit a JSON workflow definition or submit the parallel
order-processing example. The retry example fails its first two payment attempts
to expose retry/backoff behavior in the graph and history. Cancellation requires
an explicit confirmation inside the workflow detail view. The dead-letter view
is read-only; retrying terminal workflows is not exposed as an admin action.

The layout uses system fonts as a fallback if Google Fonts is unavailable. All
application behavior and monitored data work without third-party font access.
