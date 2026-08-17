"""Phase B Wave B4 Gate — Monitor + Ops merge.

Gate checklist (plan B4.0):
1. RBAC: merged process uses ServiceAccount `api-ops` on api-monitor Deployment.
2. SSE: 19 streams in one process — accepted; same GIL model as today across pods;
   Ops Celery console SSE already uses unbounded proxy timeout.
3. Blast radius: operations outage = monitor + celery control together — accepted for
   aggressive Phase B (Owner chose 4-domain target).
4. Smoke after deploy: /api/monitor/health, /api/ops/health, log SSE, celery console SSE.

Decision: GATE PASS → merge Ops into Monitor process (alias Service api-ops → api-monitor).
"""
