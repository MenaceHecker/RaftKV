# Dashboard (`web/`)

Task D7. Spec: RUNBOOK 10.7 and 10.10. API: `docs/gateway.md`. A static Vite + React +
TypeScript app that shows the cluster from the gateway API (and only what that API provides),
lets a visitor use the abuse-controlled buttons, runs the verifier, and falls back to a recorded
session when the gateway cannot be reached, so the link keeps working after AWS is destroyed
(RUNBOOK 10.9).

## Run it

```
cd web
npm ci
scripts/dev-gateway.sh &                 # real gateway, in-memory demo cluster, no AWS, :8080
GATEWAY_PROXY=http://127.0.0.1:8080 VITE_GATEWAY_URL=/api npm run dev   # same origin, no CORS
```

Without a gateway (`VITE_GATEWAY_URL` unset, `VITE_REPLAY_ONLY=true`, or `?replay=1` in the URL)
the page plays `public/recording.jsonl` and says REPLAY.

To point a build straight at a gateway instead of the proxy, the gateway must be started with
`--dashboard-origin` set to the dashboard's origin (it allows exactly one):
`scripts/dev-gateway.sh http://localhost:5173` (dev) or `http://localhost:4173` (`npm run preview`).

Gates (all from `web/`): `npm ci`, `npm run typecheck`, `npm run lint`, `npm test`,
`npm run build`, `npm run check:no-go`, `npm run check:dist`. `npm run test:contract` needs a
running dev gateway and `CONTRACT_GATEWAY=http://127.0.0.1:8080`; it checks the hand-written
guards and the CAS text against the real `internal/gateway`, so the in-process mock used by
`npm test` cannot drift unnoticed.

## Environment (all public by nature: they end up in the bundle)

| Variable | Meaning |
|---|---|
| `VITE_GATEWAY_URL` | Gateway origin (production: `https://api.raftkv.tushar25.com`, **to be confirmed**) or `/api` for the Vite dev proxy. Unset = replay only. |
| `VITE_REPLAY_ONLY` | `true` = never call a gateway. |
| `VITE_REPO_URL` | Repo URL; the verifier links `docs/CHAOS_RESULTS.md` from it. |

A test fails on any other `import.meta.env.*` read. The admin token is never an env variable.

## Vercel: what the human sets (nothing is deployed by this task)

- Project root directory: `web`. Framework preset: Vite (also written in `web/vercel.json`).
- Install `npm ci`, build `npm run build`, output `dist` (all in `web/vercel.json`).
- Production environment variables: `VITE_GATEWAY_URL=https://api.raftkv.tushar25.com` (**to be
  confirmed**) and optionally `VITE_REPO_URL`. Preview deployments: set `VITE_REPLAY_ONLY=true`
  (the gateway allows one origin and `*.vercel.app` is not it, so previews play the recording).
- Production dashboard origin: `https://raftkv.tushar25.com` (**to be confirmed**). That exact
  string must be the gateway's `--dashboard-origin` (scheme and host, no path, no wildcard).
- `web/vercel.json` sends `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`,
  `X-Frame-Options: DENY` and a CSP whose `connect-src` is `'self' https://api.raftkv.tushar25.com`
  (**to be confirmed**: change it together with `VITE_GATEWAY_URL` if the API host differs).
- An https dashboard cannot call an http gateway (mixed content): live production needs the
  gateway's TLS (D6: `--tls-domain`, see `docs/gateway.md`). A gateway without it, or not yet
  deployed, leaves the production page in replay.
- Linking the Vercel project and DNS are the human's (RUNBOOK D7 row, Appendix E).

## Pinned toolchain

`.nvmrc` = `24` and `engines` = `node >=24.10.0 <25`, `npm >=11` (this machine: Node v24.10.0,
npm 11.6.0). Every dependency is pinned to an exact version in `package.json` (no `^` ranges) and
`package-lock.json` is committed. Versions installed on 2026-11-17: react 19.3.1, react-dom 19.3.1,
vite 8.3.8, @vitejs/plugin-react 6.3.0, typescript 6.0.3, vitest 5.1.0, jsdom 29.1.1,
@testing-library/react 16.3.3, @testing-library/dom 10.4.2, @testing-library/user-event 14.6.12,
@testing-library/jest-dom 7.0.1, vitest-axe 0.1.0, axe-core 4.14.1, @biomejs/biome 2.6.8,
@types/node 24.x, @types/react 19.3.0, @types/react-dom 19.3.0.

## Why Biome and not ESLint (differs from the task card)

The card asked for ESLint (typescript-eslint, react-hooks, jsx-a11y). ESLint's dependency chain
`eslint -> file-entry-cache -> flat-cache -> flatted` ships
`web/node_modules/flatted/golang/pkg/flatted/flatted.go`. The repo-wide Go tooling descends into
`web/node_modules`: `gofmt -l .`, `go vet ./...`, `go list ./...` (it listed that file as package
`github.com/MenaceHecker/raftkv/web/node_modules/flatted/golang/pkg/flatted`), and the README
count walks in `internal/determinism/readme_test.go` and `internal/readmecounts` (they skip only
`.git`). So an ESLint install would change what those see. Fixing it means changing those walks,
which is a test change that needs a human, so the decision was to avoid the problem: lint and
format with Biome (a single native binary, no `.go` files). The a11y rule group is at error level,
plus `useExhaustiveDependencies`, `useHookAtTopLevel`, `noExplicitAny`, `noConsole` and others
(`biome.json`). Biome's static a11y rules are narrower than jsx-a11y; `vitest-axe` runs axe-core on
every panel at runtime to cover the rest.

**Do not reintroduce ESLint (or any dependency that ships Go files) without first changing those
walks. That is a human decision.** The guard: `npm run check:no-go` (`scripts/check-no-go.mjs`, a
plain Node script) exits 1 if any `*.go`, `go.mod`, `go.sum` or `.git` entry exists under
`web/node_modules`. It runs in the CI `web` job right after `npm ci`, and `scripts/check-no-go.test.ts`
proves it fails on a tree with a `.go` file and passes on one without. The root `.gitignore` ignores
`web/node_modules` and `web/dist`; `.dockerignore` ignores `web/`.

## What RUNBOOK 10.7 asks for that the API cannot show

Decided with the human (no Go change, no new gateway field):

- **Log strip coloured by term, per entry (panel 2).** The cluster document has no first or last
  index and no per-entry term, and the nodes have no entry-listing RPC. The log panel shows, per
  node, the snapshot index, applied index, commit index, log length and the lag behind the highest
  commit on one absolute index axis, coloured by the node's current term only, with an equivalent
  table. It says on the page that per-entry terms are not available.
- **Election started and vote granted (panel 3).** No node emits them (`docs/gateway.md`,
  `/v1/events`: "Not available: vote granted, election start, per-entry commit"). The timeline
  shows role changes (became candidate, became leader with the term), node up/down, snapshots,
  chaos and verify events, and says "votes are not reported by the nodes".
- **"Catch-up" (panel 3)** is derived in the browser (a node that came back up and whose
  `lastApplied` is within 2 of the leader's) and labelled derived.
- **Kill random (panel 4)** is "Kill a follower": the gateway only lets the public kill the leader,
  so killing a follower needs the admin token.
- **CAS (panel 5)** is shown disabled with the gateway's own explanation (the cluster has no
  compare-and-swap; a test pins the text to `internal/gateway/kv.go`).
- **Verifier progress (panel 6)** is elapsed time out of 60 s; operation counts are not reported
  while a run is in progress. "Latest report" is the JSON plus a link to `docs/CHAOS_RESULTS.md`.

## Gateway shapes, re-verified against main (differences from the card)

The card's JSON shapes were re-read against `internal/gateway` on `origin/main` 35b6c3d. They match.
Additions the card did not list, all handled: a 502 write answer can also be
`{"error":"cluster error","outcome":"unknown"}` (so `outcome` is read generically); the
verification-lock 503 carries `Retry-After: 5`; the report's `seed` is a JSON string; `faults` is
always an array. `chaos` and `paused` are in the cluster document although the endpoint table in
`docs/gateway.md` omits them (the code is the truth).

## Live mode and replay

- Polling: one `setTimeout` loop on `GET /v1/cluster`, 1 s visible and 5 s hidden, honours
  `Retry-After` on 429, never two requests in flight. Three consecutive failures switch to replay.
- Events: one native `EventSource` per tab (injected constructor), the eight event names, malformed
  JSON counted and dropped, ids de-duplicated, native reconnects left alone; a dead source
  (CLOSED, or three errors without an open) is recreated with jittered backoff from 5 s doubling to
  60 s and the page notes "events may be missing". Trouble with the stream alone never switches to
  replay. The stream is closed after 60 s hidden. One cluster refetch (debounced 250 ms) follows
  `state`, `node_up`, `node_down`, `chaos` and `resync`.
  A browser cannot read the status of a failed `EventSource` or set `Last-Event-ID` on a new one;
  so resume after a viewer-cap rejection is a cold start plus a refetch (OPEN QUESTION 5, accepted).
- Start: live if `VITE_GATEWAY_URL` is set, `VITE_REPLAY_ONLY` is not `true` and the URL has no
  `?replay=1`; the first `/v1/cluster` must answer within 3 s, else replay. In replay with a
  gateway configured, `/healthz` then `/v1/cluster` is probed every 30 s (jittered); success, or a
  429, returns to live.
- Recording `public/recording.jsonl` (same origin, so no AWS is needed), one JSON object per line,
  `v:1`: a header `{"v":1,"kind":"header","recordedAt":ISO,"durationMs":N,"synthetic":bool}`, then
  `{"t":ms,"kind":"cluster","data":<cluster doc>}`, `{"t":ms,"kind":"event","id":N,"name":..,"data":..}`
  (verbatim) and `{"t":ms,"kind":"verify","data":<report>}`. Unknown kinds are skipped, a bad line
  is skipped and counted, a file with no header is "no recording". Playback is 1x and loops.
- The committed recording is a small **synthetic** sample (`"synthetic": true`, built by
  `scripts/make-sample-recording.mjs`); the banner says so. The real capture of a scripted demo
  (kill leader, isolate, heal, one verify run) is D8 (human): `node scripts/record-session.mjs
  --gateway URL --out FILE --seconds N`. The recorder uses public access only, never sends an
  `Authorization` header, and refuses to write any line containing `Bearer`.

## Security and accessibility

- Admin token: a password input in a collapsed Admin section, kept only in this tab's session
  storage (try/catch around every access), never in the URL, a cookie, persistent storage, the
  console or the bundle; attached only to `POST /v1/chaos/*` and `POST /v1/verify/run`; a 401 clears
  it; `/v1/admin/reset` is not exposed. Static tests scan the source for each of these.
- No `innerHTML`; the counterexample is a text node in a `<pre>` (64 KB, then "Show all").
- Landmarks and a labelled section per panel; role and state in text and icon, not colour alone;
  `aria-disabled` with a visible reason so keyboards still find disabled buttons; 44 px targets;
  `prefers-reduced-motion`; light and dark tokens; `vitest-axe` on every panel; single column at
  640 px and below. jsdom has no layout engine, so the mobile layout is checked on the stylesheet
  and by the PR's demonstration, not by a rendered test.
