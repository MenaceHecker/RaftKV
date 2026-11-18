import { useState } from "react";
import { AdminToken } from "./panels/AdminToken";
import { ClusterView } from "./panels/ClusterView";
import { Controls } from "./panels/Controls";
import { KvPlayground } from "./panels/KvPlayground";
import { LogView } from "./panels/LogView";
import { ModeBanner } from "./panels/ModeBanner";
import { Timeline } from "./panels/Timeline";
import { Verifier } from "./panels/Verifier";
import { getToken } from "./state/admin";
import type { Runtime } from "./state/runtime";
import { useStore } from "./state/useStore";

export function App({ runtime, repoUrl = "" }: { runtime: Runtime; repoUrl?: string }) {
  const s = useStore(runtime.store);
  const [hasToken, setHasToken] = useState(() => getToken() !== null);
  const [ourRun, setOurRun] = useState(false);
  const verifying = s.verifyRunning || ourRun;
  const verifyRows = s.timeline.rows.filter((r) => r.kind === "verify");

  return (
    <div className="app">
      <header className="top">
        <h1>RaftKV</h1>
        <p className="muted">A Raft key-value store, three nodes in three availability zones.</p>
      </header>
      <ModeBanner state={s} />
      <main className="grid">
        <ClusterView cluster={s.cluster} />
        <LogView cluster={s.cluster} />
        <Timeline rows={s.timeline.rows} />
        <Controls
          client={runtime.client}
          cluster={s.cluster}
          mode={s.mode}
          verifying={verifying}
          lastFaultAt={s.lastFaultAt}
          hasToken={hasToken}
        />
        <KvPlayground client={runtime.client} mode={s.mode} verifying={verifying} />
        <AdminToken onChange={setHasToken} />
        <Verifier
          client={runtime.client}
          mode={s.mode}
          recorded={s.verify}
          events={verifyRows}
          repoUrl={repoUrl}
          onRunningChange={setOurRun}
        />
      </main>
    </div>
  );
}
