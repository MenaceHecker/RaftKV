import type { Cluster, NodeView } from "../api/types";

function hue(term: number): string {
  return `hsl(${(term * 47) % 360} 55% 45%)`; // the node's current term only
}

function Bar({ n, max }: { n: NodeView; max: number }) {
  if (!n.up) {
    return (
      <div className="bar" role="img" aria-label={`Node ${n.id}: down`}>
        <span className="muted">node {n.id}: down</span>
      </div>
    );
  }
  const pct = (v: number) => `${max === 0 ? 0 : Math.min(100, (v / max) * 100)}%`;
  const left = pct(n.snapshotIndex);
  const width = `${max === 0 ? 0 : Math.max(0, ((n.commitIndex - n.snapshotIndex) / max) * 100)}%`;
  return (
    <div
      className="bar"
      role="img"
      aria-label={`Node ${n.id}: snapshot ${n.snapshotIndex}, applied ${n.lastApplied}, commit ${n.commitIndex}, log length ${n.logLen}`}
    >
      <span className="bar-label" aria-hidden="true">
        node {n.id}
      </span>
      <span className="track" aria-hidden="true">
        <span className="fill" style={{ left, width, background: hue(n.term) }} />
        <span className="marker snapshot" style={{ left: pct(n.snapshotIndex) }} title="snapshot" />
        <span className="marker applied" style={{ left: pct(n.lastApplied) }} title="applied" />
        <span className="marker commit" style={{ left: pct(n.commitIndex) }} title="commit" />
      </span>
    </div>
  );
}

/** Panel 2: per-node markers on one absolute index axis. No log tail, no per-entry terms. */
export function LogView({ cluster }: { cluster: Cluster | null }) {
  const nodes = cluster?.nodes ?? [];
  const up = nodes.filter((n) => n.up);
  const max = Math.max(0, ...up.flatMap((n) => [n.commitIndex, n.lastApplied, n.snapshotIndex]));
  const highestCommit = Math.max(0, ...up.map((n) => n.commitIndex));
  return (
    <section className="panel" aria-labelledby="h-log">
      <h2 id="h-log">Log</h2>
      <p className="caption">
        Markers: snapshot index, applied index and commit index on one absolute index axis. Bars are
        coloured by the node's current term only.
      </p>
      <p className="caption">
        The gateway does not report per-entry terms or the log tail, so those cannot be shown.
      </p>
      {nodes.length === 0 ? (
        <p>Waiting for the cluster…</p>
      ) : (
        <>
          <div className="bars">
            {nodes.map((n) => (
              <Bar n={n} max={max} key={n.id} />
            ))}
          </div>
          <div className="table-wrap">
            <table>
              <caption>Log markers per node, with lag behind the highest commit index</caption>
              <thead>
                <tr>
                  <th scope="col">Node</th>
                  <th scope="col">Snapshot index</th>
                  <th scope="col">Applied</th>
                  <th scope="col">Commit</th>
                  <th scope="col">Log length</th>
                  <th scope="col">Commit lag</th>
                  <th scope="col">Applied lag</th>
                </tr>
              </thead>
              <tbody>
                {nodes.map((n) => (
                  <tr key={n.id} aria-label={`Node ${n.id}`}>
                    <td>{n.id}</td>
                    {n.up ? (
                      <>
                        <td>{n.snapshotIndex}</td>
                        <td>{n.lastApplied}</td>
                        <td>{n.commitIndex}</td>
                        <td>{n.logLen}</td>
                        <td>{highestCommit - n.commitIndex}</td>
                        <td>{highestCommit - n.lastApplied}</td>
                      </>
                    ) : (
                      <td colSpan={6}>down</td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
