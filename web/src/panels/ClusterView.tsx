import type { Cluster, NodeView } from "../api/types";

const STALE_MS = 5000; // a node not polled successfully for this long is drawn stale

function roleIcon(n: NodeView): string {
  if (!n.up) return "✕";
  switch (n.role) {
    case "leader":
      return "★";
    case "candidate":
      return "◐";
    default:
      return "●";
  }
}

function NodeCard({ n }: { n: NodeView }) {
  const stale = n.up && n.lastContactMs !== null && n.lastContactMs > STALE_MS;
  return (
    <article
      className={`node role-${n.up ? n.role : "down"}${stale ? " is-stale" : ""}`}
      aria-label={`Node ${n.id}`}
    >
      <h4>
        <span aria-hidden="true">{roleIcon(n)}</span> Node {n.id}
      </h4>
      <p className="role">{n.up ? n.role : "down"}</p>
      {n.up ? (
        <p className="nums">
          <span>term {n.term}</span> <span>commit {n.commitIndex}</span>{" "}
          <span>applied {n.lastApplied}</span>
        </p>
      ) : (
        <p className="nums muted">no answer from this node</p>
      )}
      {n.paused ? <p className="flag">paused</p> : null}
      {stale ? (
        <p className="flag">
          stale (last contact {Math.round((n.lastContactMs ?? 0) / 1000)} s ago)
        </p>
      ) : null}
    </article>
  );
}

/** Panel 1: three availability-zone lanes. Every figure is a field of /v1/cluster. */
export function ClusterView({ cluster }: { cluster: Cluster | null }) {
  const lanes = new Map<string, NodeView[]>();
  for (const n of cluster?.nodes ?? []) lanes.set(n.az, [...(lanes.get(n.az) ?? []), n]);
  const azs = [...lanes.keys()].sort();
  return (
    <section className="panel" aria-labelledby="h-cluster">
      <h2 id="h-cluster">Cluster</h2>
      {cluster === null ? (
        <p>Waiting for the cluster…</p>
      ) : (
        <>
          <p className="summary">
            {cluster.leaderId === 0 ? "No leader right now." : `Leader: node ${cluster.leaderId}.`}
          </p>
          <div className="lanes">
            {azs.map((az) => (
              <fieldset className="lane" key={az}>
                <legend>{az}</legend>
                {(lanes.get(az) ?? []).map((n) => (
                  <NodeCard n={n} key={n.id} />
                ))}
              </fieldset>
            ))}
          </div>
        </>
      )}
    </section>
  );
}
