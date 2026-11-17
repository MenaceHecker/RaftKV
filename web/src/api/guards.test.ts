import { describe, expect, it } from "vitest";
import { cluster, report } from "../test/fixtures";
import {
  parseChaosAck,
  parseCluster,
  parseHeal,
  parseVerifyLast,
  parseVerifyReport,
} from "./guards";

describe("parseCluster", () => {
  it("accepts the documented document", () => {
    expect(parseCluster(cluster())).toEqual(cluster());
  });
  it("accepts a null lastContactMs and a down node", () => {
    const c = cluster();
    const n = c.nodes[1];
    if (!n) throw new Error("fixture");
    n.up = false;
    n.role = "unknown";
    n.lastContactMs = null;
    expect(parseCluster(c)?.nodes[1]?.lastContactMs).toBeNull();
  });
  it("accepts a chaos view with action, target and expiresAt", () => {
    const c = cluster({
      chaos: { state: "active", action: "kill", target: 1, expiresAt: "2026-11-01T10:00:45Z" },
    });
    expect(parseCluster(c)?.chaos.expiresAt).toBe("2026-11-01T10:00:45Z");
  });
  it("tolerates extra fields", () => {
    const c = { ...cluster(), future: 1 } as unknown;
    expect(parseCluster(c)).not.toBeNull();
  });
  it("rejects missing fields", () => {
    const { nodes: _n, ...noNodes } = cluster();
    expect(parseCluster(noNodes)).toBeNull();
    const { chaos: _c, ...noChaos } = cluster();
    expect(parseCluster(noChaos)).toBeNull();
    const c = cluster();
    // biome-ignore lint/suspicious/noExplicitAny: building a bad document on purpose
    delete (c.nodes[0] as any).term;
    expect(parseCluster(c)).toBeNull();
    expect(parseCluster(null)).toBeNull();
    expect(parseCluster("x")).toBeNull();
  });
  it("rejects wrong types", () => {
    const c = cluster();
    // biome-ignore lint/suspicious/noExplicitAny: building a bad document on purpose
    (c.nodes[0] as any).commitIndex = "12";
    expect(parseCluster(c)).toBeNull();
  });
});

describe("parseVerifyReport / parseVerifyLast", () => {
  it("accepts a report with extras and a counterexample", () => {
    const r = { ...report({ verdict: "not_linearizable", counterexample: "x" }), extra: true };
    expect(parseVerifyReport(r)?.counterexample).toBe("x");
  });
  it("rejects a report missing verdict or with a bad status", () => {
    const { verdict: _v, ...noVerdict } = report();
    expect(parseVerifyReport(noVerdict)).toBeNull();
    expect(parseVerifyReport({ ...report(), status: "weird" })).toBeNull();
  });
  it("parseVerifyLast takes a running marker or a report", () => {
    expect(
      parseVerifyLast({ id: "v1", status: "running", startedAt: "2026-11-01T10:00:00Z" }),
    ).toEqual({
      id: "v1",
      status: "running",
      startedAt: "2026-11-01T10:00:00Z",
    });
    expect(parseVerifyLast(report())?.status).toBe("done");
    expect(parseVerifyLast({ status: "running" })).toBeNull();
  });
});

describe("parseChaosAck / parseHeal", () => {
  it("accept the documented bodies", () => {
    const ack = {
      action: "kill",
      target: 1,
      expiresAt: "2026-11-01T10:00:45Z",
      autoHealSeconds: 45,
    };
    expect(parseChaosAck(ack)).toEqual(ack);
    expect(parseHeal({ healed: [1, 2], failed: [], state: "idle" })).toEqual({
      healed: [1, 2],
      failed: [],
      state: "idle",
    });
  });
  it("reject missing fields", () => {
    expect(parseChaosAck({ action: "kill" })).toBeNull();
    expect(parseHeal({ healed: [1] })).toBeNull();
  });
});
