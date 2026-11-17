import type { ChaosAck, Cluster, HealResult, VerifyLast, VerifyReport } from "./types";

// STUB (D7 commit 1): real parsers arrive in the client/guards commit.
export function parseCluster(_v: unknown): Cluster | null {
  return null;
}
export function parseVerifyReport(_v: unknown): VerifyReport | null {
  return null;
}
export function parseVerifyLast(_v: unknown): VerifyLast | null {
  return null;
}
export function parseChaosAck(_v: unknown): ChaosAck | null {
  return null;
}
export function parseHeal(_v: unknown): HealResult | null {
  return null;
}
