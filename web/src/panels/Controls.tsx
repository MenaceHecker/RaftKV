import type { Client } from "../api/client";
import type { Cluster } from "../api/types";
import type { Mode } from "../state/store";

export interface ControlsProps {
  client: Client;
  cluster: Cluster | null;
  mode: Mode;
  verifying: boolean;
  lastFaultAt: number | null;
  hasToken: boolean;
}
// STUB (D7 commit 1).
export function Controls(_props: ControlsProps) {
  return <section aria-label="Controls">todo</section>;
}
