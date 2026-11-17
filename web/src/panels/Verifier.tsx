import type { Client } from "../api/client";
import type { VerifyLast } from "../api/types";
import type { Mode } from "../state/store";
import type { TimelineRow } from "../state/timeline";

export interface VerifierProps {
  client: Client;
  mode: Mode;
  recorded: VerifyLast | null;
  events: TimelineRow[]; // the `verify` rows, for fault progress
  repoUrl: string;
  onRunningChange: (running: boolean) => void;
}
// STUB (D7 commit 1).
export function Verifier(_props: VerifierProps) {
  return <section aria-label="Verifier">todo</section>;
}
