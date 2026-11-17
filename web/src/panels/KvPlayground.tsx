import type { Client } from "../api/client";
import type { Mode } from "../state/store";

export interface KvProps {
  client: Client;
  mode: Mode;
  verifying: boolean;
}
// STUB (D7 commit 1).
export function KvPlayground(_props: KvProps) {
  return <section aria-label="KV playground">todo</section>;
}
