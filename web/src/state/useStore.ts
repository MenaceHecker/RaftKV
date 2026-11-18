import { useSyncExternalStore } from "react";
import type { AppState, Store } from "./store";

export function useStore(store: Store): AppState {
  return useSyncExternalStore(store.subscribe, store.get);
}
