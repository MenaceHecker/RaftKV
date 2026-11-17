import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, expect, vi } from "vitest";
import * as axeMatchers from "vitest-axe/matchers";

expect.extend(axeMatchers);

// Testing Library only advances fake timers for you when it sees a `jest` global; without
// this shim its async wrapper waits on a fake setTimeout that never fires under vi.useFakeTimers().
(globalThis as unknown as { jest: { advanceTimersByTime: (ms: number) => void } }).jest = {
  advanceTimersByTime: (ms) => {
    vi.advanceTimersByTime(ms);
  },
};

afterEach(() => {
  cleanup();
  try {
    sessionStorage.clear();
    localStorage.clear();
  } catch {
    // storage may be unavailable
  }
});
