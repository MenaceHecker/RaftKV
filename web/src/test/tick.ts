import { act } from "@testing-library/react";
import { vi } from "vitest";

/**
 * Advance fake timers in small steps, each inside act(), so React flushes state and re-arms
 * effect timers between steps (one big act() would only flush when it ends).
 */
export async function tick(ms: number, step = 250): Promise<void> {
  if (ms <= 0) {
    await act(() => vi.advanceTimersByTimeAsync(0));
    return;
  }
  let left = ms;
  while (left > 0) {
    const d = Math.min(step, left);
    await act(() => vi.advanceTimersByTimeAsync(d));
    left -= d;
  }
}
