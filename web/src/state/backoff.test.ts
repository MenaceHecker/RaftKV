import { describe, expect, it } from "vitest";
import { reconnectDelay } from "./backoff";

describe("reconnectDelay", () => {
  it("starts at the 5 s floor and doubles to the 60 s cap (rand = 0)", () => {
    const d = [0, 1, 2, 3, 4, 5, 9].map((n) => reconnectDelay(n, () => 0));
    expect(d).toEqual([5000, 10000, 20000, 40000, 60000, 60000, 60000]);
  });
  it("adds jitter but never goes below the floor or above the cap", () => {
    for (let n = 0; n < 8; n++) {
      const lo = reconnectDelay(n, () => 0);
      const hi = reconnectDelay(n, () => 0.999999);
      expect(hi).toBeGreaterThanOrEqual(lo);
      expect(hi).toBeLessThanOrEqual(60000);
      expect(lo).toBeGreaterThanOrEqual(5000);
    }
    expect(reconnectDelay(0, () => 0.999999)).toBeGreaterThan(5000);
  });
});
