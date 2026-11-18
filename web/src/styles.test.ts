import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const css = readFileSync(join(process.cwd(), "src", "styles.css"), "utf8");

// jsdom has no layout engine, so the mobile layout is checked on the stylesheet itself
// and by the screenshot/real-gateway demonstration in the PR.
describe("styles.css", () => {
  it("defines tokens on :root and a dark scheme", () => {
    expect(css).toMatch(/:root\s*{[^}]*--/);
    expect(css).toMatch(/prefers-color-scheme:\s*dark/);
  });
  it("collapses to one column at 640 px and never forces a wide page", () => {
    const m = css.match(/@media\s*\(max-width:\s*640px\)\s*{([\s\S]*?)\n}/);
    expect(m).not.toBeNull();
    expect(m?.[1]).toMatch(/grid-template-columns:\s*1fr/);
    expect(css).not.toMatch(/min-width:\s*(3[7-9]\d|[4-9]\d\d)px/);
    expect(css).toMatch(/overflow-x:\s*(auto|hidden)/);
  });
  it("respects reduced motion and has a visible focus style", () => {
    expect(css).toMatch(/prefers-reduced-motion:\s*reduce/);
    expect(css).toMatch(/:focus-visible/);
  });
  it("makes buttons at least 44 px tall", () => {
    expect(css).toMatch(/min-height:\s*44px/);
  });
});
