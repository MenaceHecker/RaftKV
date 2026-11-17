import "vitest";
import type { AxeMatchers } from "vitest-axe/matchers";

declare module "vitest" {
  // biome-ignore lint/suspicious/noExplicitAny: matches the vitest-axe type augmentation
  interface Assertion<T = any> extends AxeMatchers {}
  interface AsymmetricMatchersContaining extends AxeMatchers {}
}
