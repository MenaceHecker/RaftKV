import { defineConfig } from "vitest/config";

// Not part of `npm test`: needs a running gateway (scripts/dev-gateway.sh) and
// CONTRACT_GATEWAY=<url>. It checks the hand-written guards against the real
// fake-cluster gateway so the mock cannot drift from internal/gateway.
export default defineConfig({
  test: { environment: "node", include: ["src/**/*.contract.test.ts"] },
});
