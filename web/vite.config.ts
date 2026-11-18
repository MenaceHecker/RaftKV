import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The dev proxy is the simplest way to run against a gateway without CORS:
// GATEWAY_PROXY=http://127.0.0.1:8080 VITE_GATEWAY_URL=/api npm run dev
const proxy = process.env.GATEWAY_PROXY;

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: proxy
      ? { "/api": { target: proxy, changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, "") } }
      : undefined,
  },
  preview: { port: 4173 },
  build: { target: "es2022", sourcemap: false },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}", "scripts/**/*.test.ts"],
    exclude: ["src/**/*.contract.test.ts"],
  },
});
