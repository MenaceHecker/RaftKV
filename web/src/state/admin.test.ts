import { afterEach, describe, expect, it, vi } from "vitest";
import { clearToken, getToken, setToken, TOKEN_KEY } from "./admin";

afterEach(() => vi.restoreAllMocks());

describe("admin token store", () => {
  it("lives in sessionStorage under one key and nowhere else", () => {
    setToken("s3cret-test-token");
    expect(sessionStorage.getItem(TOKEN_KEY)).toBe("s3cret-test-token");
    expect(getToken()).toBe("s3cret-test-token");
    expect(localStorage.length).toBe(0);
    expect(document.cookie).toBe("");
    expect(window.location.href).not.toContain("s3cret");
    expect(sessionStorage.length).toBe(1);
  });

  it("clearToken removes it", () => {
    setToken("t");
    clearToken();
    expect(getToken()).toBeNull();
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
  });

  it("an empty token is not stored", () => {
    setToken("   ");
    expect(getToken()).toBeNull();
  });

  it("survives storage that throws", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("denied");
    });
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    expect(() => setToken("t")).not.toThrow();
    expect(getToken()).toBeNull();
    expect(() => clearToken()).not.toThrow();
  });

  it("never writes the token to the console", () => {
    const spies = (["log", "info", "warn", "error", "debug"] as const).map((m) =>
      vi.spyOn(console, m).mockImplementation(() => {}),
    );
    setToken("s3cret-console");
    getToken();
    clearToken();
    for (const s of spies) expect(JSON.stringify(s.mock.calls)).not.toContain("s3cret-console");
  });
});
