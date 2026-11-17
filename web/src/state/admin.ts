// The admin token lives in sessionStorage only: never localStorage, a cookie, the URL, the
// console or the bundle. Every access is wrapped because storage can be unavailable or throw.
export const TOKEN_KEY = "raftkv.admin";

export function getToken(): string | null {
  try {
    const t = sessionStorage.getItem(TOKEN_KEY);
    return t ? t : null;
  } catch {
    return null;
  }
}

export function setToken(token: string): void {
  const t = token.trim();
  if (t === "") return;
  try {
    sessionStorage.setItem(TOKEN_KEY, t);
  } catch {
    // storage unavailable: the token simply is not kept
  }
}

export function clearToken(): void {
  try {
    sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // nothing to clear
  }
}
