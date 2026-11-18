// The admin token lives in this tab's session storage only: no persistent storage, cookie, URL,
// console output or bundle ever holds it. Every access is wrapped because storage can be unavailable or throw.
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
