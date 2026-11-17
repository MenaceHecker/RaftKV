import { useState } from "react";
import { clearToken, getToken, setToken } from "../state/admin";

/** Collapsed Admin section. The token is never echoed, logged or stored anywhere but by state/admin.ts. */
export function AdminToken({ onChange }: { onChange?: (has: boolean) => void }) {
  const [draft, setDraft] = useState("");
  const [has, setHas] = useState(() => getToken() !== null);

  const use = () => {
    setToken(draft);
    setDraft("");
    const now = getToken() !== null;
    setHas(now);
    onChange?.(now);
  };
  const clear = () => {
    clearToken();
    setHas(false);
    onChange?.(false);
  };

  return (
    <section className="panel" aria-label="Admin">
      <details>
        <summary>Admin</summary>
        <p className="caption">
          Optional. With the admin token you can kill a follower and skip the cooldowns. It is kept
          in this tab's session storage only and is sent only with fault and verification-run
          requests.
        </p>
        <div className="field">
          <label htmlFor="admin-token">Admin token</label>
          <input
            id="admin-token"
            type="password"
            autoComplete="off"
            spellCheck={false}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
          />
        </div>
        <div className="btn-row">
          <button type="button" onClick={use}>
            Use token
          </button>
          <button type="button" className="secondary" onClick={clear}>
            Clear token
          </button>
        </div>
        <p className="status" role="status">
          {has ? "A token is set for this tab." : "No token set."}
        </p>
      </details>
    </section>
  );
}
