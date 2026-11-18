import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { getToken, TOKEN_KEY } from "../state/admin";
import { AdminToken } from "./AdminToken";

describe("AdminToken panel", () => {
  it("is a collapsed Admin section with a password input that does not autocomplete", () => {
    render(<AdminToken />);
    const details = screen.getByText(/^admin$/i).closest("details");
    expect(details).not.toBeNull();
    expect(details).not.toHaveAttribute("open");
    const input = screen.getByLabelText(/admin token/i);
    expect(input).toHaveAttribute("type", "password");
    expect(input).toHaveAttribute("autocomplete", "off");
  });

  it("saves to sessionStorage only, never echoes the token, and clears", async () => {
    const onChange = vi.fn();
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});
    const { container } = render(<AdminToken onChange={onChange} />);
    const u = userEvent.setup();
    await u.type(screen.getByLabelText(/admin token/i), "tok-abc-123");
    await u.click(screen.getByRole("button", { name: /^use token/i }));
    expect(sessionStorage.getItem(TOKEN_KEY)).toBe("tok-abc-123");
    expect(localStorage.length).toBe(0);
    expect(onChange).toHaveBeenLastCalledWith(true);
    expect(container.textContent).not.toContain("tok-abc-123");
    expect(JSON.stringify(spy.mock.calls)).not.toContain("tok-abc-123");
    await u.click(screen.getByRole("button", { name: /clear token/i }));
    expect(getToken()).toBeNull();
    expect(onChange).toHaveBeenLastCalledWith(false);
  });
});
