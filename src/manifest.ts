import { defineManifest } from "@absolutejs/manifest";
import { Type } from "@sinclair/typebox";

export const manifest = defineManifest<Record<string, never>>()({
  contract: 2,
  identity: {
    accent: "#f97316",
    category: "ai",
    description:
      "Crash-safe external effects for AI agents. Persists intent and an outbox atomically, certifies provider adapters, installs tenant-scoped authority without credential values, resolves exact aliases only after execution-time authorization, schedules through @absolutejs/queue, quarantines unknown outcomes, and provides evidence-verified tenant-fenced reconciliation with append-only operator history.",
    docsUrl: "https://github.com/absolutejs/execution",
    name: "@absolutejs/execution",
    tagline: "Make agent side effects recoverable and auditable.",
  },
  settings: Type.Object({}),
  wiring: [],
});
