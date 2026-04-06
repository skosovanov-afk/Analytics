"use client";

export type ThreadMessage = {
  id: string;
  direction: "in" | "out";
  body: string | null;
  timestamp: string;
  from?: string | null;
  to?: string | null;
  subject?: string | null;
};

function formatTs(s: string) {
  if (!s) return "";
  const d = new Date(s);
  return d.toLocaleString("ru-RU", {
    day: "2-digit", month: "2-digit", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

export function ThreadView({
  messages,
  loading,
  error,
}: {
  messages: ThreadMessage[];
  loading: boolean;
  error: string | null;
}) {
  if (loading) {
    return (
      <div style={{ padding: "20px 24px", color: "var(--muted2)", fontSize: 13 }}>
        Loading thread...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: "20px 24px", color: "#f87171", fontSize: 13 }}>
        {error}
      </div>
    );
  }

  if (!messages.length) {
    return (
      <div style={{ padding: "20px 24px", color: "var(--muted2)", fontSize: 13 }}>
        No messages found
      </div>
    );
  }

  return (
    <div style={{
      padding: "16px 24px",
      maxHeight: 420,
      overflowY: "auto",
      display: "flex",
      flexDirection: "column",
      gap: 12,
    }}>
      {messages.map((m) => {
        const isIn = m.direction === "in";
        return (
          <div
            key={m.id}
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: isIn ? "flex-start" : "flex-end",
              maxWidth: "85%",
              alignSelf: isIn ? "flex-start" : "flex-end",
            }}
          >
            <div style={{
              display: "flex",
              gap: 8,
              alignItems: "center",
              marginBottom: 4,
            }}>
              <span style={{
                fontSize: 10,
                fontWeight: 700,
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                color: isIn ? "#a78bfa" : "#22c55e",
              }}>
                {isIn ? "Lead" : "Sent"}
              </span>
              <span style={{ fontSize: 11, color: "var(--muted2)" }}>
                {formatTs(m.timestamp)}
              </span>
              {m.from && (
                <span style={{ fontSize: 11, color: "var(--muted2)" }}>
                  {m.from}
                </span>
              )}
            </div>
            {m.subject && (
              <div style={{
                fontSize: 11,
                color: "var(--muted2)",
                marginBottom: 4,
                fontStyle: "italic",
              }}>
                Re: {m.subject}
              </div>
            )}
            <div style={{
              background: isIn ? "rgba(167, 139, 250, 0.08)" : "rgba(34, 197, 94, 0.06)",
              border: `1px solid ${isIn ? "rgba(167, 139, 250, 0.15)" : "rgba(34, 197, 94, 0.12)"}`,
              borderRadius: 8,
              padding: "10px 14px",
              fontSize: 13,
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              color: "var(--text)",
            }}>
              {m.body || (
                <span style={{ color: "var(--muted2)", fontStyle: "italic" }}>
                  (message text unavailable)
                </span>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
