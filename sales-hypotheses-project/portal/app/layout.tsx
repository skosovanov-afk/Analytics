import type { ReactNode } from "react";
import "./globals.css";

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link rel="stylesheet" href="/global-inline.css" />
      </head>
      <body>
        <div className="shell">{children}</div>
      </body>
    </html>
  );
}

