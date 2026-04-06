"use client";
import { useRef, type ReactNode, type CSSProperties } from "react";

export function SpotlightCard({
  children,
  className,
  style,
  onClick,
}: {
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
  onClick?: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  function onMouseMove(e: React.MouseEvent<HTMLDivElement>) {
    const el = ref.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    el.style.setProperty("--mx", `${x}px`);
    el.style.setProperty("--my", `${y}px`);
    el.style.setProperty("--opacity", "1");
  }

  function onMouseLeave() {
    const el = ref.current;
    if (!el) return;
    el.style.setProperty("--opacity", "0");
  }

  return (
    <div
      ref={ref}
      className={`spotlightCard${className ? ` ${className}` : ""}`}
      style={style}
      onMouseMove={onMouseMove}
      onMouseLeave={onMouseLeave}
      onClick={onClick}
    >
      {children}
    </div>
  );
}
