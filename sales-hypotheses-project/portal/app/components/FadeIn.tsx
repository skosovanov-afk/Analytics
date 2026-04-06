"use client";
import { useEffect, useRef, useState } from "react";
import type { CSSProperties, ReactNode } from "react";

export function FadeIn({
  children,
  delay = 0,
  duration = 420,
  blur = true,
  translateY = 10,
  className,
  style,
}: {
  children: ReactNode;
  delay?: number;
  duration?: number;
  blur?: boolean;
  translateY?: number;
  className?: string;
  style?: CSSProperties;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true);
          observer.disconnect();
        }
      },
      { threshold: 0.05 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const transition = [
    `opacity ${duration}ms ease`,
    `transform ${duration}ms ease`,
    blur ? `filter ${duration}ms ease` : null,
  ]
    .filter(Boolean)
    .join(", ");

  return (
    <div
      ref={ref}
      className={className}
      style={{
        transition,
        transitionDelay: `${delay}ms`,
        opacity: visible ? 1 : 0,
        transform: visible ? "translateY(0)" : `translateY(${translateY}px)`,
        willChange: "opacity, transform",
        ...(blur ? { filter: visible ? "blur(0)" : "blur(6px)" } : {}),
        ...style,
      }}
    >
      {children}
    </div>
  );
}
