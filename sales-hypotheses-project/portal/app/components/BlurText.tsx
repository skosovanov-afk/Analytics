"use client";
import { useEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";

export function BlurText({
  text = "",
  delay = 80,
  animateBy = "words",
  direction = "top",
  className,
  style,
}: {
  text: string;
  delay?: number;
  animateBy?: "words" | "letters";
  direction?: "top" | "bottom";
  className?: string;
  style?: CSSProperties;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [inView, setInView] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setInView(true);
          observer.disconnect();
        }
      },
      { threshold: 0.1 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const parts = animateBy === "words" ? text.split(" ") : text.split("");
  const yOffset = direction === "top" ? -14 : 14;

  return (
    <span
      ref={ref}
      className={className}
      style={{
        display: "inline-flex",
        flexWrap: "wrap",
        gap: animateBy === "words" ? "0.3em" : "0",
        ...style,
      }}
    >
      {parts.map((part, i) => (
        <span
          key={i}
          style={{
            display: "inline-block",
            opacity: inView ? 1 : 0,
            filter: inView ? "blur(0px)" : "blur(8px)",
            transform: inView ? "translateY(0)" : `translateY(${yOffset}px)`,
            transition: "opacity 0.45s ease, filter 0.45s ease, transform 0.45s ease",
            transitionDelay: `${i * delay}ms`,
            willChange: "opacity, filter, transform",
          }}
        >
          {part}
        </span>
      ))}
    </span>
  );
}
