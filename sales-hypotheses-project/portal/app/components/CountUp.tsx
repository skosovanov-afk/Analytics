"use client";
import { useEffect, useRef, useState } from "react";
import type { CSSProperties } from "react";

export function CountUp({
  to,
  from = 0,
  duration = 1.4,
  separator = ",",
  className,
  style,
}: {
  to: number;
  from?: number;
  duration?: number;
  separator?: string;
  className?: string;
  style?: CSSProperties;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [started, setStarted] = useState(false);

  // Start animation when element scrolls into view
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setStarted(true);
          observer.disconnect();
        }
      },
      { threshold: 0.1 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!started) return;
    const el = ref.current;
    if (!el) return;

    const startTime = performance.now();
    const range = to - from;

    function fmt(n: number) {
      const rounded = Math.round(n);
      if (!separator) return String(rounded);
      return rounded.toLocaleString("en-US").replace(/,/g, separator);
    }

    function tick(now: number) {
      const t = Math.min((now - startTime) / (duration * 1000), 1);
      const eased = 1 - Math.pow(1 - t, 3); // ease-out cubic
      if (el) el.textContent = fmt(from + range * eased);
      if (t < 1) requestAnimationFrame(tick);
    }

    requestAnimationFrame(tick);
  }, [started, to, from, duration, separator]);

  function fmt(n: number) {
    if (!separator) return String(Math.round(n));
    return Math.round(n).toLocaleString("en-US").replace(/,/g, separator);
  }

  return (
    <span ref={ref} className={className} style={style}>
      {fmt(from)}
    </span>
  );
}
