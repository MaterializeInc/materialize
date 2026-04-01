// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import markSrc from "~/img/materialize-mark-grayscale.svg";

/** Spawn confetti particles from a point on screen */
export function triggerConfetti() {
  const colors = ["#7B61FF", "#00C853", "#FF6D00", "#00B0FF", "#FF1744"];
  const container = document.createElement("div");
  container.style.cssText =
    "position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:99999;overflow:hidden";
  document.body.appendChild(container);

  for (let i = 0; i < 80; i++) {
    const particle = document.createElement("div");
    const color = colors[Math.floor(Math.random() * colors.length)];
    const x = 40 + Math.random() * 20; // start from middle-ish
    const drift = (Math.random() - 0.5) * 80;
    const size = 4 + Math.random() * 6;
    const duration = 1.5 + Math.random() * 1.5;
    const delay = Math.random() * 0.3;
    const rotation = Math.random() * 720 - 360;

    particle.style.cssText = `
      position:absolute;
      left:${x}%;
      top:-10px;
      width:${size}px;
      height:${size * (Math.random() > 0.5 ? 1 : 2.5)}px;
      background:${color};
      border-radius:${Math.random() > 0.5 ? "50%" : "1px"};
      opacity:1;
      animation:confetti-fall ${duration}s ease-in ${delay}s forwards;
      --drift:${drift}vw;
      --rotation:${rotation}deg;
    `;
    container.appendChild(particle);
  }

  setTimeout(() => container.remove(), 4000);
}

// Inject the confetti keyframes once
const confettiStyle = document.createElement("style");
confettiStyle.textContent = `
  @keyframes confetti-fall {
    0% { transform: translateX(0) rotate(0deg); opacity: 1; }
    100% { transform: translateX(var(--drift)) rotate(var(--rotation)); top: 110vh; opacity: 0; }
  }
`;
document.head.appendChild(confettiStyle);

/** Start a cursor trail of tiny MZ logos */
export function startCursorTrail(): () => void {
  const handleMouseMove = (e: MouseEvent) => {
    const img = document.createElement("img");
    img.src = markSrc;
    img.style.cssText = `
      position:fixed;
      left:${e.clientX}px;
      top:${e.clientY}px;
      width:16px;
      height:16px;
      pointer-events:none;
      z-index:99998;
      opacity:0.7;
      transition:all 1s ease-out;
      transform:rotate(180deg) scale(1);
    `;
    document.body.appendChild(img);

    requestAnimationFrame(() => {
      img.style.opacity = "0";
      img.style.transform = `rotate(${180 + Math.random() * 360}deg) scale(0.2)`;
      img.style.top = `${e.clientY + 30 + Math.random() * 20}px`;
    });

    setTimeout(() => img.remove(), 1000);
  };

  // Throttle to every ~60ms to avoid flooding the DOM
  let lastTime = 0;
  const throttled = (e: MouseEvent) => {
    const now = Date.now();
    if (now - lastTime < 60) return;
    lastTime = now;
    handleMouseMove(e);
  };

  document.addEventListener("mousemove", throttled);
  return () => document.removeEventListener("mousemove", throttled);
}
