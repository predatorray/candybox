// Inline SVG mark so the brand survives the no-bundle fallback and no extra HTTP roundtrip is
// needed. Mirrors the upstream logo at
// https://github.com/predatorray/candybox/blob/assets/logo.svg — four candies spilling out of an
// open box.
export function CandyLogo({ size = 50 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 240 240"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="candybox"
    >
      {/* Candies */}
      <circle cx="96" cy="118" r="22" fill="#FF5C8A" />
      <circle cx="146" cy="104" r="17" fill="#FFC247" />
      <circle cx="138" cy="146" r="15" fill="#4ECDC4" />
      <circle cx="104" cy="156" r="12" fill="#7C6CFF" />
      {/* Open box: square with top border missing */}
      <path
        d="M60 145 L60 180 L180 180 L180 90"
        fill="none"
        stroke="#3A6FF8"
        strokeWidth="14"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
