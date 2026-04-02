export default function LoadingSkeleton({ rows = 4, className = '' }) {
  return (
    <div className={`space-y-3 ${className}`} aria-busy="true" aria-label="Loading">
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="h-8 bg-surface-border rounded animate-pulse"
          style={{ width: `${75 + (i % 3) * 10}%`, opacity: 1 - i * 0.15 }}
        />
      ))}
    </div>
  )
}
