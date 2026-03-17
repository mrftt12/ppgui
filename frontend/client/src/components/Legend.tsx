export function Legend() {
  return (
    <div className="absolute bottom-6 right-6 z-40 rounded-lg border border-border bg-card/90 px-3 py-2 text-xs text-foreground shadow-lg backdrop-blur">
      <div className="flex flex-col gap-3">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            Bus Voltage (p.u.)
          </div>
          <div className="mt-1 flex flex-col gap-1">
            <div
              className="h-2 w-[140px] rounded-sm"
              style={{
                background:
                  "linear-gradient(to right, #ef4444 0%, #ffffff 50%, #ef4444 100%)",
              }}
            />
            <div className="flex justify-between text-[10px] text-muted-foreground">
              <span>&lt;0.95</span>
              <span>1.0</span>
              <span>&gt;1.05</span>
            </div>
          </div>
        </div>
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            Line Loading (%)
          </div>
          <div className="mt-1 flex flex-col gap-1">
            <div
              className="h-2 w-[140px] rounded-sm"
              style={{
                background:
                  "linear-gradient(to right, #00d26a 0%, #f59e0b 80%, #ef4444 100%)",
              }}
            />
            <div className="flex justify-between text-[10px] text-muted-foreground">
              <span>0%</span>
              <span>80%</span>
              <span>100%</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
