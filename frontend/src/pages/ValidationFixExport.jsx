import { useState, useEffect, useCallback } from "react";

// ─── colour tokens ──────────────────────────────────────────────────────────
const C = {
  bg: "#0d0f14",
  surface: "#13161e",
  card: "#191d28",
  border: "#252a38",
  accent: "#4f8ef7",
  accentHo: "#6ba3ff",
  green: "#22c55e",
  red: "#ef4444",
  yellow: "#f59e0b",
  muted: "#6b7280",
  text: "#e8eaf0",
  textDim: "#9ca3af",
};

const SEV_COLOR = { critical: C.red, warning: C.yellow, info: C.accent };
const STATUS_COLOR = { passed: C.green, failed: C.red, warning: C.yellow, error: C.red };

// ─── tiny helpers ───────────────────────────────────────────────────────────
const Badge = ({ label, color }) => (
  <span style={{
    background: color + "22", color, border: `1px solid ${color}44`,
    borderRadius: 4, padding: "2px 8px", fontSize: 11, fontWeight: 600,
    letterSpacing: ".4px", textTransform: "uppercase", fontFamily: "monospace",
  }}>{label}</span>
);

const Button = ({ children, onClick, disabled, variant = "primary", small }) => {
  const isPrimary = variant === "primary";
  const isGhost = variant === "ghost";
  return (
    <button onClick={onClick} disabled={disabled} style={{
      background: isPrimary ? C.accent : isGhost ? "transparent" : C.surface,
      color: isPrimary ? "#fff" : C.text,
      border: isGhost ? `1px solid ${C.border}` : "none",
      borderRadius: 6, cursor: disabled ? "not-allowed" : "pointer",
      padding: small ? "5px 12px" : "8px 18px",
      fontSize: small ? 12 : 13, fontWeight: 600,
      opacity: disabled ? .45 : 1,
      transition: "all .15s", fontFamily: "inherit",
    }}>{children}</button>
  );
};

const Spinner = () => (
  <span style={{
    display: "inline-block", width: 14, height: 14,
    border: `2px solid ${C.border}`, borderTopColor: C.accent,
    borderRadius: "50%", animation: "spin .7s linear infinite",
  }} />
);

// ─── main component ──────────────────────────────────────────────────────────
export default function ValidationFixExport({ validationId, apiBase = "/api/v1" }) {
  const [fixes, setFixes] = useState(null);       // quick-fix data from API
  const [loading, setLoading] = useState(true);
  const [fixInstr, setFixInstr] = useState({});          // rule_name → instruction string
  const [useAgent, setUseAgent] = useState(true);
  const [applying, setApplying] = useState(false);
  const [fixResult, setFixResult] = useState(null);
  const [exporting, setExporting] = useState(false);
  const [exportFmt, setExportFmt] = useState("csv");
  const [tab, setTab] = useState("fixes");     // "fixes" | "preview"
  const [expandedRow, setExpandedRow] = useState(null);

  // ── fetch quick fixes ──
  const load = useCallback(async () => {
    if (!validationId) return;
    setLoading(true);
    try {
      const res = await fetch(`${apiBase}/validate/${validationId}/quick-fixes`);
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setFixes(data);
      // Pre-select first suggested fix for each failed rule
      const defaults = {};
      (data.fixes || []).forEach(f => {
        if (f.status !== "passed" && f.suggested_fixes?.length) {
          defaults[f.rule_name] = f.suggested_fixes[0].instruction;
        } else {
          defaults[f.rule_name] = "keep";
        }
      });
      setFixInstr(defaults);
    } catch (e) {
      console.error("Failed to load quick fixes:", e);
    } finally {
      setLoading(false);
    }
  }, [validationId, apiBase]);

  useEffect(() => { load(); }, [load]);

  // ── apply fixes ──
  const applyFixes = async () => {
    setApplying(true);
    setFixResult(null);
    try {
      const instructions = Object.entries(fixInstr).map(([rule_name, instruction]) => ({
        rule_name, instruction
      }));
      const res = await fetch(`${apiBase}/validate/${validationId}/fix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fix_instructions: instructions, use_agent: useAgent }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setFixResult(data);
      setTab("preview");
    } catch (e) {
      alert("Fix failed: " + e.message);
    } finally {
      setApplying(false);
    }
  };

  // ── export ──
  const exportData = async (useFixed) => {
    setExporting(true);
    try {
      const url = `${apiBase}/validate/${validationId}/export?format=${exportFmt}&use_fixed=${useFixed}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition") || "";
      const fn = cd.match(/filename="([^"]+)"/)?.[1] || `export.${exportFmt}`;
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = fn;
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e) {
      alert("Export failed: " + e.message);
    } finally {
      setExporting(false);
    }
  };

  // ── render helpers ──
  const failedRules = (fixes?.fixes || []).filter(f => f.status !== "passed");
  const passedRules = (fixes?.fixes || []).filter(f => f.status === "passed");

  return (
    <div style={{
      fontFamily: "'IBM Plex Mono', 'JetBrains Mono', 'Fira Code', monospace",
      background: C.bg, color: C.text, minHeight: "100%", padding: 0,
    }}>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes fadeIn { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:none; } }
        .fix-card { animation: fadeIn .25s ease both; }
        .fix-card:hover { background: #1e2330 !important; }
        .tab-btn:hover { color: #e8eaf0 !important; }
        .suggestion-chip:hover { background: #4f8ef722 !important; border-color: #4f8ef766 !important; }
        .suggestion-chip.active { background: #4f8ef733 !important; border-color: #4f8ef7 !important; color: #6ba3ff !important; }
        .export-btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 14px #0006; }
        ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #2a2f3e; border-radius: 3px; }
      `}</style>

      {/* ── header ── */}
      <div style={{
        padding: "20px 24px 0", borderBottom: `1px solid ${C.border}`,
        display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap",
      }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 18, fontWeight: 700, letterSpacing: "-.3px" }}>
            Fix &amp; Export
          </div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>
            Review issues · apply fixes · export clean data
          </div>
        </div>

        {/* tabs */}
        <div style={{ display: "flex", gap: 0, borderBottom: `2px solid transparent` }}>
          {["fixes", "preview"].map(t => (
            <button key={t} className="tab-btn" onClick={() => setTab(t)} style={{
              background: "none", border: "none", color: tab === t ? C.accent : C.muted,
              borderBottom: `2px solid ${tab === t ? C.accent : "transparent"}`,
              padding: "10px 16px", cursor: "pointer", fontSize: 12, fontWeight: 600,
              textTransform: "uppercase", letterSpacing: ".5px", transition: "color .15s",
              marginBottom: -2,
            }}>{t === "fixes" ? "🔧 Quick Fixes" : "👁 Preview"}</button>
          ))}
        </div>
      </div>

      <div style={{ padding: 24 }}>

        {/* ── summary bar ── */}
        {fixes && (
          <div style={{
            display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap",
          }}>
            {[
              { label: "Total Issues", val: fixes.total_issues, col: fixes.total_issues > 0 ? C.red : C.green },
              { label: "Rules Passed", val: passedRules.length, col: C.green },
              { label: "Rules Failed", val: failedRules.length, col: failedRules.length > 0 ? C.red : C.green },
            ].map(({ label, val, col }) => (
              <div key={label} style={{
                background: C.card, border: `1px solid ${C.border}`,
                borderRadius: 8, padding: "10px 20px", textAlign: "center",
              }}>
                <div style={{ fontSize: 22, fontWeight: 700, color: col }}>{val}</div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>{label}</div>
              </div>
            ))}
          </div>
        )}

        {/* ── FIXES TAB ── */}
        {tab === "fixes" && (
          <>
            {loading ? (
              <div style={{ textAlign: "center", padding: 40, color: C.muted }}>
                <Spinner /> <span style={{ marginLeft: 10 }}>Loading fix suggestions…</span>
              </div>
            ) : !fixes ? (
              <div style={{ color: C.muted, padding: 24 }}>No data available.</div>
            ) : (
              <>
                {/* ── failed rules ── */}
                {failedRules.length === 0 ? (
                  <div style={{
                    background: C.card, border: `1px solid ${C.green}33`,
                    borderRadius: 10, padding: 24, textAlign: "center", color: C.green,
                  }}>
                    ✅ All validation rules passed — no fixes needed!
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                    {failedRules.map((fix, i) => (
                      <FixCard
                        key={fix.rule_name}
                        fix={fix}
                        instruction={fixInstr[fix.rule_name] || "keep"}
                        onInstructionChange={v => setFixInstr(p => ({ ...p, [fix.rule_name]: v }))}
                        expanded={expandedRow === fix.rule_name}
                        onToggleExpand={() => setExpandedRow(
                          expandedRow === fix.rule_name ? null : fix.rule_name)}
                        index={i}
                      />
                    ))}
                  </div>
                )}

                {/* ── agent toggle + apply ── */}
                {failedRules.length > 0 && (
                  <div style={{
                    marginTop: 24, background: C.card, border: `1px solid ${C.border}`,
                    borderRadius: 10, padding: 20,
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: 13, fontWeight: 600 }}>Apply Fixes</div>
                        <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
                          Runs against the full dataset and caches the fixed version for export.
                        </div>
                      </div>
                      {/* agent toggle */}
                      <label style={{
                        display: "flex", alignItems: "center", gap: 8, cursor: "pointer",
                        fontSize: 12, color: C.textDim,
                      }}>
                        <div
                          onClick={() => setUseAgent(!useAgent)}
                          style={{
                            width: 36, height: 20, borderRadius: 10,
                            background: useAgent ? C.accent : C.border,
                            position: "relative", transition: "background .2s", cursor: "pointer",
                          }}
                        >
                          <div style={{
                            position: "absolute", top: 3, left: useAgent ? 18 : 3,
                            width: 14, height: 14, borderRadius: "50%", background: "#fff",
                            transition: "left .2s",
                          }} />
                        </div>
                        {useAgent ? "🤖 AI fixes" : "🔧 Rule-based"}
                      </label>
                    </div>

                    <div style={{ fontSize: 11, color: C.muted, marginBottom: 16, lineHeight: 1.6 }}>
                      {useAgent
                        ? "The AI agent will write custom pandas code based on your instructions above."
                        : "Applies deterministic transformations: drop, fill, cap, or flag rows."}
                    </div>

                    <Button onClick={applyFixes} disabled={applying}>
                      {applying ? <><Spinner /> &nbsp;Applying fixes…</> : "▶ Apply Fixes"}
                    </Button>
                  </div>
                )}

                {/* ── passed rules (collapsed) ── */}
                {passedRules.length > 0 && (
                  <details style={{ marginTop: 16 }}>
                    <summary style={{
                      cursor: "pointer", fontSize: 12, color: C.muted,
                      padding: "6px 0", listStyle: "none",
                    }}>
                      ▸ {passedRules.length} passing rule(s) — no action needed
                    </summary>
                    <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 6 }}>
                      {passedRules.map(fix => (
                        <div key={fix.rule_name} style={{
                          background: C.card, border: `1px solid ${C.green}22`,
                          borderRadius: 8, padding: "10px 16px",
                          display: "flex", alignItems: "center", gap: 10,
                        }}>
                          <span style={{ color: C.green }}>✅</span>
                          <span style={{ fontSize: 13 }}>{fix.rule_name}</span>
                          <Badge label={fix.severity} color={SEV_COLOR[fix.severity] || C.muted} />
                        </div>
                      ))}
                    </div>
                  </details>
                )}
              </>
            )}

            {/* ── export section ── */}
            <ExportSection
              onExport={exportData}
              exporting={exporting}
              hasFixed={!!fixResult}
              exportFmt={exportFmt}
              setExportFmt={setExportFmt}
            />
          </>
        )}

        {/* ── PREVIEW TAB ── */}
        {tab === "preview" && (
          <PreviewTab fixResult={fixResult} />
        )}
      </div>
    </div>
  );
}

// ─── Fix Card ────────────────────────────────────────────────────────────────
function FixCard({ fix, instruction, onInstructionChange, expanded, onToggleExpand, index }) {
  const [customInstr, setCustomInstr] = useState("");
  const isCustom = instruction === "__custom__";

  return (
    <div className="fix-card" style={{
      background: C.card, border: `1px solid ${C.border}`,
      borderLeft: `3px solid ${SEV_COLOR[fix.severity] || C.muted}`,
      borderRadius: 10, overflow: "hidden",
      animationDelay: `${index * 40}ms`,
    }}>
      {/* header row */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12, padding: "14px 18px",
        cursor: "pointer",
      }} onClick={onToggleExpand}>
        <span style={{ fontSize: 16 }}>❌</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 8 }}>
            {fix.rule_name}
            <Badge label={fix.severity} color={SEV_COLOR[fix.severity] || C.muted} />
          </div>
          <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
            <span style={{ color: C.red }}>{fix.failed_count.toLocaleString()}</span>
            &nbsp;rows failed
            ({fix.failure_percentage.toFixed(2)}% of {(fix.total_rows || 0).toLocaleString()})
          </div>
        </div>
        <span style={{ color: C.muted, fontSize: 11 }}>{expanded ? "▲" : "▼"}</span>
      </div>

      {/* expanded body */}
      {expanded && (
        <div style={{ padding: "0 18px 18px", borderTop: `1px solid ${C.border}` }}>

          {/* suggested fix chips */}
          <div style={{ marginTop: 14 }}>
            <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, letterSpacing: ".3px" }}>
              SELECT FIX ACTION
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {fix.suggested_fixes.map(s => (
                <button
                  key={s.instruction}
                  className={`suggestion-chip ${instruction === s.instruction ? "active" : ""}`}
                  onClick={() => onInstructionChange(s.instruction)}
                  style={{
                    background: "transparent", border: `1px solid ${C.border}`,
                    borderRadius: 20, color: C.textDim, padding: "5px 12px",
                    fontSize: 12, cursor: "pointer", transition: "all .15s", fontFamily: "inherit",
                  }}
                >{s.label}</button>
              ))}
              <button
                className={`suggestion-chip ${isCustom ? "active" : ""}`}
                onClick={() => onInstructionChange("__custom__")}
                style={{
                  background: "transparent", border: `1px dashed ${C.border}`,
                  borderRadius: 20, color: C.textDim, padding: "5px 12px",
                  fontSize: 12, cursor: "pointer", transition: "all .15s", fontFamily: "inherit",
                }}
              >✏️ Custom instruction…</button>
            </div>
          </div>

          {/* custom text input */}
          {isCustom && (
            <div style={{ marginTop: 12 }}>
              <input
                value={customInstr}
                onChange={e => {
                  setCustomInstr(e.target.value);
                  onInstructionChange(e.target.value);
                }}
                placeholder="e.g.  fill missing currency with USD,  cap fraud_score at 0.85 …"
                style={{
                  width: "100%", background: "#0d0f14", border: `1px solid ${C.border}`,
                  borderRadius: 6, color: C.text, padding: "8px 12px",
                  fontSize: 12, fontFamily: "inherit", outline: "none", boxSizing: "border-box",
                }}
              />
              <div style={{ fontSize: 10, color: C.muted, marginTop: 4 }}>
                Free-text instruction passed to the AI agent.
              </div>
            </div>
          )}

          {/* current instruction display */}
          {instruction && instruction !== "__custom__" && (
            <div style={{
              marginTop: 12, background: "#0d0f14", borderRadius: 6, padding: "8px 12px",
              fontSize: 11, color: C.accent, fontFamily: "monospace",
            }}>
              → &nbsp;{instruction}
            </div>
          )}

          {/* failure examples */}
          {fix.failure_examples?.length > 0 && (
            <div style={{ marginTop: 14 }}>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8 }}>SAMPLE FAILING ROWS</div>
              <div style={{ overflowX: "auto" }}>
                <table style={{
                  borderCollapse: "collapse", width: "100%", fontSize: 11, minWidth: 400,
                }}>
                  <thead>
                    <tr>
                      {Object.keys(fix.failure_examples[0]).map(col => (
                        <th key={col} style={{
                          background: "#0d0f14", color: C.muted, padding: "6px 10px",
                          textAlign: "left", fontWeight: 600, borderBottom: `1px solid ${C.border}`,
                          whiteSpace: "nowrap",
                        }}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {fix.failure_examples.map((row, ri) => (
                      <tr key={ri} style={{ borderBottom: `1px solid ${C.border}22` }}>
                        {Object.values(row).map((val, ci) => (
                          <td key={ci} style={{
                            padding: "5px 10px", color: val === null ? C.muted : C.text,
                            fontStyle: val === null ? "italic" : "normal",
                          }}>
                            {val === null ? "NULL" : String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* SQL query */}
          {fix.executed_query && (
            <div style={{ marginTop: 12 }}>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 6 }}>DETECTION QUERY</div>
              <code style={{
                display: "block", background: "#0d0f14", borderRadius: 6, padding: "8px 12px",
                fontSize: 11, color: "#a8c7fa", overflowX: "auto", whiteSpace: "pre-wrap",
                wordBreak: "break-all",
              }}>{fix.executed_query}</code>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Export Section ──────────────────────────────────────────────────────────
const FORMATS = [
  { id: "csv", label: "CSV", icon: "📄", desc: "Comma-separated values" },
  { id: "excel", label: "Excel", icon: "📊", desc: "XLSX workbook" },
  { id: "json", label: "JSON", icon: "{ }", desc: "JSON records array" },
  { id: "parquet", label: "Parquet", icon: "🗜", desc: "Columnar format" },
  { id: "sqlite", label: "SQLite", icon: "🗄", desc: "Portable database file" },
];

function ExportSection({ onExport, exporting, hasFixed, exportFmt, setExportFmt }) {
  return (
    <div style={{
      marginTop: 28, background: C.card, border: `1px solid ${C.border}`,
      borderRadius: 10, padding: 20,
    }}>
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Export Dataset</div>
      <div style={{ fontSize: 11, color: C.muted, marginBottom: 16 }}>
        Download the original or fixed dataset in your preferred format.
      </div>

      {/* format selector */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 20 }}>
        {FORMATS.map(f => (
          <button
            key={f.id}
            onClick={() => setExportFmt(f.id)}
            style={{
              background: exportFmt === f.id ? `${C.accent}22` : "transparent",
              border: `1px solid ${exportFmt === f.id ? C.accent : C.border}`,
              borderRadius: 8, color: exportFmt === f.id ? C.accent : C.textDim,
              padding: "8px 14px", cursor: "pointer", fontSize: 12, fontFamily: "inherit",
              transition: "all .15s", display: "flex", alignItems: "center", gap: 6,
            }}
          >
            <span>{f.icon}</span>
            <span style={{ fontWeight: 600 }}>{f.label}</span>
            <span style={{ fontSize: 10, opacity: .7 }}>{f.desc}</span>
          </button>
        ))}
      </div>

      {/* export buttons */}
      <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
        {hasFixed && (
          <button
            className="export-btn"
            onClick={() => onExport(true)}
            disabled={exporting}
            style={{
              background: C.green, color: "#000", border: "none", borderRadius: 8,
              padding: "10px 20px", fontSize: 13, fontWeight: 700,
              cursor: exporting ? "not-allowed" : "pointer", fontFamily: "inherit",
              transition: "all .2s", opacity: exporting ? .5 : 1,
              display: "flex", alignItems: "center", gap: 8,
            }}
          >
            {exporting ? <Spinner /> : "⬇"} Export Fixed Data ({exportFmt.toUpperCase()})
          </button>
        )}
        <button
          className="export-btn"
          onClick={() => onExport(false)}
          disabled={exporting}
          style={{
            background: "transparent", color: C.text,
            border: `1px solid ${C.border}`, borderRadius: 8,
            padding: "10px 20px", fontSize: 13, fontWeight: 600,
            cursor: exporting ? "not-allowed" : "pointer", fontFamily: "inherit",
            transition: "all .2s", opacity: exporting ? .5 : 1,
            display: "flex", alignItems: "center", gap: 8,
          }}
        >
          {exporting ? <Spinner /> : "⬇"} Export Original ({exportFmt.toUpperCase()})
        </button>
      </div>
    </div>
  );
}

// ─── Preview Tab ─────────────────────────────────────────────────────────────
function PreviewTab({ fixResult }) {
  if (!fixResult) {
    return (
      <div style={{
        background: C.card, border: `1px dashed ${C.border}`,
        borderRadius: 10, padding: 40, textAlign: "center", color: C.muted,
      }}>
        <div style={{ fontSize: 32, marginBottom: 10 }}>🔧</div>
        <div style={{ fontSize: 14 }}>Apply fixes first to see a preview here.</div>
        <div style={{ fontSize: 12, marginTop: 6 }}>
          Switch to the <em>Quick Fixes</em> tab and click <strong>Apply Fixes</strong>.
        </div>
      </div>
    );
  }

  const { original_rows, fixed_rows, rows_removed, columns, preview } = fixResult;

  return (
    <div>
      {/* stats */}
      <div style={{ display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        {[
          { label: "Original Rows", val: (original_rows || 0).toLocaleString(), col: C.textDim },
          { label: "Fixed Rows", val: (fixed_rows || 0).toLocaleString(), col: C.green },
          { label: "Rows Removed", val: (rows_removed || 0).toLocaleString(), col: rows_removed > 0 ? C.yellow : C.green },
          { label: "Columns", val: (columns?.length || 0), col: C.accent },
        ].map(({ label, val, col }) => (
          <div key={label} style={{
            background: C.card, border: `1px solid ${C.border}`,
            borderRadius: 8, padding: "10px 20px", textAlign: "center",
          }}>
            <div style={{ fontSize: 22, fontWeight: 700, color: col }}>{val}</div>
            <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>{label}</div>
          </div>
        ))}
      </div>

      {/* preview table */}
      {preview?.length > 0 && (
        <div style={{
          background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflow: "hidden",
        }}>
          <div style={{
            padding: "12px 18px", borderBottom: `1px solid ${C.border}`,
            fontSize: 12, color: C.muted, fontWeight: 600,
          }}>
            PREVIEW — first 10 rows of fixed dataset
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 11 }}>
              <thead>
                <tr>
                  {Object.keys(preview[0]).map(col => (
                    <th key={col} style={{
                      background: "#0d0f14", color: C.muted, padding: "8px 12px",
                      textAlign: "left", fontWeight: 600, borderBottom: `1px solid ${C.border}`,
                      whiteSpace: "nowrap", position: "sticky", top: 0,
                    }}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.map((row, ri) => (
                  <tr key={ri} style={{
                    borderBottom: `1px solid ${C.border}22`,
                    background: ri % 2 === 0 ? "transparent" : "#ffffff04",
                  }}>
                    {Object.values(row).map((val, ci) => (
                      <td key={ci} style={{
                        padding: "6px 12px",
                        color: val === null || val === "None" ? C.muted : C.text,
                        fontStyle: val === null || val === "None" ? "italic" : "normal",
                      }}>
                        {val === null || val === "None" ? "NULL" : String(val)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
