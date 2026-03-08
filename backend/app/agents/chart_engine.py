"""
Chart Engine for Data Profiling
================================
Generates clean, informative charts as base64 PNG strings for web rendering.
Redesigned for clarity — useful data, not decorative noise.
"""

import io
import base64
import logging
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_MPL_READY = False

def _ensure_mpl():
    global _MPL_READY
    if not _MPL_READY:
        import matplotlib
        matplotlib.use("Agg")
        _MPL_READY = True


# ── Redesigned palette: slate-dark with clean accents ──────────
COLORS = {
    "primary":       "#10b981",   # emerald-500  — main accent
    "primary_dim":   "#065f46",   # emerald-900  — dim accent
    "secondary":     "#3b82f6",   # blue-500
    "warning":       "#f59e0b",   # amber-500
    "danger":        "#ef4444",   # red-500
    "success":       "#22c55e",   # green-500
    "bg":            "#0a0a0f",   # near-black
    "card_bg":       "#111118",   # card surface
    "surface":       "#1a1a24",   # elevated surface
    "text":          "#f1f5f9",   # slate-100
    "text_muted":    "#94a3b8",   # slate-400
    "text_dim":      "#475569",   # slate-600
    "grid":          "#1e293b",   # slate-800
    "border":        "#2d3748",   # slate-700
}

TYPE_COLORS = {
    "categorical":        "#818cf8",   # indigo-400
    "numeric_continuous": "#60a5fa",   # blue-400
    "numeric_discrete":   "#38bdf8",   # sky-400
    "datetime":           "#34d399",   # emerald-400
    "text_freeform":      "#fbbf24",   # amber-400
    "boolean":            "#fb923c",   # orange-400
    "identifier":         "#94a3b8",   # slate-400
}

PALETTE = [
    "#60a5fa", "#818cf8", "#34d399", "#fbbf24",
    "#f87171", "#a78bfa", "#fb923c", "#38bdf8",
    "#4ade80", "#f472b6",
]


def _fig_to_base64(fig, dpi: int = 110) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor=fig.get_facecolor(), edgecolor="none", pad_inches=0.12)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()
    import matplotlib.pyplot as plt
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


def _apply_theme(ax, fig=None, show_grid_x=False, show_grid_y=True):
    """Apply clean dark theme to a single axes."""
    ax.set_facecolor(COLORS["card_bg"])
    if fig:
        fig.set_facecolor(COLORS["bg"])
    ax.tick_params(colors=COLORS["text_muted"], labelsize=8, length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.xaxis.label.set_color(COLORS["text_muted"])
    ax.yaxis.label.set_color(COLORS["text_muted"])
    ax.title.set_color(COLORS["text"])
    if show_grid_y:
        ax.grid(axis="y", color=COLORS["grid"], alpha=0.6, linewidth=0.5, zorder=0)
    if show_grid_x:
        ax.grid(axis="x", color=COLORS["grid"], alpha=0.6, linewidth=0.5, zorder=0)


class ChartEngine:
    """Generates all chart types for the data profiling dashboard."""

    # ── Dataset Overview Charts ─────────────────────────────────

    def generate_dataset_overview(
        self, df: pd.DataFrame, profiles: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        _ensure_mpl()
        charts = {}
        try:
            charts["type_distribution"] = self._type_distribution_donut(profiles)
        except Exception as e:
            logger.warning(f"type_distribution chart failed: {e}")
        try:
            charts["null_heatmap"] = self._null_completeness(profiles)
        except Exception as e:
            logger.warning(f"null_heatmap chart failed: {e}")
        try:
            charts["correlation_matrix"] = self._correlation_matrix(df)
        except Exception as e:
            logger.warning(f"correlation_matrix chart failed: {e}")
        return charts

    # ── Per-Column Charts ───────────────────────────────────────

    def generate_profile_charts(
        self, df: pd.DataFrame, profiles: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        _ensure_mpl()
        charts = {}
        for p in profiles:
            col_name = p["column_name"]
            dtype = p["data_type"]
            try:
                if dtype in ("numeric_continuous", "numeric_discrete"):
                    charts[col_name] = self._numeric_histogram(df, col_name, p)
                elif dtype == "categorical":
                    charts[col_name] = self._categorical_bar(df, col_name, p)
                elif dtype == "datetime":
                    charts[col_name] = self._datetime_timeline(df, col_name)
                elif dtype == "boolean":
                    charts[col_name] = self._boolean_donut(df, col_name)
                elif dtype in ("text_freeform", "identifier"):
                    charts[col_name] = self._text_length_hist(df, col_name)
            except Exception as e:
                logger.warning(f"Chart for {col_name} failed: {e}")
        return charts

    # ── Pivot Chart ─────────────────────────────────────────────

    def generate_pivot_chart(
        self, pivot_df: pd.DataFrame, dimensions: List[str],
        measures: List[Dict[str, str]],
    ) -> Optional[str]:
        _ensure_mpl()
        import matplotlib.pyplot as plt
        if pivot_df.empty:
            return None
        try:
            dim = dimensions[0] if dimensions else pivot_df.columns[0]
            measure_cols = [c for c in pivot_df.columns if c != dim]
            if not measure_cols:
                return None

            fig, ax = plt.subplots(figsize=(9, 4))
            _apply_theme(ax, fig)

            x = pivot_df[dim].astype(str).head(20)
            val_col = measure_cols[0]
            values = pivot_df[val_col].head(20).fillna(0)
            x_pos = range(len(x))

            bar_colors = [COLORS["primary"] if i % 2 == 0 else COLORS["secondary"] for i in x_pos]
            bars = ax.bar(x_pos, values, color=bar_colors, edgecolor="none",
                          alpha=0.85, width=0.65, zorder=3)

            ax.set_xticks(x_pos)
            ax.set_xticklabels(x, rotation=40, ha="right", fontsize=8)
            ax.set_ylabel(val_col, fontsize=9)
            ax.set_title(f"{val_col} by {dim}", fontsize=11, fontweight="bold", pad=12)

            for bar, v in zip(bars, values):
                if pd.notna(v) and v != 0:
                    label = f"{v:,.0f}" if abs(v) >= 1 else f"{v:.3f}"
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + max(values) * 0.015,
                            label, ha="center", va="bottom",
                            fontsize=7, color=COLORS["text_muted"])

            fig.tight_layout()
            return _fig_to_base64(fig)
        except Exception as e:
            logger.warning(f"Pivot chart failed: {e}")
            return None

    # ── Before/After filter comparison ────────────────────────

    def generate_filtered_chart(
        self, df_before: pd.DataFrame, df_after: pd.DataFrame,
        filter_columns: List[str],
    ) -> Optional[str]:
        _ensure_mpl()
        import matplotlib.pyplot as plt
        if not filter_columns:
            return None
        try:
            col = filter_columns[0]
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 3.5))
            _apply_theme(ax1, fig)
            _apply_theme(ax2)

            def _plot_col(ax, df, color, label):
                if pd.api.types.is_numeric_dtype(df[col]):
                    ax.hist(df[col].dropna(), bins=25, color=color, alpha=0.85, edgecolor="none")
                else:
                    vc = df[col].value_counts().head(10)
                    ax.barh(range(len(vc)), vc.values, color=color, alpha=0.85, edgecolor="none")
                    ax.set_yticks(range(len(vc)))
                    ax.set_yticklabels([str(v)[:16] for v in vc.index], fontsize=7)
                ax.set_title(label, fontsize=10, fontweight="bold", color=COLORS["text"])

            _plot_col(ax1, df_before, COLORS["secondary"], f"Before  ({len(df_before):,} rows)")
            _plot_col(ax2, df_after,  COLORS["primary"],   f"After   ({len(df_after):,} rows)")
            fig.suptitle(f"Filter Impact: {col}", fontsize=12, fontweight="bold",
                         color=COLORS["text"], y=1.01)
            fig.tight_layout()
            return _fig_to_base64(fig)
        except Exception as e:
            logger.warning(f"Filtered chart failed: {e}")
            return None

    # ══════════════════════════════════════════════════════════
    # PRIVATE CHART METHODS
    # ══════════════════════════════════════════════════════════

    def _type_distribution_donut(self, profiles: List[Dict]) -> str:
        import matplotlib.pyplot as plt

        type_counts: Dict[str, int] = {}
        for p in profiles:
            dt = p["data_type"]
            type_counts[dt] = type_counts.get(dt, 0) + 1

        labels = list(type_counts.keys())
        sizes  = list(type_counts.values())
        colors = [TYPE_COLORS.get(t, COLORS["text_muted"]) for t in labels]
        nice   = [t.replace("_", " ").title() for t in labels]

        fig, ax = plt.subplots(figsize=(4, 3.2))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["bg"])

        wedges, _, autotexts = ax.pie(
            sizes, labels=None, colors=colors,
            autopct=lambda p: f"{p:.0f}%" if p > 5 else "",
            startangle=90, pctdistance=0.72,
            wedgeprops={"width": 0.45, "edgecolor": COLORS["bg"], "linewidth": 2},
        )
        for at in autotexts:
            at.set_fontsize(8)
            at.set_color(COLORS["text"])
            at.set_fontweight("bold")

        ax.legend(
            wedges, [f"{l}  ·  {s}" for l, s in zip(nice, sizes)],
            loc="center left", bbox_to_anchor=(1.02, 0.5),
            fontsize=8, frameon=False,
        )
        for text in ax.get_legend().get_texts():
            text.set_color(COLORS["text_muted"])

        ax.set_title("Column Types", fontsize=11, fontweight="bold",
                     color=COLORS["text"], pad=8)
        return _fig_to_base64(fig)

    def _null_completeness(self, profiles: List[Dict]) -> str:
        """Sorted horizontal completeness bars — green=complete, amber=partial, red=sparse."""
        import matplotlib.pyplot as plt

        data = sorted(
            [(p["column_name"], p.get("null_percentage", 0)) for p in profiles],
            key=lambda x: x[1]
        )
        names = [d[0] for d in data]
        completeness = [100 - d[1] for d in data]   # show completeness, not nulls

        bar_colors = []
        for c in completeness:
            if c >= 99:   bar_colors.append(COLORS["success"])
            elif c >= 90: bar_colors.append(COLORS["primary"])
            elif c >= 70: bar_colors.append(COLORS["warning"])
            else:         bar_colors.append(COLORS["danger"])

        h = max(2.8, len(names) * 0.30)
        fig, ax = plt.subplots(figsize=(5.5, h))
        _apply_theme(ax, fig, show_grid_x=True, show_grid_y=False)

        y_pos = range(len(names))
        ax.barh(y_pos, completeness, color=bar_colors, alpha=0.85,
                edgecolor="none", height=0.55, zorder=3)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(names, fontsize=8)
        ax.set_xlabel("Completeness %", fontsize=8)
        ax.set_xlim(0, 115)
        ax.set_title("Data Completeness", fontsize=11, fontweight="bold", pad=10)
        ax.invert_yaxis()

        for i, val in enumerate(completeness):
            label = "100%" if val >= 100 else f"{val:.1f}%"
            ax.text(val + 1, i, label, va="center", fontsize=7,
                    color=COLORS["text_muted"])

        fig.tight_layout()
        return _fig_to_base64(fig)

    def _correlation_matrix(self, df: pd.DataFrame) -> str:
        import matplotlib.pyplot as plt
        from matplotlib.colors import LinearSegmentedColormap

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) < 2:
            fig, ax = plt.subplots(figsize=(3.5, 2.5))
            _apply_theme(ax, fig)
            ax.text(0.5, 0.5, "Fewer than 2\nnumeric columns",
                    ha="center", va="center", fontsize=10, color=COLORS["text_muted"])
            ax.axis("off")
            return _fig_to_base64(fig)

        corr = df[numeric_cols].corr()
        n = len(numeric_cols)
        size = max(4, min(7, n * 0.85))
        fig, ax = plt.subplots(figsize=(size, size * 0.85))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["card_bg"])

        cmap = LinearSegmentedColormap.from_list(
            "hale", [COLORS["danger"], COLORS["surface"], COLORS["secondary"]]
        )
        im = ax.imshow(corr.values, cmap=cmap, aspect="auto", vmin=-1, vmax=1)

        short = [c[:10] for c in numeric_cols]
        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels(short, rotation=45, ha="right", fontsize=7,
                           color=COLORS["text_muted"])
        ax.set_yticklabels(short, fontsize=7, color=COLORS["text_muted"])
        ax.tick_params(length=0)

        for i in range(n):
            for j in range(n):
                val = corr.values[i, j]
                c = COLORS["text"] if abs(val) > 0.4 else COLORS["text_dim"]
                ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                        fontsize=max(6, 8 - n // 3), color=c,
                        fontweight="bold" if abs(val) >= 0.7 else "normal")

        ax.set_title("Correlation Matrix", fontsize=11, fontweight="bold",
                     color=COLORS["text"], pad=10)
        cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
        cbar.ax.tick_params(labelsize=7, colors=COLORS["text_muted"], length=0)
        cbar.outline.set_visible(False)

        fig.tight_layout()
        return _fig_to_base64(fig)

    def _numeric_histogram(self, df: pd.DataFrame, col: str, profile: Dict) -> str:
        import matplotlib.pyplot as plt

        data = df[col].dropna()
        if len(data) == 0:
            return self._empty_chart(col)

        fig, (ax_hist, ax_box) = plt.subplots(
            2, 1, figsize=(4, 3.2), height_ratios=[3.5, 1],
            gridspec_kw={"hspace": 0.04}
        )
        _apply_theme(ax_hist, fig)
        _apply_theme(ax_box)

        n_bins = min(30, max(8, len(data) // 15))
        ax_hist.hist(data, bins=n_bins, color=COLORS["primary"], alpha=0.75,
                     edgecolor=COLORS["card_bg"], linewidth=0.4, zorder=3)
        ax_hist.set_ylabel("Count", fontsize=8)
        ax_hist.tick_params(labelbottom=False)

        # Mean + std lines
        mean_val = profile.get("mean_value") or float(data.mean())
        std_val  = float(data.std()) if len(data) > 1 else 0
        ymax = ax_hist.get_ylim()[1]

        ax_hist.axvline(mean_val, color=COLORS["warning"], linestyle="--",
                        linewidth=1.2, alpha=0.9, zorder=4)
        ax_hist.text(mean_val, ymax * 0.92, f"μ = {mean_val:.2g}",
                     fontsize=7, color=COLORS["warning"], ha="left",
                     bbox=dict(boxstyle="round,pad=0.2", facecolor=COLORS["card_bg"],
                               edgecolor="none", alpha=0.8))

        if std_val > 0:
            ax_hist.axvspan(mean_val - std_val, mean_val + std_val,
                            alpha=0.08, color=COLORS["warning"], zorder=2)

        # Box plot
        bp = ax_box.boxplot(
            data, vert=False, widths=0.5, patch_artist=True,
            boxprops=dict(facecolor=COLORS["primary_dim"], edgecolor=COLORS["primary"]),
            medianprops=dict(color=COLORS["warning"], linewidth=2),
            whiskerprops=dict(color=COLORS["text_muted"], linewidth=0.8),
            capprops=dict(color=COLORS["text_muted"], linewidth=0.8),
            flierprops=dict(marker=".", markerfacecolor=COLORS["danger"],
                            markersize=2.5, alpha=0.6, markeredgewidth=0),
        )
        ax_box.set_yticks([])
        ax_box.set_xlabel(col, fontsize=8)
        ax_box.set_facecolor(COLORS["card_bg"])

        return _fig_to_base64(fig)

    def _categorical_bar(self, df: pd.DataFrame, col: str, profile: Dict) -> str:
        import matplotlib.pyplot as plt

        clean = df[col].replace(r"^\s*$", np.nan, regex=True).dropna()
        vc = clean.value_counts().head(12)
        if len(vc) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(4, max(2.2, len(vc) * 0.3)))
        _apply_theme(ax, fig, show_grid_x=True, show_grid_y=False)

        y_pos = range(len(vc))
        bars = ax.barh(y_pos, vc.values, color=PALETTE[:len(vc)],
                       alpha=0.82, edgecolor="none", height=0.62, zorder=3)
        ax.set_yticks(y_pos)
        ax.set_yticklabels([str(v)[:20] for v in vc.index], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel("Count", fontsize=8)

        total = len(clean)
        x_max = max(vc.values)
        for bar, (_, count) in zip(bars, vc.items()):
            pct = count / total * 100
            ax.text(bar.get_width() + x_max * 0.02,
                    bar.get_y() + bar.get_height() / 2,
                    f"{count:,}  ({pct:.0f}%)",
                    va="center", fontsize=7, color=COLORS["text_muted"])

        ax.set_xlim(0, x_max * 1.25)
        fig.tight_layout()
        return _fig_to_base64(fig)

    def _datetime_timeline(self, df: pd.DataFrame, col: str) -> str:
        import matplotlib.pyplot as plt

        try:
            dates = pd.to_datetime(df[col], errors="coerce").dropna()
        except Exception:
            return self._empty_chart(col)
        if len(dates) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(4, 2.4))
        _apply_theme(ax, fig)

        date_range = (dates.max() - dates.min()).days
        if date_range > 365 * 2:
            grouped = dates.dt.year.value_counts().sort_index()
            xlabel = "Year"
        elif date_range > 60:
            grouped = dates.dt.to_period("M").value_counts().sort_index()
            grouped.index = grouped.index.astype(str)
            xlabel = "Month"
        else:
            grouped = dates.dt.date.value_counts().sort_index()
            xlabel = "Date"

        ax.fill_between(range(len(grouped)), grouped.values,
                        color=COLORS["secondary"], alpha=0.25, zorder=2)
        ax.plot(range(len(grouped)), grouped.values,
                color=COLORS["secondary"], linewidth=1.5, zorder=3)
        ax.set_xticks(range(0, len(grouped), max(1, len(grouped) // 5)))
        tl = [str(v)[-7:] for v in grouped.index]
        ax.set_xticklabels(
            [tl[i] for i in range(0, len(tl), max(1, len(tl) // 5))],
            rotation=40, ha="right", fontsize=7
        )
        ax.set_ylabel("Count", fontsize=8)
        ax.set_xlabel(xlabel, fontsize=8)
        fig.tight_layout()
        return _fig_to_base64(fig)

    def _boolean_donut(self, df: pd.DataFrame, col: str) -> str:
        import matplotlib.pyplot as plt

        vc = df[col].value_counts()
        if len(vc) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(3.2, 2.8))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["bg"])

        colors = [COLORS["success"], COLORS["danger"],
                  COLORS["warning"], COLORS["text_muted"]]
        wedges, _, autotexts = ax.pie(
            vc.values, labels=None,
            colors=colors[:len(vc)],
            autopct="%1.0f%%",
            startangle=90, pctdistance=0.72,
            wedgeprops={"width": 0.45, "edgecolor": COLORS["bg"], "linewidth": 2},
        )
        for at in autotexts:
            at.set_fontsize(9)
            at.set_color(COLORS["text"])
            at.set_fontweight("bold")

        ax.legend(wedges, [str(v) for v in vc.index],
                  loc="lower center", bbox_to_anchor=(0.5, -0.1),
                  fontsize=8, frameon=False, ncol=len(vc))
        for t in ax.get_legend().get_texts():
            t.set_color(COLORS["text_muted"])

        return _fig_to_base64(fig)

    def _text_length_hist(self, df: pd.DataFrame, col: str) -> str:
        import matplotlib.pyplot as plt

        lengths = df[col].dropna().astype(str).str.len()
        if len(lengths) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(4, 2.4))
        _apply_theme(ax, fig)

        ax.hist(lengths, bins=min(25, max(6, lengths.nunique())),
                color=COLORS["secondary"], alpha=0.75,
                edgecolor=COLORS["card_bg"], linewidth=0.4, zorder=3)
        ax.set_xlabel("Character Length", fontsize=8)
        ax.set_ylabel("Count", fontsize=8)
        ax.axvline(lengths.mean(), color=COLORS["warning"],
                   linestyle="--", linewidth=1, alpha=0.9)

        fig.tight_layout()
        return _fig_to_base64(fig)

    def _empty_chart(self, col: str) -> str:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(3.5, 2))
        _apply_theme(ax, fig)
        ax.text(0.5, 0.5, "No data available", ha="center", va="center",
                fontsize=9, color=COLORS["text_dim"])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        return _fig_to_base64(fig)