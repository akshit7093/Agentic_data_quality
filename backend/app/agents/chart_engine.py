"""
Matplotlib Chart Engine for Data Profiling
==========================================
Generates publication-quality charts as base64 PNG strings for web rendering.

All charts use a consistent dark theme with the app's color palette.
Charts are generated at DPI=100, tight layout, web-optimized sizes.
"""

import io
import base64
import logging
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Lazy matplotlib import (avoid backend issues) ──────────────
_MPL_READY = False

def _ensure_mpl():
    global _MPL_READY
    if not _MPL_READY:
        import matplotlib
        matplotlib.use("Agg")  # Non-interactive backend
        _MPL_READY = True


# ── Color palette ──────────────────────────────────────────────
COLORS = {
    "primary": "#6366f1",       # indigo-500
    "primary_light": "#818cf8", # indigo-400
    "secondary": "#8b5cf6",     # violet-500
    "success": "#22c55e",       # green-500
    "warning": "#f59e0b",       # amber-500
    "danger": "#ef4444",        # red-500
    "info": "#06b6d4",          # cyan-500
    "bg": "#1e1e2e",            # dark background
    "card_bg": "#2a2a3e",       # card background
    "text": "#e2e8f0",          # light text
    "text_muted": "#94a3b8",    # muted text
    "grid": "#374151",          # grid lines
    "border": "#4b5563",        # borders
}

TYPE_COLORS = {
    "categorical": "#a78bfa",   # violet-400
    "numeric_continuous": "#60a5fa",  # blue-400
    "numeric_discrete": "#38bdf8",    # sky-400
    "datetime": "#34d399",      # emerald-400
    "text_freeform": "#fbbf24", # amber-400
    "boolean": "#fb923c",       # orange-400
    "identifier": "#94a3b8",    # gray-400
}

PALETTE = ["#6366f1", "#8b5cf6", "#06b6d4", "#22c55e", "#f59e0b",
           "#ef4444", "#ec4899", "#14b8a6", "#f97316", "#3b82f6"]


def _fig_to_base64(fig, dpi: int = 100) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG data URI."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor=fig.get_facecolor(), edgecolor="none", pad_inches=0.15)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()
    import matplotlib.pyplot as plt
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


def _apply_dark_theme(ax, fig=None):
    """Apply consistent dark theme to axes."""
    ax.set_facecolor(COLORS["card_bg"])
    if fig:
        fig.set_facecolor(COLORS["bg"])
    ax.tick_params(colors=COLORS["text_muted"], labelsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(COLORS["grid"])
    ax.spines["bottom"].set_color(COLORS["grid"])
    ax.xaxis.label.set_color(COLORS["text_muted"])
    ax.yaxis.label.set_color(COLORS["text_muted"])
    ax.title.set_color(COLORS["text"])


# ================================================================
# CHART ENGINE
# ================================================================

class ChartEngine:
    """Generates all chart types for the data profiling dashboard."""

    # ── Dataset Overview Charts ─────────────────────────────────

    def generate_dataset_overview(
        self, df: pd.DataFrame, profiles: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Generate overview-level charts for the whole dataset."""
        _ensure_mpl()
        charts = {}

        try:
            charts["type_distribution"] = self._type_distribution_donut(profiles)
        except Exception as e:
            logger.warning(f"type_distribution chart failed: {e}")

        try:
            charts["null_heatmap"] = self._null_heatmap(profiles)
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
        """Generate a mini chart per column based on its type."""
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
                    charts[col_name] = self._boolean_pie(df, col_name)
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
        """Generate a bar chart for pivot results."""
        _ensure_mpl()
        import matplotlib.pyplot as plt

        if pivot_df.empty or len(pivot_df) == 0:
            return None

        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            _apply_dark_theme(ax, fig)

            dim = dimensions[0] if dimensions else pivot_df.columns[0]
            measure_cols = [c for c in pivot_df.columns if c != dim]

            if not measure_cols:
                plt.close(fig)
                return None

            x = pivot_df[dim].astype(str).head(20)
            val_col = measure_cols[0]
            values = pivot_df[val_col].head(20)

            bars = ax.bar(range(len(x)), values, color=PALETTE[:len(x)],
                         edgecolor="none", alpha=0.9, width=0.7)

            ax.set_xticks(range(len(x)))
            ax.set_xticklabels(x, rotation=45, ha="right", fontsize=7)
            ax.set_ylabel(val_col, fontsize=9)
            ax.set_title(f"{val_col} by {dim}", fontsize=11, fontweight="bold", pad=10)
            ax.grid(axis="y", color=COLORS["grid"], alpha=0.3, linewidth=0.5)

            # Value labels on bars
            for bar, v in zip(bars, values):
                if pd.notna(v):
                    label = f"{v:,.0f}" if abs(v) >= 1 else f"{v:.2f}"
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                           label, ha="center", va="bottom", fontsize=6,
                           color=COLORS["text_muted"])

            return _fig_to_base64(fig)
        except Exception as e:
            logger.warning(f"Pivot chart failed: {e}")
            return None

    # ── Filtered Distribution Chart ─────────────────────────────

    def generate_filtered_chart(
        self, df_before: pd.DataFrame, df_after: pd.DataFrame,
        filter_columns: List[str],
    ) -> Optional[str]:
        """Generate before/after comparison for filtered data."""
        _ensure_mpl()
        import matplotlib.pyplot as plt

        if not filter_columns:
            return None

        try:
            col = filter_columns[0]  # Focus on first filtered column
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5))
            _apply_dark_theme(ax1, fig)
            _apply_dark_theme(ax2)

            # Before
            if pd.api.types.is_numeric_dtype(df_before[col]):
                ax1.hist(df_before[col].dropna(), bins=20, color=COLORS["primary"],
                        alpha=0.8, edgecolor="none")
                ax2.hist(df_after[col].dropna(), bins=20, color=COLORS["success"],
                        alpha=0.8, edgecolor="none")
            else:
                vc_before = df_before[col].value_counts().head(10)
                vc_after = df_after[col].value_counts().head(10)
                ax1.barh(range(len(vc_before)), vc_before.values,
                        color=COLORS["primary"], alpha=0.8)
                ax1.set_yticks(range(len(vc_before)))
                ax1.set_yticklabels([str(v)[:15] for v in vc_before.index], fontsize=7)
                ax2.barh(range(len(vc_after)), vc_after.values,
                        color=COLORS["success"], alpha=0.8)
                ax2.set_yticks(range(len(vc_after)))
                ax2.set_yticklabels([str(v)[:15] for v in vc_after.index], fontsize=7)

            ax1.set_title(f"Before ({len(df_before):,} rows)", fontsize=9,
                         fontweight="bold", color=COLORS["text"])
            ax2.set_title(f"After ({len(df_after):,} rows)", fontsize=9,
                         fontweight="bold", color=COLORS["text"])

            fig.suptitle(f"Filter Impact: {col}", fontsize=11,
                        fontweight="bold", color=COLORS["text"], y=1.02)
            fig.tight_layout()
            return _fig_to_base64(fig)
        except Exception as e:
            logger.warning(f"Filtered chart failed: {e}")
            return None

    # ════════════════════════════════════════════════════════════
    # PRIVATE CHART METHODS
    # ════════════════════════════════════════════════════════════

    def _type_distribution_donut(self, profiles: List[Dict]) -> str:
        """Donut chart of column semantic types."""
        import matplotlib.pyplot as plt

        type_counts: Dict[str, int] = {}
        for p in profiles:
            dt = p["data_type"]
            type_counts[dt] = type_counts.get(dt, 0) + 1

        labels = list(type_counts.keys())
        sizes = list(type_counts.values())
        colors = [TYPE_COLORS.get(t, COLORS["text_muted"]) for t in labels]

        fig, ax = plt.subplots(figsize=(3.5, 3.5))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["bg"])

        wedges, texts, autotexts = ax.pie(
            sizes, labels=None, colors=colors, autopct="%1.0f%%",
            startangle=90, pctdistance=0.75,
            wedgeprops={"width": 0.4, "edgecolor": COLORS["bg"], "linewidth": 2},
        )
        for t in autotexts:
            t.set_fontsize(8)
            t.set_color(COLORS["text"])
            t.set_fontweight("bold")

        # Legend
        nice_labels = [t.replace("_", " ").title() for t in labels]
        legend = ax.legend(
            wedges, [f"{l} ({s})" for l, s in zip(nice_labels, sizes)],
            loc="center left", bbox_to_anchor=(1.05, 0.5),
            fontsize=7, frameon=False,
        )
        for text in legend.get_texts():
            text.set_color(COLORS["text_muted"])

        ax.set_title("Column Types", fontsize=11, fontweight="bold",
                     color=COLORS["text"], pad=10)

        return _fig_to_base64(fig)

    def _null_heatmap(self, profiles: List[Dict]) -> str:
        """Horizontal bar chart showing null % per column."""
        import matplotlib.pyplot as plt

        names = [p["column_name"] for p in profiles]
        nulls = [p.get("null_percentage", 0) for p in profiles]

        fig, ax = plt.subplots(figsize=(6, max(2.5, len(names) * 0.35)))
        _apply_dark_theme(ax, fig)

        # Color: green→yellow→red based on null %
        bar_colors = []
        for n in nulls:
            if n == 0:
                bar_colors.append(COLORS["success"])
            elif n < 5:
                bar_colors.append(COLORS["warning"])
            else:
                bar_colors.append(COLORS["danger"])

        y_pos = range(len(names))
        bars = ax.barh(y_pos, nulls, color=bar_colors, alpha=0.85,
                      edgecolor="none", height=0.6)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(names, fontsize=8)
        ax.set_xlabel("Missing %", fontsize=9)
        ax.set_title("Data Completeness", fontsize=11, fontweight="bold", pad=10)
        ax.set_xlim(0, max(max(nulls) * 1.3, 5))
        ax.grid(axis="x", color=COLORS["grid"], alpha=0.3, linewidth=0.5)
        ax.invert_yaxis()

        # Value labels
        for bar, val in zip(bars, nulls):
            label = f"{val:.1f}%" if val > 0 else "0%"
            ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                   label, va="center", fontsize=7, color=COLORS["text_muted"])

        return _fig_to_base64(fig)

    def _correlation_matrix(self, df: pd.DataFrame) -> str:
        """Correlation heatmap for numeric columns."""
        import matplotlib.pyplot as plt
        from matplotlib.colors import LinearSegmentedColormap

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) < 2:
            # Not enough numeric columns for correlation
            fig, ax = plt.subplots(figsize=(3, 2))
            _apply_dark_theme(ax, fig)
            ax.text(0.5, 0.5, "< 2 numeric\ncolumns",
                   ha="center", va="center", fontsize=10, color=COLORS["text_muted"])
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis("off")
            return _fig_to_base64(fig)

        corr = df[numeric_cols].corr()
        n = len(numeric_cols)
        fig, ax = plt.subplots(figsize=(max(3.5, n * 0.8), max(3, n * 0.7)))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["card_bg"])

        cmap = LinearSegmentedColormap.from_list(
            "custom", [COLORS["danger"], COLORS["card_bg"], COLORS["info"]]
        )
        im = ax.imshow(corr.values, cmap=cmap, aspect="auto", vmin=-1, vmax=1)

        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        short_names = [c[:12] for c in numeric_cols]
        ax.set_xticklabels(short_names, rotation=45, ha="right", fontsize=7,
                          color=COLORS["text_muted"])
        ax.set_yticklabels(short_names, fontsize=7, color=COLORS["text_muted"])

        # Annotate cells
        for i in range(n):
            for j in range(n):
                val = corr.values[i, j]
                color = COLORS["text"] if abs(val) > 0.5 else COLORS["text_muted"]
                ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                       fontsize=7, color=color, fontweight="bold" if abs(val) > 0.7 else "normal")

        ax.set_title("Correlation Matrix", fontsize=11, fontweight="bold",
                     color=COLORS["text"], pad=10)
        cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cbar.ax.tick_params(labelsize=7, colors=COLORS["text_muted"])

        return _fig_to_base64(fig)

    def _numeric_histogram(self, df: pd.DataFrame, col: str, profile: Dict) -> str:
        """Histogram + box plot for numeric column."""
        import matplotlib.pyplot as plt

        data = df[col].dropna()
        if len(data) == 0:
            return self._empty_chart(col)

        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(3.5, 2.8), height_ratios=[3, 1],
            gridspec_kw={"hspace": 0.05}
        )
        _apply_dark_theme(ax1, fig)
        _apply_dark_theme(ax2)

        # Histogram
        ax1.hist(data, bins=min(30, max(5, len(data) // 20)),
                color=COLORS["primary"], alpha=0.8, edgecolor=COLORS["card_bg"],
                linewidth=0.5)
        ax1.set_ylabel("Count", fontsize=7)
        ax1.tick_params(labelbottom=False)

        # Stats annotation
        mean_val = profile.get("mean_value")
        if mean_val is not None:
            ax1.axvline(mean_val, color=COLORS["warning"], linestyle="--",
                       linewidth=1, alpha=0.8)
            ax1.text(mean_val, ax1.get_ylim()[1] * 0.9, f"μ={mean_val:.1f}",
                    fontsize=6, color=COLORS["warning"], ha="left")

        ax1.grid(axis="y", color=COLORS["grid"], alpha=0.3, linewidth=0.5)

        # Box plot
        bp = ax2.boxplot(data, vert=False, widths=0.6,
                        patch_artist=True,
                        boxprops=dict(facecolor=COLORS["primary"], alpha=0.6,
                                     edgecolor=COLORS["primary_light"]),
                        medianprops=dict(color=COLORS["warning"], linewidth=2),
                        whiskerprops=dict(color=COLORS["text_muted"]),
                        capprops=dict(color=COLORS["text_muted"]),
                        flierprops=dict(marker=".", markerfacecolor=COLORS["danger"],
                                       markersize=3, alpha=0.5))
        ax2.set_yticks([])
        ax2.set_xlabel(col, fontsize=7)

        return _fig_to_base64(fig)

    def _categorical_bar(self, df: pd.DataFrame, col: str, profile: Dict) -> str:
        """Horizontal bar chart for categorical column."""
        import matplotlib.pyplot as plt

        # Use clean values (exclude empty strings)
        clean = df[col].replace(r'^\s*$', np.nan, regex=True).dropna()
        vc = clean.value_counts().head(12)
        if len(vc) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(3.5, max(1.8, len(vc) * 0.28)))
        _apply_dark_theme(ax, fig)

        colors = PALETTE[:len(vc)]
        y_pos = range(len(vc))
        bars = ax.barh(y_pos, vc.values, color=colors, alpha=0.85,
                      edgecolor="none", height=0.65)
        ax.set_yticks(y_pos)
        ax.set_yticklabels([str(v)[:18] for v in vc.index], fontsize=7)
        ax.invert_yaxis()
        ax.grid(axis="x", color=COLORS["grid"], alpha=0.3, linewidth=0.5)

        # Value + percentage labels
        total = len(clean)
        for bar, (val_name, count) in zip(bars, vc.items()):
            pct = count / total * 100
            ax.text(bar.get_width() + max(vc.values) * 0.02,
                   bar.get_y() + bar.get_height()/2,
                   f"{count:,} ({pct:.0f}%)", va="center", fontsize=6,
                   color=COLORS["text_muted"])

        return _fig_to_base64(fig)

    def _datetime_timeline(self, df: pd.DataFrame, col: str) -> str:
        """Time series histogram for datetime column."""
        import matplotlib.pyplot as plt

        try:
            dates = pd.to_datetime(df[col], errors="coerce").dropna()
        except Exception:
            return self._empty_chart(col)

        if len(dates) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(3.5, 2))
        _apply_dark_theme(ax, fig)

        # Group by month or year depending on range
        date_range = (dates.max() - dates.min()).days
        if date_range > 365 * 3:
            grouped = dates.dt.year.value_counts().sort_index()
            xlabel = "Year"
        elif date_range > 90:
            grouped = dates.dt.to_period("M").value_counts().sort_index()
            grouped.index = grouped.index.astype(str)
            xlabel = "Month"
        else:
            grouped = dates.dt.date.value_counts().sort_index()
            xlabel = "Date"

        ax.bar(range(len(grouped)), grouped.values,
              color=COLORS["success"], alpha=0.8, edgecolor="none", width=0.8)
        ax.set_xticks(range(0, len(grouped), max(1, len(grouped) // 6)))
        tick_labels = [str(v)[-7:] for v in grouped.index]
        ax.set_xticklabels(
            [tick_labels[i] for i in range(0, len(tick_labels), max(1, len(tick_labels) // 6))],
            rotation=45, ha="right", fontsize=6
        )
        ax.set_ylabel("Count", fontsize=7)
        ax.grid(axis="y", color=COLORS["grid"], alpha=0.3, linewidth=0.5)

        return _fig_to_base64(fig)

    def _boolean_pie(self, df: pd.DataFrame, col: str) -> str:
        """Pie chart for boolean column."""
        import matplotlib.pyplot as plt

        vc = df[col].value_counts()
        if len(vc) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(3, 2.5))
        fig.set_facecolor(COLORS["bg"])
        ax.set_facecolor(COLORS["bg"])

        colors = [COLORS["success"], COLORS["danger"]] + [COLORS["warning"]]
        ax.pie(vc.values, labels=[str(v) for v in vc.index],
              colors=colors[:len(vc)], autopct="%1.0f%%",
              startangle=90, textprops={"fontsize": 8, "color": COLORS["text"]})

        return _fig_to_base64(fig)

    def _text_length_hist(self, df: pd.DataFrame, col: str) -> str:
        """Histogram of text lengths for text/ID columns."""
        import matplotlib.pyplot as plt

        lengths = df[col].dropna().astype(str).str.len()
        if len(lengths) == 0:
            return self._empty_chart(col)

        fig, ax = plt.subplots(figsize=(3.5, 2))
        _apply_dark_theme(ax, fig)

        ax.hist(lengths, bins=min(20, max(5, lengths.nunique())),
               color=COLORS["info"], alpha=0.8, edgecolor=COLORS["card_bg"],
               linewidth=0.5)
        ax.set_xlabel("Character Length", fontsize=7)
        ax.set_ylabel("Count", fontsize=7)
        ax.grid(axis="y", color=COLORS["grid"], alpha=0.3, linewidth=0.5)

        return _fig_to_base64(fig)

    def _empty_chart(self, col: str) -> str:
        """Placeholder for columns with no data."""
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(3, 2))
        _apply_dark_theme(ax, fig)
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
               fontsize=10, color=COLORS["text_muted"])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        return _fig_to_base64(fig)
