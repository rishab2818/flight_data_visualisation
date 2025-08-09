import os
import dask.dataframe as dd
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import numexpr as ne

def _downsample_stride(df: pd.DataFrame, max_points: int = 100_000) -> pd.DataFrame:
    n = len(df)
    if n <= max_points or n == 0:
        return df
    stride = max(1, n // max_points)
    return df.iloc[::stride]

def _downsample_lttb(x: np.ndarray, y: np.ndarray, threshold: int) -> tuple[np.ndarray, np.ndarray]:
    # Largest-Triangle-Three-Buckets algorithm
    n = x.size
    if threshold >= n or threshold == 0:
        return x, y
    bucket_size = (n - 2) / (threshold - 2)
    a = 0
    sampled_x = [x[0]]
    sampled_y = [y[0]]
    for i in range(0, threshold - 2):
        start = int(np.floor((i + 1) * bucket_size) + 1)
        end = int(np.floor((i + 2) * bucket_size) + 1)
        end = min(end, n)
        bucket_x = x[start:end]
        bucket_y = y[start:end]
        if bucket_x.size == 0:
            continue
        range_start = int(np.floor(i * bucket_size) + 1)
        range_end = int(np.floor((i + 1) * bucket_size) + 1)
        range_end = min(range_end, n)
        point_ax = x[a]
        point_ay = y[a]
        range_x = x[range_start:range_end]
        range_y = y[range_start:range_end]
        avg_x = np.mean(range_x) if range_x.size else 0.0
        avg_y = np.mean(range_y) if range_y.size else 0.0
        area = np.abs((point_ax - avg_x) * (bucket_y - point_ay) - (point_ax - bucket_x) * (avg_y - point_ay))
        idx = np.argmax(area)
        a = start + idx
        sampled_x.append(x[a])
        sampled_y.append(y[a])
    sampled_x.append(x[-1])
    sampled_y.append(y[-1])
    return np.array(sampled_x), np.array(sampled_y)

def _apply_filters_and_computes(df: pd.DataFrame, computes: list[dict] | None, filters: list[dict] | None) -> pd.DataFrame:
    # computes: [{name, expr}], evaluated via numexpr
    if computes:
        for c in computes:
            name = c.get("name")
            expr = c.get("expr")
            if not name or not expr: continue
            try:
                df[name] = ne.evaluate(expr, local_dict=df.to_dict("series"))
            except Exception:
                # ignore bad expressions; could log
                pass
    # filters: [{col, op, value}]; ops: ==, !=, >, <, >=, <=
    if filters:
        mask = pd.Series(True, index=df.index)
        for f in filters:
            col, op, val = f.get("col"), f.get("op"), f.get("value")
            if col not in df.columns: 
                continue
            if op == "==": mask &= (df[col] == val)
            elif op == "!=": mask &= (df[col] != val)
            elif op == ">": mask &= (df[col] > val)
            elif op == "<": mask &= (df[col] < val)
            elif op == ">=": mask &= (df[col] >= val)
            elif op == "<=": mask &= (df[col] <= val)
        df = df[mask]
    return df

def build_plot_html(parquet_path: str, x_col: str, y_col: str, title: str, out_dir: str, method: str = "stride", max_points: int = 100_000, computes=None, filters=None) -> str:
    cols = list({x_col, y_col} | {c["name"] for c in (computes or []) if "name" in c})
    ddf = dd.read_parquet(parquet_path, columns=list(cols))
    df = ddf.dropna().compute()
    df = _apply_filters_and_computes(df, (computes or []), (filters or []))
    df = df.sort_values(x_col)
    if method == "lttb":
        x, y = _downsample_lttb(df[x_col].to_numpy(), df[y_col].to_numpy(), threshold=max_points)
        fig = go.Figure(go.Scatter(x=x, y=y, mode="markers"))
        fig.update_layout(title=title, xaxis_title=x_col, yaxis_title=y_col, template="plotly_white")
    else:
        df = _downsample_stride(df, max_points=max_points)
        fig = px.scatter(df, x=x_col, y=y_col, title=title)
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"plot_{x_col}_vs_{y_col}.html")
    fig.write_html(out, include_plotlyjs="cdn")
    return out

def build_overlay_plot_html(
    series: list[dict],
    title: str,
    out_dir: str,
    method: str = "stride",
    max_points: int = 100_000,
) -> str:
    """
    series item supports extra keys:
      - label: legend name
      - axis: "y" (default) or "y2" (use secondary Y axis)
      - units: optional string to show in y-axis title & hover
    """
    import plotly.graph_objects as go
    import plotly.express as px
    import numpy as np
    import dask.dataframe as dd
    import os

    if not series:
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, "overlay_empty.html")
        go.Figure().write_html(out, include_plotlyjs="cdn")
        return out

    # Normalize series entries and collect metadata
    safe = []
    for s in series:
        ss = dict(s)
        ss["computes"] = (s.get("computes") or [])
        ss["filters"] = (s.get("filters") or [])
        ss["axis"] = s.get("axis") or "y"
        ss["units"] = s.get("units") or ""
        ss["label"] = s.get("label") or s.get("y_col") or "series"
        safe.append(ss)

    # Color palette (distinct, readable)
    palette = px.colors.qualitative.Set2 + px.colors.qualitative.Set1 + px.colors.qualitative.Dark24

    # Axis titles
    x_title = safe[0]["x_col"]
    y1_labels = [s["label"] for s in safe if s["axis"] == "y"]
    y2_labels = [s["label"] for s in safe if s["axis"] == "y2"]
    y1_units = list({s["units"] for s in safe if s["axis"] == "y" and s["units"]})
    y2_units = list({s["units"] for s in safe if s["axis"] == "y2" and s["units"]})

    def _mk_y_title(labels, units):
        if not labels:
            return "Value"
        base = ", ".join(labels[:3]) + ("…" if len(labels) > 3 else "")
        if units:
            return f"{base} ({'/'.join(units)})"
        return base

    y_title = _mk_y_title(y1_labels, y1_units)
    y2_title = _mk_y_title(y2_labels, y2_units)

    fig = go.Figure()

    # Build traces
    for idx, s in enumerate(safe):
        x_col, y_col = s["x_col"], s["y_col"]
        extra_cols = [c.get("name") for c in s["computes"] if isinstance(c, dict) and c.get("name")]
        cols = list(set([x_col, y_col] + extra_cols))

        ddf = dd.read_parquet(s["parquet_path"], columns=cols)
        df = ddf.dropna().compute()
        df = _apply_filters_and_computes(df, s["computes"], s["filters"])
        if df.empty:
            continue
        df = df.sort_values(x_col)

        # Downsample
        if method == "lttb":
            x, y = _downsample_lttb(df[x_col].to_numpy(), df[y_col].to_numpy(), threshold=max_points)
        else:
            ddf_small = _downsample_stride(df, max_points=max_points)
            x, y = ddf_small[x_col].to_numpy(), ddf_small[y_col].to_numpy()

        hover = (
            f"<b>{s['label']}</b><br>"
            f"dataset: {os.path.basename(s['parquet_path'])}<br>"
            f"{x_col}: %{{x}}<br>"
            f"{y_col}{(' ('+s['units']+')') if s['units'] else ''}: %{{y}}<extra></extra>"
        )

        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines+markers",
                name=s["label"],
                hovertemplate=hover,
                marker=dict(size=5),
                line=dict(width=1.5),
                legendgroup=s["label"],
            )
        )
        # Assign axis after adding trace
        fig.data[-1].yaxis = "y2" if s["axis"] == "y2" else "y"
        # Color
        fig.data[-1].marker.color = palette[idx % len(palette)]
        fig.data[-1].line.color = palette[idx % len(palette)]

    # Layout with secondary Y axis if needed
    use_y2 = any(s["axis"] == "y2" for s in safe)
    layout = dict(
        title=title,
        template="plotly_white",
        legend=dict(title="Series", orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(title=x_title, zeroline=False),
        yaxis=dict(title=y_title, zeroline=False),
    )
    if use_y2:
        layout["yaxis2"] = dict(title=y2_title or "Value", overlaying="y", side="right", zeroline=False)

    fig.update_layout(**layout)

    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"overlay_{abs(hash(title)) & 0xFFFFFFFF}.html")
    fig.write_html(out, include_plotlyjs="cdn")
    return out
