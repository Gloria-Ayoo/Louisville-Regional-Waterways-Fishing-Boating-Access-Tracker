"""
Louisville Waterways Fishing — Plotly Dash Dashboard

Reads the analytics-ready view `vw_fishing_outlook` from Supabase
PostgreSQL and visualizes the 7-day outlook across six fishing
locations.

Views: 4 KPIs · activity heatmap · trend line chart · safety
distribution bar · interactive map · detail table.

Run:  python src/dashboard.py
Then open http://localhost:8050

Requires the same .env at the project root that etl_pipeline.py uses.
Run etl_pipeline.py at least once and create the view via
sql/create_views.sql before launching the dashboard.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback, ctx, dash_table, dcc, html
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dashboard")


# ---------- Theme + style constants ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Muted, slightly desaturated semantic colors — clear but not loud.
SAFETY_COLORS = {"Safe": "#15803d", "Caution": "#a16207", "Unsafe": "#b91c1c"}

# Corporate slate/navy palette. One primary accent, no gradient noise.
THEME = {
    "primary":     "#1e3a5f",   # corporate navy — header, buttons, accents
    "primary_alt": "#334e6f",   # softer navy for hover states
    "background":  "#f5f7fa",   # very light cool gray — the page surface
    "card":        "#ffffff",
    "border":      "#e2e8f0",   # crisp 1px card border
    "text_dark":   "#1a202c",   # near-black, for headings + KPI values
    "text_muted":  "#64748b",   # slate gray, for labels + subtitles
    "subtle":      "#f1f5f9",   # plot background, slightly cooler than the page
}

CARD_STYLE = {
    "boxShadow": "none",
    "borderRadius": "8px",
    "border": f"1px solid {THEME['border']}",
    "backgroundColor": THEME["card"],
}


# ---------- Database access ----------

def get_database_url() -> str:
    """SQLAlchemy URL from .env at project root."""
    load_dotenv(PROJECT_ROOT / ".env")
    creds = {k: os.getenv(k) for k in ("user", "password", "host", "dbname")}
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise RuntimeError(f"Missing .env values: {', '.join(missing)}")
    port = os.getenv("port", "5432")
    safe_pw = quote_plus(creds["password"])
    return f"postgresql+psycopg2://{creds['user']}:{safe_pw}@{creds['host']}:{port}/{creds['dbname']}"


# Single source of truth for the analytics dataset — joins live in the
# view, this just SELECTs from it.
ANALYTICS_QUERY = text("SELECT * FROM public.vw_fishing_outlook ORDER BY date, location_name;")


def load_data() -> pd.DataFrame:
    """Pull the analytics dataset. Returns empty DataFrame on any failure."""
    try:
        engine = create_engine(get_database_url())
        df = pd.read_sql(ANALYTICS_QUERY, engine)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        log.info("Loaded %d rows from vw_fishing_outlook", len(df))
        return df
    except Exception as e:
        log.error("Failed to load data: %s", e)
        return pd.DataFrame()


# ---------- Figure factories ----------

def empty_figure(message: str = "No data — run etl_pipeline.py first") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False,
                       font={"size": 14, "color": "#6c757d"},
                       xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(xaxis={"visible": False}, yaxis={"visible": False},
                      plot_bgcolor="white",
                      margin={"t": 30, "b": 30, "l": 30, "r": 30})
    return fig


def make_activity_heatmap(df: pd.DataFrame) -> go.Figure:
    """Location × date heatmap, sorted best→worst by weekly average."""
    if df.empty:
        return empty_figure()

    location_order = (df.groupby("location_name")["fish_activity_score"]
                      .mean().sort_values(ascending=False).index.tolist())

    def pivot(col):
        return df.pivot(index="location_name", columns="date", values=col).reindex(location_order)

    score_grid = pivot("fish_activity_score")
    customdata = [[[pivot("safety_status").values[i][j],
                    pivot("temperature_f").values[i][j],
                    pivot("wind_speed_mph").values[i][j],
                    pivot("precipitation_in").values[i][j],
                    pivot("discharge_m3s").values[i][j]]
                   for j in range(score_grid.shape[1])]
                  for i in range(score_grid.shape[0])]

    text_labels = [["—" if pd.isna(z) else f"{int(round(z))}" for z in row]
                   for row in score_grid.values]
    date_labels = [d.strftime("%a<br>%b %d") for d in score_grid.columns]

    fig = go.Figure(data=go.Heatmap(
        z=score_grid.values, x=date_labels, y=score_grid.index,
        colorscale="RdYlGn", zmin=0, zmax=100,
        xgap=3, ygap=3,
        text=text_labels, texttemplate="%{text}",
        textfont={"size": 13, "color": "#1a2733", "family": "Arial Black"},
        customdata=customdata,
        hovertemplate=("<b>%{y}</b><br>Date: %{x}<br>"
                       "Activity Score: <b>%{z:.0f}</b>/100<br>"
                       "Safety: %{customdata[0]}<br>"
                       "Temp: %{customdata[1]:.1f}°F<br>"
                       "Wind: %{customdata[2]:.1f} mph<br>"
                       "Precip: %{customdata[3]:.2f} in<br>"
                       "Flow: %{customdata[4]:.0f} m³/s<extra></extra>"),
        colorbar={"title": {"text": "Activity<br>Score", "font": {"size": 12}},
                  "len": 0.85, "thickness": 14, "outlinewidth": 0},
    ))
    fig.update_layout(
        title={"text": "<b>Fish Activity Forecast</b>", "x": 0.02,
               "xanchor": "left", "font": {"size": 18}},
        xaxis={"side": "top", "tickfont": {"size": 11}, "showgrid": False},
        yaxis={"tickfont": {"size": 12}, "automargin": True,
               "showgrid": False, "autorange": "reversed"},
        plot_bgcolor="#f4f7fa", paper_bgcolor="rgba(0,0,0,0)",
        margin={"t": 70, "b": 30, "l": 10, "r": 10}, height=420,
    )
    return fig


def make_trend_chart(df: pd.DataFrame) -> go.Figure:
    """Time-series line per location, with poor/good reference bands."""
    if df.empty:
        return empty_figure()

    df_sorted = df.sort_values(["location_name", "date"]).copy()
    df_sorted["date"] = pd.to_datetime(df_sorted["date"])

    fig = px.line(df_sorted, x="date", y="fish_activity_score",
                  color="location_name", markers=True,
                  color_discrete_sequence=px.colors.qualitative.D3,
                  labels={"date": "", "fish_activity_score": "Activity Score",
                          "location_name": ""})

    fig.add_hrect(y0=0, y1=33, fillcolor=SAFETY_COLORS["Unsafe"],
                  opacity=0.06, line_width=0, layer="below")
    fig.add_hrect(y0=67, y1=100, fillcolor=SAFETY_COLORS["Safe"],
                  opacity=0.06, line_width=0, layer="below")
    fig.add_hline(y=33, line_dash="dot", line_color=SAFETY_COLORS["Unsafe"], opacity=0.4,
                  annotation_text=" Poor", annotation_position="right",
                  annotation_font_color=SAFETY_COLORS["Unsafe"], annotation_font_size=10)
    fig.add_hline(y=67, line_dash="dot", line_color=SAFETY_COLORS["Safe"], opacity=0.4,
                  annotation_text=" Good", annotation_position="right",
                  annotation_font_color=SAFETY_COLORS["Safe"], annotation_font_size=10)

    fig.update_traces(
        line={"width": 2.5},
        marker={"size": 8, "line": {"width": 1, "color": "white"}},
        hovertemplate=("<b>%{fullData.name}</b><br>Date: %{x|%a %b %d}<br>"
                       "Score: <b>%{y:.0f}</b>/100<extra></extra>"),
    )
    fig.update_layout(
        title={"text": "<b>Activity Score Trends</b>", "x": 0.02,
               "xanchor": "left", "font": {"size": 18}},
        yaxis={"range": [-5, 105], "gridcolor": "#dbe5ee",
               "title": {"text": "Activity Score (0–100)", "font": {"size": 11}}},
        xaxis={"gridcolor": "#dbe5ee", "tickformat": "%a<br>%b %d"},
        plot_bgcolor="#f4f7fa", paper_bgcolor="rgba(0,0,0,0)",
        legend={"orientation": "h", "y": -0.2, "x": 0.5, "xanchor": "center", "title": ""},
        hovermode="closest",
        margin={"t": 60, "b": 80, "l": 50, "r": 50}, height=440,
    )
    return fig


def make_safety_distribution(df: pd.DataFrame) -> go.Figure:
    """Stacked horizontal bars: Safe/Caution/Unsafe day counts per location."""
    if df.empty:
        return empty_figure()
    counts = df.groupby(["location_name", "safety_status"]).size().reset_index(name="days")
    fig = px.bar(counts, x="days", y="location_name", color="safety_status",
                 orientation="h", color_discrete_map=SAFETY_COLORS,
                 category_orders={"safety_status": ["Safe", "Caution", "Unsafe"]},
                 labels={"days": "Days", "location_name": "", "safety_status": "Safety"})
    fig.update_layout(
        title={"text": "<b>Safety Status Distribution</b>", "x": 0.02,
               "xanchor": "left", "font": {"size": 16}},
        plot_bgcolor="#f4f7fa", paper_bgcolor="rgba(0,0,0,0)",
        legend={"orientation": "h", "y": -0.2, "x": 0.5, "xanchor": "center"},
        margin={"t": 50, "b": 40, "l": 10, "r": 10}, height=420,
    )
    fig.update_xaxes(gridcolor="#dbe5ee")
    return fig


def make_location_map(df: pd.DataFrame, selected_date) -> go.Figure:
    """OpenStreetMap with safety-colored dots for the selected date."""
    if df.empty:
        return empty_figure()
    if selected_date is None:
        selected_date = df["date"].max()
    elif isinstance(selected_date, str):
        selected_date = pd.to_datetime(selected_date).date()

    snapshot = df[df["date"] == selected_date].copy()
    if snapshot.empty:
        return empty_figure(f"No data for {selected_date}")

    fig = px.scatter_map(snapshot, lat="latitude", lon="longitude",
                         color="safety_status", color_discrete_map=SAFETY_COLORS,
                         hover_name="location_name",
                         hover_data={"city": True, "fish_activity_score": True,
                                     "temperature_f": ":.1f", "wind_speed_mph": ":.1f",
                                     "discharge_m3s": ":.0f",
                                     "latitude": False, "longitude": False,
                                     "safety_status": False},
                         zoom=8.5, size_max=25, size=[20] * len(snapshot),
                         category_orders={"safety_status": ["Safe", "Caution", "Unsafe"]})
    fig.update_layout(
        title={"text": f"<b>{selected_date.strftime('%A, %b %d')}</b>", "x": 0.02,
               "xanchor": "left", "font": {"size": 16}},
        map_style="open-street-map", paper_bgcolor="rgba(0,0,0,0)",
        margin={"t": 50, "b": 0, "l": 0, "r": 0}, height=420,
        legend={"orientation": "h", "y": -0.05, "x": 0.5, "xanchor": "center"},
    )
    return fig


def make_kpi_values(df: pd.DataFrame) -> dict:
    """Compute the four headline metrics."""
    if df.empty:
        return {"locations": "—", "safe_days": "—", "avg_score": "—",
                "best_spot": "—", "best_score": ""}
    safe_rows = int((df["safety_status"] == "Safe").sum())
    best = df.loc[df["fish_activity_score"].idxmax()]
    return {
        "locations": str(df["location_name"].nunique()),
        "safe_days": f"{safe_rows}/{len(df)}",
        "avg_score": f"{df['fish_activity_score'].mean():.0f}",
        "best_spot": best["location_name"],
        "best_score": f"Score: {best['fish_activity_score']:.0f}",
    }


# ---------- Layout helpers ----------

def kpi_card(
    value_id: str,
    title: str,
    subtitle: str = "",
    icon: str = "",
    icon_color: str = "",
    subtitle_id: str | None = None,
) -> dbc.Card:
    """Clean white card — colored icon + label on top, big bold value as the
    focal point, optional muted subtitle below.

    Parameters
    ----------
    value_id    : Dash component id for the main value element.
    title       : Label text (rendered uppercase).
    subtitle    : Static subtitle string. Ignored when subtitle_id is set.
    icon        : Bootstrap Icons class, e.g. ``"bi-geo-alt-fill"``.
    icon_color  : CSS color for the icon, e.g. ``"#2563eb"``.
    subtitle_id : When set, renders a callback-updatable ``html.Div`` with
                  this id instead of the static subtitle string.  Use this
                  for the Best Spot card so the callback can write
                  ``"Score: 65"`` dynamically.
    """
    label_row = html.Div(
        [
            html.I(className=f"bi {icon} me-1",
                   style={"color": icon_color, "fontSize": "0.95rem"}) if icon else None,
            html.Span(title.upper(),
                      style={"color": THEME["text_muted"],
                             "letterSpacing": "0.6px", "fontWeight": "600"}),
        ],
        className="small d-flex align-items-center",
    )

    if subtitle_id is not None:
        sub_el = html.Div(id=subtitle_id, className="small mt-2",
                          style={"color": THEME["text_muted"]})
    elif subtitle:
        sub_el = html.Div(subtitle, className="small mt-2",
                          style={"color": THEME["text_muted"]})
    else:
        sub_el = None

    return dbc.Card(
        dbc.CardBody(
            [label_row,
             html.H2(id=value_id, className="mt-2 mb-0",
                     style={"color": THEME["text_dark"], "fontWeight": "700",
                            "fontSize": "2rem", "lineHeight": "1.1"}),
             sub_el],
            style={"padding": "20px 22px"},
        ),
        style=CARD_STYLE,
        className="h-100",
    )


# ---------- Dash app ----------

app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           suppress_callback_exceptions=True,
           title="Louisville Fishing Dashboard")
server = app.server

app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}<title>{%title%}</title>{%favicon%}{%css%}
        <style>
            body {
                background-color: #f5f7fa;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                color: #1a202c;
                min-height: 100vh;
            }
            .Select-control, .DateInput_input { border-radius: 6px !important; }
            .filter-label {
                color: #64748b !important;
                font-weight: 600 !important;
                letter-spacing: 0.4px;
                text-transform: uppercase;
                font-size: 0.75rem;
            }
            /* Subtle border-color shift on card hover — no transform, no glow */
            .card { transition: border-color 0.15s ease; }
            .card:hover { border-color: #cbd5e1 !important; }
            /* Refresh button hover */
            #refresh-button:hover {
                background-color: #f1f5f9 !important;
                color: #1e3a5f !important;
            }
        </style>
    </head>
    <body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>
"""

# Prime the date picker bounds + location options from the first load.
_initial_df = load_data()
if _initial_df.empty:
    log.warning("Starting with no data. Run etl_pipeline.py to populate the warehouse.")
    _min_date = _max_date = pd.Timestamp.today().date()
    _location_options = []
else:
    _min_date, _max_date = _initial_df["date"].min(), _initial_df["date"].max()
    _location_options = [{"label": n, "value": n}
                         for n in sorted(_initial_df["location_name"].unique())]


# ---------- Layout ----------

app.layout = dbc.Container([

    # Header — solid corporate navy, no gradient
    html.Div(
        dbc.Row([
            dbc.Col([
                html.Div(
                    "Louisville Waterways Fishing Outlook",
                    style={"fontWeight": "700", "fontSize": "1.5rem",
                           "color": "white", "letterSpacing": "-0.2px"},
                ),
                html.Div("7-day forecast · weather + river flow · six locations",
                         style={"color": "#cbd5e1", "fontSize": "0.9rem",
                                "marginTop": "2px"}),
            ], md=8),
            dbc.Col(
                dbc.Button(
                    "↻  Refresh data",
                    id="refresh-button",
                    className="float-end",
                    style={
                        "backgroundColor": "white",
                        "color": THEME["primary"],
                        "border": "none",
                        "borderRadius": "6px",
                        "fontWeight": "600",
                        "padding": "8px 18px",
                    },
                ),
                md=4, className="d-flex align-items-center"),
        ]),
        style={
            "backgroundColor": THEME["primary"],
            "padding": "22px 28px",
            "borderRadius": "8px",
            "marginTop": "16px",
            "marginBottom": "22px",
        },
    ),

    # Filters + quick-select date buttons
    dbc.Row([
        dbc.Col([
            html.Label("Date range", className="filter-label"),
            html.Div([
                dbc.ButtonGroup([
                    dbc.Button("Today", id="btn-today", size="sm",
                               style={"color": THEME["primary"],
                                      "backgroundColor": "white",
                                      "border": f"1px solid {THEME['primary']}",
                                      "fontWeight": "500"}),
                    dbc.Button("Next 7 days", id="btn-7day", size="sm",
                               style={"color": THEME["primary"],
                                      "backgroundColor": "white",
                                      "border": f"1px solid {THEME['primary']}",
                                      "fontWeight": "500"}),
                    dbc.Button("All time", id="btn-all", size="sm",
                               style={"color": THEME["primary"],
                                      "backgroundColor": "white",
                                      "border": f"1px solid {THEME['primary']}",
                                      "fontWeight": "500"}),
                ], className="me-3 mb-2"),
                dcc.DatePickerRange(
                    id="date-range",
                    min_date_allowed=_min_date, max_date_allowed=_max_date,
                    start_date=_min_date, end_date=_max_date,
                    display_format="MMM D", className="d-inline-block"),
            ], className="d-flex flex-wrap align-items-center"),
        ], md=8),
        dbc.Col([
            html.Label("Locations", className="filter-label"),
            dcc.Dropdown(id="location-filter", options=_location_options,
                         value=[o["value"] for o in _location_options],
                         multi=True, placeholder="All locations"),
        ], md=4),
    ], className="mb-3"),

    # KPI cards
    dbc.Row([
        dbc.Col(kpi_card("kpi-locations-value", "Locations Tracked",
                         icon="bi-geo-alt-fill", icon_color="#2563eb"), md=3),
        dbc.Col(kpi_card("kpi-safe-value", "Safe Day Slots",
                         subtitle="Out of all (location × day) cells",
                         icon="bi-shield-check", icon_color="#16a34a"), md=3),
        dbc.Col(kpi_card("kpi-score-value", "Avg Activity Score",
                         subtitle="0–100, higher is better",
                         icon="bi-graph-up", icon_color="#d97706"), md=3),
        dbc.Col(kpi_card("kpi-best-value", "Best Spot This Week",
                         icon="bi-star-fill", icon_color="#9333ea",
                         subtitle_id="kpi-best-subtitle"), md=3),
    ], className="g-3 mb-3"),

    # Heatmap
    dbc.Row(dbc.Col(dbc.Card(dbc.CardBody([
        html.Div([
            html.Span("Top row = best location this week",
                      style={"fontWeight": 600, "color": THEME["primary"]}),
            html.Span("  ·  ", style={"color": "#bbb"}),
            html.Span("0 (poor) → 100 (excellent)",
                      style={"color": THEME["text_muted"]}),
        ], className="small mb-2"),
        dcc.Graph(id="heatmap-chart"),
    ]), style=CARD_STYLE), width=12), className="mb-3"),

    # Trend chart
    dbc.Row(dbc.Col(dbc.Card(dbc.CardBody([
        html.Div([
            html.Span("Daily trajectory of fish activity per location",
                      style={"fontWeight": 600, "color": THEME["primary"]}),
            html.Span("  ·  ", style={"color": "#bbb"}),
            html.Span("Shaded bands mark the poor (red) and good (green) zones",
                      style={"color": THEME["text_muted"]}),
        ], className="small mb-2"),
        dcc.Graph(id="trend-chart"),
    ]), style=CARD_STYLE), width=12), className="mb-3"),

    # Safety distribution + Map
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(id="safety-chart")),
                         style=CARD_STYLE, className="h-100"), md=6),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([
                html.Label("Map date", className="filter-label me-2"),
                dcc.Dropdown(id="map-date", options=[], clearable=False,
                             style={"width": "200px", "display": "inline-block"}),
            ], className="mb-2"),
            dcc.Graph(id="map-chart"),
        ]), style=CARD_STYLE, className="h-100"), md=6),
    ], className="mb-3"),

    # Detail table
    dbc.Row(dbc.Col(dbc.Card(dbc.CardBody([
        html.H5("Detailed Forecast", className="mb-3"),
        dash_table.DataTable(
            id="detail-table",
            page_size=15, sort_action="native", filter_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"fontFamily": "Arial, sans-serif", "fontSize": 13,
                        "padding": "8px", "textAlign": "left"},
            style_header={"backgroundColor": "#f8f9fa", "fontWeight": "bold",
                          "borderBottom": "2px solid #dee2e6"},
            style_data_conditional=[
                {"if": {"filter_query": '{safety_status} = "Safe"'},
                 "backgroundColor": "#f0fdf4"},
                {"if": {"filter_query": '{safety_status} = "Caution"'},
                 "backgroundColor": "#fefce8"},
                {"if": {"filter_query": '{safety_status} = "Unsafe"'},
                 "backgroundColor": "#fef2f2"},
            ],
        ),
    ]), style=CARD_STYLE), width=12), className="mb-4"),

    # Footer
    html.Hr(),
    html.Div("Data: Open-Meteo Forecast & Flood APIs · Pipeline: etl_pipeline.py · "
             "Refresh re-queries Supabase PostgreSQL",
             className="text-muted small text-center mb-4"),

    # Cached records — refresh button writes, filter callbacks read.
    dcc.Store(id="data-store"),
], fluid=True)


# ---------- Callbacks ----------

@callback(
    Output("data-store", "data"),
    Output("date-range", "min_date_allowed"),
    Output("date-range", "max_date_allowed"),
    Output("date-range", "start_date"),
    Output("date-range", "end_date"),
    Output("location-filter", "options"),
    Output("location-filter", "value"),
    Input("refresh-button", "n_clicks"),
)
def refresh_data(n_clicks):
    """Re-query Postgres and update filter bounds. Fires on load + on click."""
    df = load_data()
    if df.empty:
        today = pd.Timestamp.today().date()
        return [], today, today, today, today, [], []

    min_d, max_d = df["date"].min(), df["date"].max()
    locations = sorted(df["location_name"].unique())
    options = [{"label": n, "value": n} for n in locations]
    df_out = df.copy()
    df_out["date"] = df_out["date"].astype(str)
    return df_out.to_dict("records"), min_d, max_d, min_d, max_d, options, locations


def _filter_df(records, start_date, end_date, locations) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date).date()]
    if locations:
        df = df[df["location_name"].isin(locations)]
    return df


@callback(
    Output("date-range", "start_date", allow_duplicate=True),
    Output("date-range", "end_date", allow_duplicate=True),
    Input("btn-today", "n_clicks"),
    Input("btn-7day", "n_clicks"),
    Input("btn-all", "n_clicks"),
    State("data-store", "data"),
    prevent_initial_call=True,
)
def quick_date_select(_today, _seven, _all, records):
    """Map quick-select buttons to date-range presets."""
    if not records:
        return dash.no_update, dash.no_update
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    available = sorted(df["date"].unique())
    if not available:
        return dash.no_update, dash.no_update

    today = pd.Timestamp.today().date()
    triggered = ctx.triggered_id

    if triggered == "btn-today":
        target = today if today in available else min(available, key=lambda d: abs((d - today).days))
        return target, target

    if triggered == "btn-7day":
        future = [d for d in available if d >= today]
        start = today if today in available else (future[0] if future else available[0])
        end = min(start + pd.Timedelta(days=6), available[-1])
        end_date = end.date() if hasattr(end, "date") else end
        return start, end_date

    if triggered == "btn-all":
        return available[0], available[-1]

    return dash.no_update, dash.no_update


@callback(
    Output("kpi-locations-value", "children"),
    Output("kpi-safe-value", "children"),
    Output("kpi-score-value", "children"),
    Output("kpi-best-value", "children"),
    Output("kpi-best-subtitle", "children"),
    Output("heatmap-chart", "figure"),
    Output("trend-chart", "figure"),
    Output("safety-chart", "figure"),
    Output("detail-table", "data"),
    Output("detail-table", "columns"),
    Output("map-date", "options"),
    Output("map-date", "value"),
    Input("data-store", "data"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("location-filter", "value"),
    State("map-date", "value"),
)
def update_dashboard(records, start_date, end_date, locations, current_map_date):
    """Recompute every visualization when data or filters change."""
    df = _filter_df(records, start_date, end_date, locations)

    kpis = make_kpi_values(df)
    heatmap = make_activity_heatmap(df)
    trend = make_trend_chart(df)
    safety = make_safety_distribution(df)

    if df.empty:
        table_data, table_cols = [], []
    else:
        table_df = df[["location_name", "date", "safety_status", "fish_activity_score",
                       "temperature_f", "wind_speed_mph", "precipitation_in",
                       "weather_description", "discharge_m3s", "flow_category"]].copy()
        for col, nd in [("temperature_f", 1), ("wind_speed_mph", 1),
                        ("precipitation_in", 2), ("discharge_m3s", 0)]:
            table_df[col] = pd.to_numeric(table_df[col], errors="coerce").round(nd)
        table_data = table_df.to_dict("records")
        table_cols = [
            {"name": "Location", "id": "location_name"},
            {"name": "Date", "id": "date"},
            {"name": "Safety", "id": "safety_status"},
            {"name": "Activity (0–100)", "id": "fish_activity_score"},
            {"name": "Temp (°F)", "id": "temperature_f", "type": "numeric"},
            {"name": "Wind (mph)", "id": "wind_speed_mph", "type": "numeric"},
            {"name": "Precip (in)", "id": "precipitation_in", "type": "numeric"},
            {"name": "Conditions", "id": "weather_description"},
            {"name": "Flow (m³/s)", "id": "discharge_m3s", "type": "numeric"},
            {"name": "Flow Category", "id": "flow_category"},
        ]

    if df.empty:
        date_options, current_map_date = [], None
    else:
        dates = sorted(df["date"].unique())
        date_options = [{"label": d.strftime("%a %b %d"), "value": d.isoformat()}
                        for d in dates]
        if current_map_date not in {o["value"] for o in date_options}:
            current_map_date = date_options[-1]["value"]

    return (kpis["locations"], kpis["safe_days"], kpis["avg_score"], kpis["best_spot"],
            kpis["best_score"],
            heatmap, trend, safety, table_data, table_cols, date_options, current_map_date)


@callback(
    Output("map-chart", "figure"),
    Input("data-store", "data"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("location-filter", "value"),
    Input("map-date", "value"),
)
def update_map(records, start_date, end_date, locations, map_date):
    df = _filter_df(records, start_date, end_date, locations)
    return make_location_map(df, map_date)


# ---------- Main ----------

if __name__ == "__main__":
    log.info("Starting dashboard at http://localhost:8050")
    log.info("Press Ctrl+C to stop the server.")
    app.run(debug=False, host="127.0.0.1", port=8050)