"""
Louisville Waterways Fishing — Plotly Dash Dashboard
====================================================
A web dashboard that reads the analytics-ready fishing_recommendation
fact table from Supabase PostgreSQL and visualizes the 7-day fishing
outlook across six Louisville-area locations.

Designed as the consumption layer on top of etl_pipeline.py. The ETL
populates the warehouse; this script reads from it.

Views included
--------------
- Four KPI summary cards (total locations, safe days, average activity
  score, best spot of the week)
- Activity heatmap: location x date, colored by fish_activity_score,
  hovertip shows weather + flood + safety detail
- Safety distribution: stacked bar of Safe / Caution / Unsafe day counts
  per location
- Interactive map of the six fishing locations colored by safety status
  for the currently-selected date
- Detail table with all weather + flood + recommendation columns,
  filterable and sortable

How to run
----------
    pip install dash plotly dash-bootstrap-components
    python src/dashboard.py

Then open http://localhost:8050 in your browser.

Requires the same .env at the project root that etl_pipeline.py uses.
Run etl_pipeline.py at least once before launching the dashboard so the
database has data to display.
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
from dash import Dash, Input, Output, State, callback, dash_table, dcc, html
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =====================================================================
# Logging
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dashboard")


# =====================================================================
# Paths
# =====================================================================
# Script lives in <project_root>/src/dashboard.py — walk up one level so
# we find the same .env etl_pipeline.py uses.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# =====================================================================
# Style constants
# =====================================================================
SAFETY_COLORS = {
    "Safe":    "#198754",  # green
    "Caution": "#ffc107",  # amber
    "Unsafe":  "#dc3545",  # red
}

CARD_STYLE = {
    "boxShadow": "0 2px 4px rgba(0,0,0,0.08)",
    "borderRadius": "8px",
    "border": "1px solid #e3e6ea",
}


# =====================================================================
# Database connection
# =====================================================================

def get_database_url() -> str:
    """Same connection logic the ETL uses. Reads .env from project root."""
    load_dotenv(PROJECT_ROOT / ".env")

    user = os.getenv("user")
    password = os.getenv("password")
    host = os.getenv("host")
    port = os.getenv("port", "5432")
    dbname = os.getenv("dbname")

    missing = [
        name for name, value in {
            "user": user, "password": password, "host": host, "dbname": dbname,
        }.items() if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing values in .env for: {', '.join(missing)}. "
            "Expected user, password, host, port, dbname."
        )

    safe_password = quote_plus(password)
    return f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}"


# =====================================================================
# Data loading
# =====================================================================

# The analytics-ready join — same query as the example in the README.
# Returns one denormalized row per (location, date).
ANALYTICS_QUERY = text("""
    SELECT
        l.location_id,
        l.location_name,
        l.city,
        l.latitude,
        l.longitude,
        r.date,
        r.safety_status,
        r.fish_activity_score,
        w.temperature_f,
        w.wind_speed_mph,
        w.precipitation_in,
        w.pressure_hpa,
        w.weather_code,
        wc.description AS weather_description,
        wc.category AS weather_category,
        f.discharge_m3s,
        f.flow_category
    FROM public.fishing_recommendation r
    JOIN public.location          l  ON l.location_id  = r.location_id
    JOIN public.weather_data      w  ON w.weather_id   = r.weather_id
    LEFT JOIN public.weather_code wc ON wc.code        = w.weather_code
    JOIN public.river_flood_data  f  ON f.flood_id     = r.flood_id
    ORDER BY r.date, l.location_name;
""")


def load_data() -> pd.DataFrame:
    """Pull the analytics dataset from Supabase. Returns an empty
    DataFrame (not None) if anything goes wrong so the dashboard can
    show a friendly empty state instead of crashing."""
    try:
        engine = create_engine(get_database_url())
        df = pd.read_sql(ANALYTICS_QUERY, engine)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        log.info("Loaded %d rows from fishing_recommendation join", len(df))
        return df
    except Exception as e:
        log.error("Failed to load data: %s", e)
        return pd.DataFrame()


# =====================================================================
# Figure factories
# =====================================================================
# Each function takes a (possibly filtered) DataFrame and returns a
# Plotly figure. They all handle empty input by returning a placeholder
# annotation instead of crashing.
# =====================================================================

def empty_figure(message: str = "No data — run etl_pipeline.py first") -> go.Figure:
    """A blank figure with a centered message, used when data is empty."""
    fig = go.Figure()
    fig.add_annotation(
        text=message, showarrow=False, font={"size": 14, "color": "#6c757d"},
        xref="paper", yref="paper", x=0.5, y=0.5,
    )
    fig.update_layout(
        xaxis={"visible": False}, yaxis={"visible": False},
        plot_bgcolor="white", margin={"t": 30, "b": 30, "l": 30, "r": 30},
    )
    return fig


def make_activity_heatmap(df: pd.DataFrame) -> go.Figure:
    """Location (Y) x Date (X) heatmap, colored by fish_activity_score.
    Hover shows the full weather + flood + safety detail for each cell."""
    if df.empty:
        return empty_figure()

    # Pivot to grid form for the heatmap, keep the underlying detail for
    # customdata so the hovertip can show everything.
    pivot = df.pivot(index="location_name", columns="date", values="fish_activity_score")

    # Pull every per-cell field we want to show in the tooltip
    def pivot_field(col):
        return df.pivot(index="location_name", columns="date", values=col)

    detail_safety = pivot_field("safety_status").values
    detail_temp = pivot_field("temperature_f").values
    detail_wind = pivot_field("wind_speed_mph").values
    detail_precip = pivot_field("precipitation_in").values
    detail_flow = pivot_field("discharge_m3s").values

    # Stack the per-cell detail arrays so they can be referenced by
    # %{customdata[0]}, %{customdata[1]}, ... in the hovertemplate.
    customdata = []
    for i in range(pivot.shape[0]):
        row = []
        for j in range(pivot.shape[1]):
            row.append([
                detail_safety[i][j],
                detail_temp[i][j],
                detail_wind[i][j],
                detail_precip[i][j],
                detail_flow[i][j],
            ])
        customdata.append(row)

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[d.isoformat() for d in pivot.columns],
        y=pivot.index,
        colorscale="RdYlGn",
        zmin=0,
        zmax=100,
        customdata=customdata,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Date: %{x}<br>"
            "Activity Score: <b>%{z:.0f}</b>/100<br>"
            "Safety: %{customdata[0]}<br>"
            "Temp: %{customdata[1]:.1f}°F<br>"
            "Wind: %{customdata[2]:.1f} mph<br>"
            "Precip: %{customdata[3]:.2f} in<br>"
            "Flow: %{customdata[4]:.0f} m³/s<extra></extra>"
        ),
        colorbar={"title": "Activity<br>Score", "len": 0.8},
    ))

    fig.update_layout(
        title="Fish Activity Forecast (0–100, higher is better)",
        xaxis_title=None,
        yaxis_title=None,
        plot_bgcolor="white",
        margin={"t": 50, "b": 40, "l": 10, "r": 10},
        height=380,
    )
    return fig


def make_safety_distribution(df: pd.DataFrame) -> go.Figure:
    """Stacked bar: per location, count of Safe / Caution / Unsafe days."""
    if df.empty:
        return empty_figure()

    counts = (
        df.groupby(["location_name", "safety_status"]).size()
        .reset_index(name="days")
    )

    fig = px.bar(
        counts,
        x="days",
        y="location_name",
        color="safety_status",
        orientation="h",
        color_discrete_map=SAFETY_COLORS,
        category_orders={"safety_status": ["Safe", "Caution", "Unsafe"]},
        labels={"days": "Days", "location_name": "", "safety_status": "Safety"},
    )
    fig.update_layout(
        title="Safety Status Distribution",
        plot_bgcolor="white",
        legend={"orientation": "h", "y": -0.2, "x": 0.5, "xanchor": "center"},
        margin={"t": 50, "b": 40, "l": 10, "r": 10},
        height=380,
    )
    fig.update_xaxes(gridcolor="#eee")
    return fig


def make_location_map(df: pd.DataFrame, selected_date) -> go.Figure:
    """Map of the six Louisville-area fishing locations, dot color = safety
    status on the chosen date. If no date is selected, use the most recent."""
    if df.empty:
        return empty_figure()

    if selected_date is None:
        selected_date = df["date"].max()
    else:
        # When coming from a callback this may arrive as an ISO string
        if isinstance(selected_date, str):
            selected_date = pd.to_datetime(selected_date).date()

    snapshot = df[df["date"] == selected_date].copy()
    if snapshot.empty:
        return empty_figure(f"No data for {selected_date}")

    fig = px.scatter_mapbox(
        snapshot,
        lat="latitude",
        lon="longitude",
        color="safety_status",
        color_discrete_map=SAFETY_COLORS,
        hover_name="location_name",
        hover_data={
            "city": True,
            "fish_activity_score": True,
            "temperature_f": ":.1f",
            "wind_speed_mph": ":.1f",
            "discharge_m3s": ":.0f",
            "latitude": False,
            "longitude": False,
            "safety_status": False,
        },
        zoom=8.5,
        size_max=25,
        size=[20] * len(snapshot),
        category_orders={"safety_status": ["Safe", "Caution", "Unsafe"]},
    )
    fig.update_layout(
        title=f"Locations — {selected_date.strftime('%a %b %d')}",
        mapbox_style="open-street-map",  # no token required
        margin={"t": 50, "b": 0, "l": 0, "r": 0},
        height=380,
        legend={"orientation": "h", "y": -0.05, "x": 0.5, "xanchor": "center"},
    )
    return fig


def make_kpi_values(df: pd.DataFrame) -> dict:
    """Compute the four headline metrics for the KPI cards."""
    if df.empty:
        return {
            "locations": "—", "safe_days": "—",
            "avg_score": "—", "best_spot": "—",
        }
    total_rows = len(df)
    safe_rows = int((df["safety_status"] == "Safe").sum())
    avg_score = df["fish_activity_score"].mean()

    # Best spot of the week = (location, date) with the highest score
    best = df.loc[df["fish_activity_score"].idxmax()]
    best_label = (
        f"{best['location_name']} · {best['fish_activity_score']:.0f}"
    )

    return {
        "locations": str(df["location_name"].nunique()),
        "safe_days": f"{safe_rows}/{total_rows}",
        "avg_score": f"{avg_score:.0f}",
        "best_spot": best_label,
    }


# =====================================================================
# Layout components
# =====================================================================

def kpi_card(card_id: str, title: str, value_id: str, subtitle: str = "") -> dbc.Card:
    """A single KPI tile. Value is populated by a callback."""
    return dbc.Card(
        dbc.CardBody([
            html.Div(title, className="text-muted small text-uppercase"),
            html.H3(id=value_id, className="mt-2 mb-0"),
            html.Div(subtitle, className="text-muted small mt-1") if subtitle else None,
        ]),
        id=card_id,
        style=CARD_STYLE,
        className="h-100",
    )


# =====================================================================
# Dash app
# =====================================================================
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Louisville Fishing Dashboard",
)
server = app.server  # for deployment, e.g. gunicorn

# Initial load so we know the date bounds for the picker.
_initial_df = load_data()
if _initial_df.empty:
    log.warning("Starting with no data. Run etl_pipeline.py to populate the warehouse.")
    _min_date = _max_date = pd.Timestamp.today().date()
    _location_options = []
else:
    _min_date = _initial_df["date"].min()
    _max_date = _initial_df["date"].max()
    _location_options = [
        {"label": name, "value": name}
        for name in sorted(_initial_df["location_name"].unique())
    ]


# =====================================================================
# Layout
# =====================================================================
app.layout = dbc.Container([

    # ---------- Header ----------
    dbc.Row([
        dbc.Col([
            html.H2("Louisville Waterways Fishing Outlook", className="mb-0"),
            html.Div(
                "7-day forecast combining weather + river flow for six "
                "Louisville-area fishing locations",
                className="text-muted",
            ),
        ], md=8),
        dbc.Col([
            dbc.Button(
                [html.I(className="bi bi-arrow-clockwise me-2"), "Refresh data"],
                id="refresh-button",
                color="primary",
                className="float-end",
            ),
        ], md=4, className="d-flex align-items-center"),
    ], className="my-3"),

    html.Hr(),

    # ---------- Filters ----------
    dbc.Row([
        dbc.Col([
            html.Label("Date range", className="small text-muted"),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=_min_date,
                max_date_allowed=_max_date,
                start_date=_min_date,
                end_date=_max_date,
                display_format="MMM D",
                className="d-block",
            ),
        ], md=6),
        dbc.Col([
            html.Label("Locations", className="small text-muted"),
            dcc.Dropdown(
                id="location-filter",
                options=_location_options,
                value=[opt["value"] for opt in _location_options],
                multi=True,
                placeholder="All locations",
            ),
        ], md=6),
    ], className="mb-3"),

    # ---------- KPI Cards ----------
    dbc.Row([
        dbc.Col(kpi_card("kpi-locations", "Locations Tracked", "kpi-locations-value"), md=3),
        dbc.Col(kpi_card("kpi-safe", "Safe Day Slots", "kpi-safe-value",
                          "Out of all (location × day) cells"), md=3),
        dbc.Col(kpi_card("kpi-score", "Avg Activity Score", "kpi-score-value",
                          "0–100, higher is better"), md=3),
        dbc.Col(kpi_card("kpi-best", "Best Spot This Week", "kpi-best-value",
                          "Location · Score"), md=3),
    ], className="g-3 mb-3"),

    # ---------- Heatmap ----------
    dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody(dcc.Graph(id="heatmap-chart")),
                style=CARD_STYLE,
            ),
            width=12,
        ),
    ], className="mb-3"),

    # ---------- Safety distribution + Map ----------
    dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody(dcc.Graph(id="safety-chart")),
                style=CARD_STYLE, className="h-100",
            ),
            md=6,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.Label("Map date", className="small text-muted me-2"),
                        dcc.Dropdown(
                            id="map-date",
                            options=[],  # populated by callback
                            clearable=False,
                            style={"width": "200px", "display": "inline-block"},
                        ),
                    ], className="mb-2"),
                    dcc.Graph(id="map-chart"),
                ]),
                style=CARD_STYLE, className="h-100",
            ),
            md=6,
        ),
    ], className="mb-3"),

    # ---------- Detail Table ----------
    dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H5("Detailed Forecast", className="mb-3"),
                    dash_table.DataTable(
                        id="detail-table",
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "fontFamily": "Arial, sans-serif",
                            "fontSize": 13,
                            "padding": "8px",
                            "textAlign": "left",
                        },
                        style_header={
                            "backgroundColor": "#f8f9fa",
                            "fontWeight": "bold",
                            "borderBottom": "2px solid #dee2e6",
                        },
                        style_data_conditional=[
                            {"if": {"filter_query": '{safety_status} = "Safe"'},
                             "backgroundColor": "#d1e7dd"},
                            {"if": {"filter_query": '{safety_status} = "Caution"'},
                             "backgroundColor": "#fff3cd"},
                            {"if": {"filter_query": '{safety_status} = "Unsafe"'},
                             "backgroundColor": "#f8d7da"},
                        ],
                    ),
                ]),
                style=CARD_STYLE,
            ),
            width=12,
        ),
    ], className="mb-4"),

    # ---------- Footer ----------
    html.Hr(),
    html.Div(
        "Data: Open-Meteo Forecast & Flood APIs · Pipeline: etl_pipeline.py · "
        "Refresh re-queries Supabase PostgreSQL",
        className="text-muted small text-center mb-4",
    ),

    # ---------- Hidden state ----------
    # Cached DataFrame so filter callbacks don't hit the DB on every change.
    # The Refresh button updates this store; everything else reads from it.
    dcc.Store(id="data-store"),

], fluid=True)


# =====================================================================
# Callbacks
# =====================================================================

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
    """Re-query Postgres and update every filter's allowed range.
    Fires on page load (n_clicks=None) and every time the button is clicked."""
    df = load_data()

    if df.empty:
        today = pd.Timestamp.today().date()
        return [], today, today, today, today, [], []

    min_d = df["date"].min()
    max_d = df["date"].max()
    locations = sorted(df["location_name"].unique())
    options = [{"label": n, "value": n} for n in locations]

    # Store as JSON-serializable records; isoformat dates so JSON survives
    df_out = df.copy()
    df_out["date"] = df_out["date"].astype(str)
    return (
        df_out.to_dict("records"),
        min_d, max_d, min_d, max_d,
        options, locations,
    )


def _filter_df(records, start_date, end_date, locations) -> pd.DataFrame:
    """Apply the dashboard filters to the cached records."""
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
    Output("kpi-locations-value", "children"),
    Output("kpi-safe-value", "children"),
    Output("kpi-score-value", "children"),
    Output("kpi-best-value", "children"),
    Output("heatmap-chart", "figure"),
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
    """Recompute every visualization when the data or filters change."""
    df = _filter_df(records, start_date, end_date, locations)

    kpis = make_kpi_values(df)
    heatmap = make_activity_heatmap(df)
    safety = make_safety_distribution(df)

    # Detail table — present a friendly subset of columns, rounded.
    if df.empty:
        table_data = []
        table_cols = []
    else:
        table_df = df[[
            "location_name", "date", "safety_status", "fish_activity_score",
            "temperature_f", "wind_speed_mph", "precipitation_in",
            "weather_description", "discharge_m3s", "flow_category",
        ]].copy()
        # Round numerics so the table doesn't drown in decimals
        for col, nd in [
            ("temperature_f", 1), ("wind_speed_mph", 1),
            ("precipitation_in", 2), ("discharge_m3s", 0),
        ]:
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

    # Map-date dropdown options come from the filtered date range
    date_options = []
    if not df.empty:
        dates = sorted(df["date"].unique())
        date_options = [
            {"label": d.strftime("%a %b %d"), "value": d.isoformat()}
            for d in dates
        ]
        # If the previously-selected date is no longer in range, fall back
        # to the most recent date available.
        valid_values = {opt["value"] for opt in date_options}
        if current_map_date not in valid_values:
            current_map_date = date_options[-1]["value"]
    else:
        current_map_date = None

    return (
        kpis["locations"],
        kpis["safe_days"],
        kpis["avg_score"],
        kpis["best_spot"],
        heatmap,
        safety,
        table_data,
        table_cols,
        date_options,
        current_map_date,
    )


@callback(
    Output("map-chart", "figure"),
    Input("data-store", "data"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("location-filter", "value"),
    Input("map-date", "value"),
)
def update_map(records, start_date, end_date, locations, map_date):
    """Map is a separate callback because it depends on its own date
    dropdown in addition to the global filters."""
    df = _filter_df(records, start_date, end_date, locations)
    return make_location_map(df, map_date)


# =====================================================================
# Main
# =====================================================================
if __name__ == "__main__":
    log.info("Starting dashboard at http://localhost:8050")
    log.info("Press Ctrl+C to stop the server.")
    app.run(debug=False, host="0.0.0.0", port=8050)
