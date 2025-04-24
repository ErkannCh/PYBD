# tabs/tab1.py
import datetime
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd

from app import app, db

# -------------------------------------------------------------------
# Préparer la liste des options (symbol + nom) pour le Dropdown
# -------------------------------------------------------------------
_comp_df = db.df_query(
    """
    SELECT DISTINCT c.id, c.symbol, c.name
    FROM companies c
    JOIN daystocks d ON c.id = d.cid
    ORDER BY c.symbol
    """
)
_symbol_options = [
    {"label": f"{row.symbol} – {row.name}", "value": row.symbol}
    for _, row in _comp_df.iterrows()
]

# -------------------------------------------------------------------
# Layout du premier onglet
# -------------------------------------------------------------------
tab1_layout = html.Div([
    html.Br(),
    html.Div(
        className="d-flex flex-wrap",
        children=[
            # Choix de l'action
            html.Div([
                html.Label("Action"),
                dcc.Dropdown(
                    id="symbol-dropdown",
                    options=_symbol_options,
                    value=_symbol_options[0]["value"] if _symbol_options else None,
                    clearable=False,
                    style={"minWidth": "200px"}
                )
            ], style={"marginRight": "2rem"}),

            # Choix de la période
            html.Div([
                html.Label("Période"),
                dcc.DatePickerRange(
                    id="date-picker-range",
                    start_date=(datetime.date.today() - datetime.timedelta(days=3000)),
                    end_date=datetime.date.today(),
                    display_format="YYYY-MM-DD"
                )
            ], style={"marginRight": "2rem"}),

            # Ligne ou Chandeliers
            html.Div([
                html.Label("Type de graphique"),
                dcc.RadioItems(
                    id="chart-type",
                    options=[
                        {"label": "Ligne", "value": "line"},
                        {"label": "Chandeliers", "value": "candlestick"}
                    ],
                    value="line",
                    inline=True
                )
            ], style={"marginRight": "2rem"}),

            # Échelle Y
            html.Div([
                html.Label("Échelle Y"),
                dcc.RadioItems(
                    id="yaxis-type",
                    options=[
                        {"label": "Linéaire", "value": "linear"},
                        {"label": "Log", "value": "log"}
                    ],
                    value="linear",
                    inline=True
                )
            ]),
        ]
    ),

    html.Hr(),

    # Graphique
    dcc.Graph(id="price-chart", config={"displayModeBar": True})
])

# -------------------------------------------------------------------
# Callback pour mettre à jour le graphique
# -------------------------------------------------------------------
@app.callback(
    Output("price-chart", "figure"),
    [
        Input("symbol-dropdown", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("chart-type", "value"),
        Input("yaxis-type", "value"),
    ],
)
def update_price_chart(symbol, start_date, end_date, chart_type, yaxis_type):
    # Pas de sélection ? on renvoie une figure vide
    if not symbol or not start_date or not end_date:
        return go.Figure()
    # Récupérer les données daystocks pour ce symbol
    q = f"""
		SELECT ds.date, ds.open, ds.high, ds.low, ds.close
		FROM daystocks ds
		JOIN companies c ON ds.cid = c.id
		WHERE c.symbol = '{symbol}'
		AND ds.date >= '{start_date}'
		AND ds.date <= '{end_date}'
		ORDER BY ds.date;
    """
    df = db.df_query(q, parse_dates=["date"])
    if df.empty:
        return go.Figure().add_annotation(
            text="Aucune donnée pour cette période.",
            showarrow=False
        )

    # Construire la figure
    if chart_type == "line":
        fig = go.Figure(
            go.Scatter(
                x=df["date"],
                y=df["close"],
                mode="lines",
                name=symbol
            )
        )
    else:  # chandeliers
        fig = go.Figure(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=symbol
            )
        )

    # Mise en forme
    fig.update_layout(
        title=f"{symbol} du {pd.to_datetime(start_date).date()} au {pd.to_datetime(end_date).date()}",
        xaxis_title="Date",
        yaxis_title="Prix",
        margin={"l":40, "r":20, "t":50, "b":40},
        template="plotly_white"
    )
    fig.update_yaxes(type=yaxis_type)

    return fig
