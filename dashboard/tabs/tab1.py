# tabs/tab1.py

import datetime
from dash import html, dcc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import plotly.colors as pc

from app import app, db


# -------------------------------------------------------------------
# Préparer la liste des options (symbol + nom) pour le Dropdown
# -------------------------------------------------------------------
_comp_df = db.df_query(
    """
    SELECT DISTINCT c.id, c.symbol, c.name
    FROM companies c
    WHERE c.symbol IS NOT NULL AND c.symbol != ''
      AND c.id IN (
        SELECT cid FROM daystocks
        UNION
        SELECT cid FROM stocks
      )
    ORDER BY c.symbol
    """
)

colors = pc.qualitative.Plotly 
_symbol_options = []

for _, row in _comp_df.iterrows():
    symbol = str(row.symbol).strip()
    name = str(row.name).strip()

    # Vérifie que le symbol et le name existent et sont propres
    if symbol and name and symbol.lower() != "none" and name.lower() != "none":
        _symbol_options.append({
            "label": f"{symbol} – {name}",
            "value": symbol
        })


# -------------------------------------------------------------------
# Options pour l'intervalle : daystocks vs stocks (10 minutes)
# -------------------------------------------------------------------
_interval_options = [
    {"label": "Journalier", "value": "day"},
    {"label": "10 minutes", "value": "10min"},
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
                    value=[],
                    placeholder="Sélectionnez une action",
                    multi=True,   # <-- Add this
                    clearable=False,
                    style={"minWidth": "200px"}
                )
            ], style={"marginRight": "2rem"}),

            # Choix de la période
            html.Div([
                html.Label("Période"),
                dcc.DatePickerRange(
                    id="date-picker-range",
                    start_date=(datetime.date.today() - datetime.timedelta(days=2350)),
                    end_date=datetime.date.today(),
                    display_format="YYYY-MM-DD"
                )
            ], style={"marginRight": "2rem"}),

            # Choix de l'intervalle
            html.Div([
                html.Label("Intervalle"),
                dcc.RadioItems(
                    id="interval-type",
                    options=_interval_options,
                    value="day",
                    inline=True
                )
            ], style={"marginRight": "2rem"}),

            # Ligne ou Chandeliers
            html.Div([
                html.Label("Type de graphique"),
                dcc.RadioItems(
                    id="chart-type",
                    options=[
                        {"label": "Ligne", "value": "line"},
                        {"label": "Chandeliers", "value": "candlestick"},
                        {"label": "Bollinger", "value": "bollinger"},
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
                        {"label": "Log", "value": "log"},
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
# Callback pour mettre à jour le graphique (daystocks ou stocks)
# -------------------------------------------------------------------


@app.callback(
    Output("symbol-dropdown", "value"),
    Input("price-chart", "restyleData"),
    State("symbol-dropdown", "value"),
    prevent_initial_call=True
)
def update_dropdown_from_legend(restyle_data, selected_symbols):
    if restyle_data is None:
        return selected_symbols

    # Exemple de restyle_data :
    # [{'visible': ['legendonly']}, [2]]  --> ici, on a cliqué sur la trace index 2

    changed_visibility = restyle_data[0].get('visible', [])
    changed_indices = restyle_data[1]

    if not changed_visibility or not changed_indices:
        return selected_symbols

    # "legendonly" signifie que la courbe a été désactivée
    symbols_to_remove = []
    if changed_visibility[0] == 'legendonly':
        for idx in changed_indices:
            if isinstance(idx, int) and idx < len(selected_symbols):
                symbol = selected_symbols[idx]
                symbols_to_remove.append(symbol)

    # Enlève les symbols désactivés
    new_selection = [s for s in selected_symbols if s not in symbols_to_remove]
    return new_selection

@app.callback(
    Output("price-chart", "figure"),
    [
        Input("symbol-dropdown", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("chart-type", "value"),
        Input("yaxis-type", "value"),
        Input("interval-type", "value"),
    ],
)
def update_price_chart(symbols, start_date, end_date, chart_type, yaxis_type, interval_type):
    # symbols is now a list!
    if not symbols or not start_date or not end_date:
        return go.Figure()

    fig = go.Figure()

    for symbol in symbols:
        # Charger les données selon l'intervalle choisi
        if interval_type == "day":
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
        else:
            q = f"""
            SELECT s.date, s.value AS close
            FROM stocks s
            JOIN companies c ON s.cid = c.id
            WHERE c.symbol = '{symbol}'
              AND s.date >= '{start_date}'
              AND s.date <= '{end_date}'
            ORDER BY s.date;
            """
            df = db.df_query(q, parse_dates=["date"])
            if not df.empty:
                df["open"] = df["close"]
                df["high"] = df["close"]
                df["low"] = df["close"]

        # Skip if no data
        if df.empty:
            continue

        if chart_type == "line":
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["close"],
                mode="lines",
                name=symbol,
            ))

        elif chart_type == "candlestick":
            fig.add_trace(go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=symbol,
            ))

        elif chart_type == "bollinger":
            # Calcul des bandes de Bollinger
            window = 20  # Période de calcul
            if len(df) >= window:
                df['rolling_mean'] = df['close'].rolling(window=window).mean()
                df['rolling_std'] = df['close'].rolling(window=window).std()
                df['upper_band'] = df['rolling_mean'] + (2 * df['rolling_std'])
                df['lower_band'] = df['rolling_mean'] - (2 * df['rolling_std'])

                # Associer une couleur par action
                color = colors[symbols.index(symbol) % len(colors)]
                legend_group_name = f"bollinger_{symbol}"

                # Moyenne mobile (la seule visible dans la légende)
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["rolling_mean"],
                    mode="lines",
                    line=dict(color=color, dash="dash"),  # couleur unique
                    name=symbol,  # Visible dans la légende
                    legendgroup=legend_group_name,
                    showlegend=True
                ))

                # Bande supérieure
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["upper_band"],
                    mode="lines",
                    line=dict(color=color, width=0.5),  # même couleur, plus fin
                    name=f"{symbol} - Upper",
                    legendgroup=legend_group_name,
                    showlegend=False
                ))

                # Bande inférieure
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["lower_band"],
                    mode="lines",
                    line=dict(color=color, width=0.5),  # même couleur
                    name=f"{symbol} - Lower",
                    legendgroup=legend_group_name,
                    fill='tonexty',
                    fillcolor='rgba(0,0,0,0)',  # transparent ou léger
                    showlegend=False
                ))

                # Close
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["close"],
                    mode="lines",
                    line=dict(color=color, width=1, dash="dot"),
                    name=f"{symbol} - Close",
                    legendgroup=legend_group_name,
                    showlegend=False
                ))





    # Mise en forme générale
    fig.update_layout(
        title={
            'text': (
                f"Évolution des actions sélectionnées<br>"
                f"<sub>Astuce: cliquez sur une action dans la légende pour l'afficher/masquer</sub>"
            ),
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis_title="Date",
        yaxis_title="Prix",
        margin={"l": 40, "r": 20, "t": 70, "b": 40},
        template="plotly_white",
        yaxis_type=yaxis_type,
        legend_title="Actions",
        showlegend=True,
    )


    return fig
