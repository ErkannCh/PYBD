# tabs/tab1.py

import datetime
from dash import html, dcc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import plotly.colors as pc

from app import app, db


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

    if symbol and name and symbol.lower() != "none" and name.lower() != "none":
        _symbol_options.append({
            "label": f"{symbol} – {name}",
            "value": symbol
        })



tab1_layout = html.Div([
    html.Br(),
    html.Div(
        className="d-flex flex-wrap",
        children=[

            html.Div([
                html.Label("Action"),
                dcc.Dropdown(
                    id="symbol-dropdown",
                    options=_symbol_options,
                    value=[],
                    placeholder="Sélectionnez une action",
                    multi=True,
                    clearable=False,
                    style={"minWidth": "200px"}
                )
            ], style={"marginRight": "2rem"}),

            html.Div([
                html.Label("Période"),
                dcc.DatePickerRange(
                    id="date-picker-range",
                    start_date=(datetime.date.today() - datetime.timedelta(days=2350)),
                    end_date=datetime.date.today(),
                    display_format="YYYY-MM-DD"
                )
            ], style={"marginRight": "2rem"}),

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

            html.Div([
                html.Label("Indicateurs Techniques"),
                dcc.Checklist(
                    id="technical-indicators",
                    options=[
                        {"label": "SMA (20)", "value": "sma20"},
                        {"label": "EMA (20)", "value": "ema20"},
                        {"label": "RSI (14)", "value": "rsi14"},
                    ],
                    value=[],
                    inline=True,
                )
            ], style={"marginRight": "2rem"}),

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
    dcc.Graph(id="price-chart", config={"displayModeBar": True})
])


@app.callback(
    Output("symbol-dropdown", "value"),
    Input("price-chart", "restyleData"),
    State("symbol-dropdown", "value"),
    prevent_initial_call=True
)
def update_dropdown_from_legend(restyle_data, selected_symbols):
    if restyle_data is None:
        return selected_symbols

    changed_visibility = restyle_data[0].get('visible', [])
    changed_indices = restyle_data[1]

    if not changed_visibility or not changed_indices:
        return selected_symbols

    symbols_to_remove = []
    

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
        Input("technical-indicators", "value"),
    ],
)
def update_price_chart(symbols, start_date, end_date, chart_type, yaxis_type, technical_indicators):

    if not symbols or not start_date or not end_date:
        return go.Figure()

    fig = go.Figure()

    for symbol in symbols:
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
            window = 20 
            if len(df) >= window:
                df['rolling_mean'] = df['close'].rolling(window=window).mean()
                df['rolling_std'] = df['close'].rolling(window=window).std()
                df['upper_band'] = df['rolling_mean'] + (2 * df['rolling_std'])
                df['lower_band'] = df['rolling_mean'] - (2 * df['rolling_std'])

                color = colors[symbols.index(symbol) % len(colors)]
                legend_group_name = f"bollinger_{symbol}"

                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["rolling_mean"],
                    mode="lines",
                    line=dict(color=color, dash="dash"),
                    name=symbol,
                    legendgroup=legend_group_name,
                    showlegend=True
                ))

                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["upper_band"],
                    mode="lines",
                    line=dict(color=color, width=0.5),
                    name=f"{symbol} - Upper",
                    legendgroup=legend_group_name,
                    showlegend=False
                ))

                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["lower_band"],
                    mode="lines",
                    line=dict(color=color, width=0.5),
                    name=f"{symbol} - Lower",
                    legendgroup=legend_group_name,
                    fill='tonexty',
                    fillcolor='rgba(0,0,0,0)',
                    showlegend=False
                ))

                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["close"],
                    mode="lines",
                    line=dict(color=color, width=1, dash="dot"),
                    name=f"{symbol} - Close",
                    legendgroup=legend_group_name,
                    showlegend=False
                ))
        if "sma20" in technical_indicators and len(df) >= 20:
            df["SMA20"] = df["close"].rolling(window=20).mean()
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["SMA20"],
                mode="lines",
                line=dict(dash="dash", color="blue"),
                name=f"{symbol} - SMA20"
            ))

        if "ema20" in technical_indicators and len(df) >= 20:
            df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["EMA20"],
                mode="lines",
                line=dict(dash="dot", color="green"),
                name=f"{symbol} - EMA20"
            ))

        if "rsi14" in technical_indicators and len(df) >= 14:
            delta = df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            df["RSI"] = 100 - (100 / (1 + rs))
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["RSI"],
                mode="lines",
                line=dict(color="red"),
                name=f"{symbol} - RSI14"
            ))

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
        showlegend=True
    )



    return fig
