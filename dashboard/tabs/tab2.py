# tabs/tab2.py

import pandas as pd
import datetime
from dash import dcc, html, callback, Output, Input
import dash_bootstrap_components as dbc

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

_symbol_options = []
for _, row in _comp_df.iterrows():
    symbol = str(row.symbol).strip()
    name = str(row.name).strip()

    if symbol and name and symbol.lower() != "none" and name.lower() != "none":
        _symbol_options.append({
            "label": f"{symbol} – {name}",
            "value": symbol
        })

tab2_layout = html.Div([
    html.Div([
        html.H1("Tab2 - Données brutes"),
        
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(
                    id='tab2-symbol-dropdown',
                    options=_symbol_options,
                    multi=False,
                    placeholder="Choisir une action",
                    clearable=False,
                )
            ], width=6),
            dbc.Col([
                dcc.DatePickerRange(
                    id='tab2-date-picker',
                    start_date_placeholder_text="Date début",
                    end_date_placeholder_text="Date fin",
                    start_date=(datetime.date.today() - datetime.timedelta(days=2350)),
                    end_date=datetime.date.today(),
                    display_format='YYYY-MM-DD',
                )
            ], width=6)
        ], className="mb-4"),
        
        html.Div(id='tab2-table-container')
    ]),
])

def generate_html_table(df: pd.DataFrame):
    """Create a full HTML table from a DataFrame, formatted in French"""

    rename_columns = {
        "symbol": "Action",
        "date": "Date",
        "open": "Ouverture",
        "high": "Haut",
        "low": "Bas",
        "close": "Clôture",
        "volume": "Volume",
        "écart_type": "Écart type"
    }
    df_display = df.rename(columns=rename_columns)

    if "Écart type" in df_display.columns:
        df_display["Écart type"] = df_display["Écart type"].round(2)
    if "Date" in df_display.columns:
        df_display["Date"] = pd.to_datetime(df_display["Date"]).dt.date

    return html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(col) for col in df_display.columns])),
            html.Tbody([
                html.Tr([
                    html.Td(df_display.iloc[i][col]) for col in df_display.columns
                ]) for i in range(len(df_display))
            ])
        ], style={
            "width": "100%",
            "borderCollapse": "collapse",
            "border": "1px solid black"
        })
    ], style={
        "maxHeight": "500px",
        "overflowY": "scroll",
        "display": "block",
        "border": "1px solid grey",
        "padding": "5px"
    })


@app.callback(
    Output('tab2-table-container', 'children'),
    Input('tab2-symbol-dropdown', 'value'),
    Input('tab2-date-picker', 'start_date'),
    Input('tab2-date-picker', 'end_date'),
)
def update_table(selected_symbol, start_date, end_date):
    if not selected_symbol:
        return html.P("Veuillez sélectionner une action.")

    query = """
        SELECT c.symbol, s.date, s.open, s.high, s.low, s.close, s.volume
        FROM daystocks s
        JOIN companies c ON s.cid = c.id
        WHERE c.symbol = %s
    """
    params = [selected_symbol]

    if start_date and end_date:
        query += " AND s.date BETWEEN %s AND %s"
        params.extend([start_date, end_date])

    query += " ORDER BY s.date ASC"

    df = db.df_query(query, params=tuple(params))

    if df.empty:
        return html.P("Aucune donnée disponible pour cette sélection.")
    df['écart_type'] = df['close'].rolling(window=7, center=True, min_periods=1).std()

    return generate_html_table(df)

