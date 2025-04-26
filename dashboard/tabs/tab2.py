# tabs/tab2.py

import pandas as pd

from dash import dcc, html, callback, Output, Input, State
import dash.dependencies as ddep
import dash_bootstrap_components as dbc

from app import app, db

# -------------------------------------------------------------------
# Préparer la liste des options pour le Dropdown
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

_symbol_options = []
for _, row in _comp_df.iterrows():
    symbol = str(row.symbol).strip()
    name = str(row.name).strip()

    if symbol and name and symbol.lower() != "none" and name.lower() != "none":
        _symbol_options.append({
            "label": f"{symbol} – {name}",
            "value": symbol
        })


# -------------------------------------------------------------------
# Layout de Tab 2
# -------------------------------------------------------------------
tab2_layout = html.Div([
    html.Div([
        html.H1("Tab2 - Données brutes"),
        
        # Dropdowns for selecting actions and date
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(
                    id='tab2-symbol-dropdown',
                    options=_symbol_options,  # <= HERE, options are already filled
                    multi=True,
                    placeholder="Choisir des actions",
                    clearable=False,
                )
            ], width=6),
            dbc.Col([
                dcc.DatePickerRange(
                    id='tab2-date-picker',
                    start_date_placeholder_text="Date début",
                    end_date_placeholder_text="Date fin",
                    display_format='YYYY-MM-DD',
                )
            ], width=6)
        ], className="mb-4"),
        
        # Space for the output table
        html.Div(id='tab2-table-container')
    ]),
])

# ----------------------------------------------
# Helper function: Build HTML table
# ----------------------------------------------

def generate_html_table(df: pd.DataFrame, max_rows=20):
    """Create an HTML table from a DataFrame"""
    return html.Table([
        html.Thead(html.Tr([html.Th(col) for col in df.columns])),
        html.Tbody([
            html.Tr([
                html.Td(df.iloc[i][col]) for col in df.columns
            ]) for i in range(min(len(df), max_rows))
        ])
    ], style={"width": "100%", "borderCollapse": "collapse", "border": "1px solid black"})



# -------------------------------------------------------------------
# Callback pour charger et afficher les données brutes
# -------------------------------------------------------------------

@app.callback(
    Output('tab2-table-container', 'children'),
    Input('tab2-symbol-dropdown', 'value'),
    Input('tab2-date-picker', 'start_date'),
    Input('tab2-date-picker', 'end_date'),
)
def update_table(selected_symbols, start_date, end_date):
    if not selected_symbols:
        return html.P("Veuillez sélectionner une action.")

    if isinstance(selected_symbols, str):
        selected_symbols = [selected_symbols]  # Force to list

    # Build SQL query
    query = """
        SELECT c.symbol, s.date, s.open, s.high, s.low, s.close, s.volume
        FROM daystocks s
        JOIN companies c ON s.cid = c.id
        WHERE c.symbol = ANY(%s)
    """
    params = [selected_symbols]

    if start_date and end_date:
        query += " AND s.date BETWEEN %s AND %s"
        params.extend([start_date, end_date])

    query += " ORDER BY s.date DESC"

    # Execute the query and get the result
    df = db.df_query(query, params=tuple(params))

    if df.empty:
        return html.P("Aucune donnée disponible pour cette sélection.")

    # Return the generated table
    return generate_html_table(df)
