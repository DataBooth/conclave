import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""
    # Papal Data Pipeline Notebook

    This notebook documents and executes the process of loading, cleaning, and previewing papal data tables into a persistent DuckDB database.

    **Data sources are cited with each table.**

    > **Last updated:** 2025-04-21
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd

    return mo, duckdb, pd


@app.cell
def _(duckdb):
    # Connect to persistent DuckDB database
    con = duckdb.connect("papal_data.duckdb")
    return con


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Popes Table

    **Source:** [ksreyes/popes (GitHub)](https://github.com/ksreyes/popes)  
    **Raw CSV:** https://raw.githubusercontent.com/ksreyes/popes/master/popes.csv  

    This table includes all popes from St. Peter to Francis.
    """)
    return


@app.cell
def _(con):
    # Load popes CSV directly from GitHub into DuckDB
    con.execute("""
        CREATE OR REPLACE TABLE popes AS
        FROM 'https://raw.githubusercontent.com/ksreyes/popes/master/popes.csv'
    """)
    popes_df = con.execute("SELECT * FROM popes LIMIT 5").fetchdf()
    return popes_df


@app.cell
def _(popes_df):
    popes_df
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Update Pope Francis's End Date

    (If needed, update the end date for Pope Francis.)
    """)
    return


@app.cell
def _(con):
    # Update Francis's end date
    con.execute("""
        UPDATE popes
        SET reign_ended = '2025-04-21'
        WHERE name = 'Francis'
    """)
    francis_row = con.execute("SELECT * FROM popes WHERE name = 'Francis'").fetchdf()
    return francis_row


@app.cell
def _(francis_row):
    francis_row
    return


# --- Conclaves Table ---


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Conclaves Table

    **Source:** [Wikipedia: List of Papal Conclaves](https://en.wikipedia.org/wiki/List_of_papal_conclaves)  
    *(You may need to pre-process or find a CSV version of this data. For demonstration, a placeholder URL is used below.)*
    """)
    return


@app.cell
def _(con):
    conclaves_csv_url = "https://raw.githubusercontent.com/YOUR-REPO/conclaves.csv"  # Replace with actual
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE conclaves AS
            FROM '{conclaves_csv_url}'
        """)
        conclaves_df = con.execute("SELECT * FROM conclaves LIMIT 5").fetchdf()
    except Exception as e:
        conclaves_df = f"Could not load conclaves table: {e}"
    return conclaves_df


@app.cell
def _(conclaves_df):
    conclaves_df
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Cardinals Table

    **Source:** [Wikipedia: List of current cardinals](https://en.wikipedia.org/wiki/List_of_current_cardinals)  
    *(You may need to pre-process or find a CSV version of this data. For demonstration, a placeholder URL is used below.)*
    """)
    return


@app.cell
def _(con):
    cardinals_csv_url = "https://raw.githubusercontent.com/YOUR-REPO/cardinals.csv"  # Replace with actual
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE cardinals AS
            FROM '{cardinals_csv_url}'
        """)
        cardinals_df = con.execute("SELECT * FROM cardinals LIMIT 5").fetchdf()
    except Exception as e:
        cardinals_df = f"Could not load cardinals table: {e}"
    return cardinals_df


@app.cell
def _(cardinals_df):
    cardinals_df
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Papal Documents Table

    **Source:** [Vatican.va](https://www.vatican.va/content/vatican/en.html)  
    *(You may need to pre-process or find a CSV version of this data. For demonstration, a placeholder URL is used below.)*
    """)
    return


@app.cell
def _(con):
    documents_csv_url = "https://raw.githubusercontent.com/YOUR-REPO/papal_documents.csv"  # Replace with actual
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE papal_documents AS
            FROM '{documents_csv_url}'
        """)
        documents_df = con.execute("SELECT * FROM papal_documents LIMIT 5").fetchdf()
    except Exception as e:
        documents_df = f"Could not load papal_documents table: {e}"
    return documents_df


@app.cell
def _(documents_df):
    documents_df
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data Sources and References

    - **Popes:** [ksreyes/popes (GitHub)](https://github.com/ksreyes/popes)
    - **Conclaves:** [Wikipedia: List of Papal Conclaves](https://en.wikipedia.org/wiki/List_of_papal_conclaves) *(CSV needed)*
    - **Cardinals:** [Wikipedia: List of current cardinals](https://en.wikipedia.org/wiki/List_of_current_cardinals) *(CSV needed)*
    - **Papal Documents:** [Vatican.va](https://www.vatican.va/content/vatican/en.html) *(CSV needed)*

    > Please ensure all data sources are cited and that you have permission to use and share these datasets.
    """)
    return


if __name__ == "__main__":
    app.run()
