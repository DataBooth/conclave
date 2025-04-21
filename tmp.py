import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""Introductory text""")
    return


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _(pd):
    df = pd.DataFrame([])
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
