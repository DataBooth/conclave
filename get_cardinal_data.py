import httpx
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from io import StringIO

CACHE_FILE = Path("cardinals_page.html")


def flatten_col(col):
    if isinstance(col, tuple):
        # Join non-empty elements, strip and lower
        return "_".join(
            str(c).strip().replace(" ", "_").lower() for c in col if c and c != "nan"
        )
    else:
        return str(col).strip().replace(" ", "_").lower()


def get_cardinals_data(force_refresh: bool = False):
    """
    Fetches and parses the list of current cardinals from Wikipedia,
    caching the page content unless force_refresh is True.

    Args:
        force_refresh: If True, bypass the cache and fetch the page again.

    Returns:
        A pandas DataFrame containing the cardinals data.
    """

    if force_refresh or not CACHE_FILE.exists():
        url = "https://en.wikipedia.org/wiki/List_of_current_cardinals"
        resp = httpx.get(url)
        resp.raise_for_status()
        html = resp.text
        CACHE_FILE.write_text(html, encoding="utf-8")
        print("Page fetched and cached.")
    else:
        html = CACHE_FILE.read_text(encoding="utf-8")
        print("Page loaded from cache.")

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})

    cardinals_dfs = []
    for table in tables:
        # Use StringIO to avoid FutureWarning
        df = pd.read_html(StringIO(str(table)))[0]
        # Determine eligibility by inspecting each row's style
        eligibility = []
        for row in table.find_all("tr"):
            style = row.get("style", "")
            if "background:#FFCCCC" in style or "background-color:#FFCCCC" in style:
                eligibility.append(False)
            else:
                eligibility.append(True)
        eligibility = eligibility[1:]  # Skip header row
        if len(eligibility) == len(df):
            df["eligible"] = eligibility
        cardinals_dfs.append(df)

    cardinals = pd.concat(cardinals_dfs, ignore_index=True)
    cardinals.columns = [flatten_col(c) for c in cardinals.columns]
    return cardinals


def main(force_refresh: bool = False):
    cardinals_data = get_cardinals_data(force_refresh)
    print(cardinals_data.head())
    # Optionally save to CSV
    cardinals_data.to_csv("current_cardinals.csv", index=False)


if __name__ == "__main__":
    main(force_refresh=False)
