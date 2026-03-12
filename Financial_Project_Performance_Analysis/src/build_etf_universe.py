from pathlib import Path
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table

console = Console()

BASE_URL = "https://www.borsaitaliana.it/borsa/etf.html"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def fetch_etf_page() -> str:
    """
    Scarica la pagina ETF.
    """
    response = requests.get(BASE_URL, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return response.text


def parse_etfs_from_html(html: str) -> list[dict]:
    """
    Estrae nome ETF dalla pagina.
    Questa parte potrebbe richiedere adattamenti se la struttura HTML è diversa.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 1:
            continue

        name_link = tds[0].find("a")
        if not name_link:
            continue

        name = name_link.get_text(strip=True)

        if name:
            rows.append(
                {
                    "ticker": "",
                    "name": name,
                    "type": "etf",
                    "market": "",
                    "source_page": "ETF",
                }
            )

    return rows


def build_etf_universe() -> pd.DataFrame:
    html = fetch_etf_page()
    rows = parse_etfs_from_html(html)

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(
            "Nessun ETF estratto. Probabilmente la struttura HTML della pagina ETF è diversa."
        )

    df = df.drop_duplicates(subset=["name"]).reset_index(drop=True)
    df = df.sort_values(by=["name"]).reset_index(drop=True)

    return df


def save_raw_etf_universe(df: pd.DataFrame) -> Path:
    output_path = get_project_root() / "data" / "raw" / "italy_etfs_raw.csv"
    df.to_csv(output_path, index=False)
    return output_path


def print_preview(df: pd.DataFrame, n: int = 15) -> None:
    table = Table(title="Preview universo ETF italiano")

    table.add_column("Name", style="white")
    table.add_column("Type", style="bold cyan")
    table.add_column("Market", style="cyan")
    table.add_column("Source", style="magenta")

    preview_df = df.head(n)

    for _, row in preview_df.iterrows():
        table.add_row(
            str(row["name"]),
            str(row["type"]),
            str(row["market"]),
            str(row["source_page"]),
        )

    console.print(table)
    console.print(f"\n[bold]Totale ETF estratti:[/bold] {len(df)}")


def main() -> None:
    df = build_etf_universe()
    output_path = save_raw_etf_universe(df)

    console.print(f"\n[green]File salvato in:[/green] {output_path}")
    print_preview(df)


if __name__ == "__main__":
    main()