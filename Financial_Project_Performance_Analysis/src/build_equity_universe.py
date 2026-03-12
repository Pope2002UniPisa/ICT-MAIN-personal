from pathlib import Path
import string
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table

console = Console()

BASE_URL = "https://www.borsaitaliana.it/borsa/azioni/listino-a-z.html"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def fetch_letter_page(letter: str) -> str:
    """
    Scarica la pagina del listino A-Z per una specifica lettera.
    """
    params = {"initial": letter, "lang": "it"}
    response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return response.text


def parse_equities_from_html(html: str) -> list[dict]:
    """
    Estrae nome e mercato dalla tabella della pagina.
    Il ticker Yahoo NON è presente direttamente qui, quindi per ora salviamo:
    - name
    - market
    - type
    - source_letter
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Le righe utili hanno in genere link al nome titolo
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # prima colonna: nome titolo
        name_link = tds[0].find("a")
        if not name_link:
            continue

        name = name_link.get_text(strip=True)
        market = ""

        # spesso il mercato è mostrato in piccolo sotto il nome
        small_text = tds[0].get_text(" ", strip=True)
        if small_text.startswith(name):
            market = small_text.replace(name, "", 1).strip()

        if name:
            rows.append(
                {
                    "ticker": "",          # lo mapperemo dopo a Yahoo
                    "name": name,
                    "type": "equity",
                    "market": market,
                }
            )

    return rows


def build_equity_universe() -> pd.DataFrame:
    """
    Costruisce un DataFrame con tutte le azioni italiane dal listino A-Z.
    """
    all_rows = []

    for letter in string.ascii_uppercase:
        console.print(f"[cyan]Scarico lettera {letter}...[/cyan]")
        try:
            html = fetch_letter_page(letter)
            rows = parse_equities_from_html(html)

            for row in rows:
                row["source_letter"] = letter

            all_rows.extend(rows)
            time.sleep(0.5)

        except Exception as exc:
            console.print(f"[red]Errore sulla lettera {letter}: {exc}[/red]")

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise ValueError(
            "Nessun dato estratto dal listino A-Z. "
            "Controlla se la struttura della pagina è cambiata."
        )

    df = df.drop_duplicates(subset=["name"]).reset_index(drop=True)
    df = df.sort_values(by=["name"]).reset_index(drop=True)

    return df


def save_raw_equity_universe(df: pd.DataFrame) -> Path:
    output_path = get_project_root() / "data" / "raw" / "italy_equities_raw.csv"
    df.to_csv(output_path, index=False)
    return output_path


def print_preview(df: pd.DataFrame, n: int = 15) -> None:
    table = Table(title="Preview universo azionario italiano")

    table.add_column("Name", style="white")
    table.add_column("Type", style="bold green")
    table.add_column("Market", style="cyan")
    table.add_column("Letter", style="magenta")

    preview_df = df.head(n)

    for _, row in preview_df.iterrows():
        table.add_row(
            str(row["name"]),
            str(row["type"]),
            str(row["market"]),
            str(row["source_letter"]),
        )

    console.print(table)
    console.print(f"\n[bold]Totale azioni estratte:[/bold] {len(df)}")


def main() -> None:
    df = build_equity_universe()
    output_path = save_raw_equity_universe(df)

    console.print(f"\n[green]File salvato in:[/green] {output_path}")
    print_preview(df)


if __name__ == "__main__":
    main()