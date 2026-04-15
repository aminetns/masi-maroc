#!/usr/bin/env python3
"""
Script d'injection automatique des cours BVC → Supabase
Tourne chaque jour via GitHub Actions à 17h00 (heure Maroc)
"""

import requests
import json
from datetime import date
from bs4 import BeautifulSoup

SUPA_URL = "https://cctylvtdqcxqmkoynavr.supabase.co"
SUPA_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNjdHlsdnRkcWN4cW1rb3luYXZyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYxNzM4OTUsImV4cCI6MjA5MTc0OTg5NX0.VZpYItIJ0CXlGYHvylaMExwZQ8dyAiDoNro3xiqrKdw"

# Mapping nom BVC → ticker Supabase
NAME_TICKER = {
    "MANAGEM": "MNG", "ATTIJARIWAFA BANK": "ATW", "MAROC TELECOM": "IAM",
    "ITISSALAT AL-MAGHRIB": "IAM", "BCP": "BCP", "BANK OF AFRICA": "BOA",
    "COSUMAR": "CSR", "LAFARGEHOLCIM MAROC": "LHM", "CIMENTS DU MAROC": "CMA",
    "LABEL VIE": "LBV", "WAFA ASSURANCE": "WAA", "AFRIQUIA GAZ": "GAZ",
    "SGTM": "TGC", "TGCC": "TGCC", "CMT": "CMT", "AKDITAL": "AKT",
    "ARADEI CAPITAL": "ADH", "ALLIANCES": "ADI", "JET CONTRACTORS": "JET",
    "TAQA MOROCCO": "TMA", "CIH BANK": "CIH", "CREDIT DU MAROC": "CDM",
    "CDM": "CDM", "BMCI": "BCI", "ATLANTASANAD": "ATL", "SANLAM MAROC": "SAH",
    "DELTA HOLDING": "DHO", "SONASID": "SID", "ALUMINIUM MAROC": "ALM",
    "RISMA": "RIS", "SMI": "SMI", "SALAFIN": "SLF", "EQDOM": "EQD",
    "MAGHREBAIL": "MAB", "SOTHEMA": "SOT", "MUTANDIS": "MUT",
    "STE BOISSONS DU MAROC": "SBM", "OULMES": "UMR", "LESIEUR CRISTAL": "LES",
    "DISWAY": "DWY", "DISTY TECHNOLOGIES": "DYT", "AUTO HALL": "ATH",
    "AUTO NEJMA": "NEJ", "AGMA": "AGMA", "AFMA": "AFM", "HPS": "HPS",
    "MICRODATA": "MIC", "IMMORENTE INVEST": "IMO", "COLORADO": "COL",
    "ADDOHA": "ARD", "CMGP GROUP": "CTM", "PROMOPHARM": "PRO",
    "FENIE BROSSETTE": "FBR", "CFG BANK": "CFG", "VICENNE": "VIC",
    "STROC INDUSTRIE": "STR", "M2M GROUP": "M2M", "S2M": "S2M",
    "INVOLYS": "INV", "TOTALENERGIES MARKETING MAROC": "TTE",
    "SNEP": "SNP", "ZELLIDJA": "ZDJ", "AFRIC INDUSTRIES": "AFI",
    "IB MAROC": "IBC", "RESID DAR SAADA": "RDS", "MARSA MAROC": "MSA",
    "SODEP": "MSA", "CASH PLUS": "CSH", "DARI COUSPATE": "DRI",
    "MAGHREB OXYGENE": "MOX", "MAROC LEASING": "MLE", "BALIMA": "BAL",
    "MED PAPER": "MDP", "SRM": "SRM", "CTM": "CTM2", "CARTIER SAADA": "CAR",
    "UNIMER": "UMR", "STOKVIS NORD AFRIQUE": "SNA", "REBAB COMPANY": "REB",
    "IBMAROC.COM": "IBC"
}

def fetch_bvc_data():
    """Scrape les cours depuis le site BVC mobile"""
    url = "https://www.casablanca-bourse.com/bourseweb/Resume-Seance.aspx?Cat=1&IdLink=301"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9",
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        results = []
        
        # Parser les tableaux du site BVC
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue
                try:
                    nom = cols[0].get_text(strip=True).upper()
                    cours_str = cols[1].get_text(strip=True).replace(" ", "").replace(",", ".")
                    var_str = cols[2].get_text(strip=True).replace("%", "").replace(",", ".").replace("+", "")
                    vol_str = cols[4].get_text(strip=True).replace(" ", "").replace(",", ".")
                    
                    cours = float(cours_str) if cours_str else None
                    variation = float(var_str) if var_str else None
                    volume = int(float(vol_str)) if vol_str else None
                    
                    if not cours:
                        continue
                    
                    # Chercher le ticker
                    ticker = None
                    for key, val in NAME_TICKER.items():
                        if key in nom or nom in key:
                            ticker = val
                            break
                    
                    if ticker:
                        results.append({
                            "ticker": ticker,
                            "cours": cours,
                            "variation": variation,
                            "volume": volume,
                        })
                except Exception:
                    continue
        
        return results
    except Exception as e:
        print(f"Erreur scraping BVC: {e}")
        return []

def inject_to_supabase(data):
    """Injecter les cours dans Supabase"""
    today = str(date.today())
    headers = {
        "Content-Type": "application/json",
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
        "Prefer": "return=minimal"
    }
    
    ok = 0
    fail = 0
    
    for row in data:
        # market_data
        payload = {
            "ticker": row["ticker"],
            "cours": row["cours"],
            "variation": row["variation"],
            "volume": row["volume"],
            "source": "bvc_auto",
            "trade_date": today
        }
        r = requests.post(
            f"{SUPA_URL}/rest/v1/market_data",
            headers=headers,
            json=payload,
            timeout=10
        )
        
        # price_history (upsert)
        r2 = requests.post(
            f"{SUPA_URL}/rest/v1/price_history",
            headers={**headers, "Prefer": "resolution=merge-duplicates"},
            json={"ticker": row["ticker"], "trade_date": today, "cours": row["cours"], "volume": row["volume"]},
            timeout=10
        )
        
        if r.status_code in (200, 201, 204):
            ok += 1
            print(f"  ✅ {row['ticker']}: {row['cours']} MAD ({row['variation']:+.2f}%)")
        else:
            fail += 1
            print(f"  ❌ {row['ticker']}: HTTP {r.status_code}")
    
    print(f"\n✅ {ok} injectés | ❌ {fail} échecs")

if __name__ == "__main__":
    print("🔄 Récupération cours BVC...")
    data = fetch_bvc_data()
    print(f"📊 {len(data)} valeurs trouvées")
    
    if data:
        print("💾 Injection Supabase...")
        inject_to_supabase(data)
    else:
        print("⚠️  Aucune donnée récupérée")
