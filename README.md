# OnLab v2 - multi-broker market data pipeline

Ez a projekt egy BSc önálló laboratóriumi munka adatgyűjtő és adatelőkészítő része. A cél több pénzügyi adatforrásból származó idősoros adat letöltése, egységesítése, validálása és rétegzett tárolása volt.

A rendszer elsősorban 1 perces pénzügyi OHLCV adatok kezelésére készült. Az elkészült adatréteg későbbi trendvizsgálatokhoz, backtestinghez és generatív MI alapú pénzügyi elemzésekhez szolgálhat alapként.

## Fő funkciók

- Több brókerből származó historikus piaci adatok letöltése.
- Raw, bronze és silver adatstruktúra kialakítása.
- Brókerspecifikus adatformátumok egységes OHLCV sémára hozása.
- Azure Blob Storage alapú tárolás.
- Manifest és `_SUCCESS` marker használata az újrafuttathatóság miatt.
- Calendar-alapú silver generálás.
- Quality mezők és outlier jelölések előállítása.
- Streamlit alapú felhasználói felület az indításhoz és ellenőrzéshez.
- Method 0 és method 1 silver idősorok összehasonlítása interaktív grafikonon.

## Támogatott adatforrások

Jelenleg a rendszer az alábbi adatforrásokat kezeli:

- Binance
- Dukascopy
- Saxo Bank
- Interactive Brokers

## Támogatott instrumentumok

A jelenlegi konfigurációban kezelt fő instrumentumok:

- BTCUSD
- XAUUSD
- XAGUSD
- EURUSD
- US500
- DAX

## Adatrétegek

A projekt medallion jellegű rétegzést használ:

```text
raw -> bronze -> silver
```

### Raw

A raw réteg a forráshoz közeli adatokat tárolja. Célja, hogy a későbbi feldolgozási lépések bármikor újragenerálhatók legyenek az eredeti adatokból.

Példa:

```text
raw/dukascopy/dax/2024/01/DEUIDXEUR-ticks-2024-01.parquet
raw/interactive_brokers/xauusd/2024/01/XAUUSD-bars-2024-01.parquet
```

### Bronze

A bronze réteg brókerenként egységes OHLCV sémára hozott adatokat tartalmaz. Az időbélyegek UTC alapúak.

Fő oszlopok:

```text
timestamp, open, high, low, close, volume
```

Példa:

```text
bronze/binance/btcusd/2024/01/BTCUSDT-1m-2024-01.parquet
bronze/dukascopy/xauusd/2024/01/XAUUSD-1m-2024-01.parquet
```

### Silver calendar

A silver calendar azt írja le, hogy egy adott instrumentumnál mely percekre várható gyertya.

- BTCUSD esetén 24/7 generált calendar készül.
- A többi instrumentumnál Interactive Brokers historical schedule alapján készül a calendar.

Példa:

```text
silver/calendar/btcusd/2024/01/BTCUSD-calendar-1m-2024-01.parquet
silver/calendar/dax/2024/01/DAX-calendar-1m-2024-01.parquet
```

### Silver generated OHLCV

A silver generated OHLCV réteg calendar alapján illesztett, több brókerből előállított, minőségjelzőkkel ellátott idősorokat tartalmaz.

Két generálási módszer készült:

- `method_0`: medián alapú OHLC generálás.
- `method_1`: preferred broker alapú OHLC generálás.

Példa:

```text
silver/generated_ohlcv/method_0/xauusd/2024/01/XAUUSD-generated-1m-2024-01.parquet
silver/generated_ohlcv/method_1/xauusd/2024/01/XAUUSD-generated-1m-2024-01.parquet
```

## Silver OHLCV fontosabb mezők

- `timestamp`: UTC időbélyeg.
- `asset`: instrumentum neve.
- `open`, `high`, `low`, `close`: generált OHLC értékek.
- `volume`: volumen, ha értelmezhető.
- `generation_method_id`: silver generálási módszer azonosítója.
- `generation_method`: silver generálási módszer neve.
- `selected_broker`: method 1 esetén a kiválasztott bróker.
- `broker_count`: az adott időpontban elérhető brókerek száma.
- `consensus_quality`: több forrásból, egy forrásból vagy hiányzó adatból készült-e a gyertya.
- `candle_quality`: broker-broker eltérés alapján számolt minőségi kategória.
- `is_outlier`: kiugró ármozgás jelölése.
- `quality_status`: összesített minőségi állapot.

## Konfiguráció

A rendszer konfigurációvezérelt. A fő konfigurációs belépési pont:

```text
conf/config.yaml
```

Fontosabb CSV konfigurációk:

```text
config/broker_strategy.csv
config/broker_asset_matrix.csv
config/broker_asset_settings.csv
config/download_period.csv
config/saxo_bank_instruments.csv
config/interactive_brokers_instruments.csv
config/interactive_brokers_calendar_instruments.csv
config/silver_quality_thresholds.csv
```

A Hydra konfiguráció parancssorból is felülírható, például:

```powershell
docker compose exec on-lab-v2 python -m scripts.show_config run.start_month=2024-01 run.end_month=2024-02 filters.assets=[BTCUSD]
```

## Futtatás Dockerrel

Másik gépen a projekt indításának alaplépései:

1. Repository klónozása.
2. `.env.example` alapján `.env` fájl létrehozása.
3. Azure connection string megadása a `.env` fájlban.
4. Docker indítása.

```powershell
docker compose up --build
```

A Jupyter Lab a következő porton érhető el:

```text
http://localhost:8888
```

A Streamlit UI a következő porton érhető el:

```text
http://localhost:8501
```

## Fő notebookok

- `run_ingestion.ipynb`: brókeradatok letöltése és raw/bronze réteg előállítása.
- `run_calendars.ipynb`: silver calendar generálása.
- `run_quality.ipynb`: silver generated OHLCV előállítása és feltöltése.
- `data_quality_report.ipynb`: coverage, broker/ticker és minőségi riportok.

## Fő scriptek

- `scripts/show_config.py`: Hydra konfiguráció megjelenítése.
- `scripts/validate_config.py`: konfigurációs fájlok ellenőrzése.
- `scripts/run_ingestion.py`: ingestion pipeline indítása konfigurációból.
- `scripts/run_silver_quality.py`: silver quality pipeline indítása konfigurációból.

Példa:

```powershell
docker compose exec on-lab-v2 python -m scripts.validate_config
```

## Streamlit UI

A Streamlit alkalmazás több oldalból áll:

- Főoldal
- Adat letöltés
- Silver feltöltés
- Elemzés

Az UI célja, hogy a hosszabb futású folyamatok előtt ellenőrizhető legyen, mi fog lefutni, mely adategységek vannak már készen, és milyen eredmények születtek.

## Azure struktúra

Az adatok Azure Blob Storage-ban, a `market-data` konténerben tárolódnak.

Példa struktúra:

```text
raw/...
bronze/...
silver/calendar/...
silver/generated_ohlcv/method_0/...
silver/generated_ohlcv/method_1/...
```

Minden fontos feldolgozási egységhez tartozhat:

```text
parquet fájl
_MANIFEST.json
_SUCCESS
```

## Lokális és titkos fájlok

A következő fájlok és mappák nem kerülnek Gitbe:

- `.env`
- `data/`
- `outputs/`
- `multirun/`
- parquet, zip és ideiglenes adatfájlok

Saxo Bank futtatásakor az access token nem kerül `.env` fájlba, azt futtatáskor kell megadni. Interactive Brokers használatához a TWS-nek futnia kell a gépen.

## Jelenlegi eredmény

A projekt eredményeként létrejött egy két évre épített, több brókerből származó pénzügyi idősoros adatréteg. Az adatok raw, bronze és silver szinten tárolódnak, UTC alapú időbélyegekkel, havi bontásban, Azure Blob Storage-ban.

A silver réteg már alkalmas további gold szintű elemzések, trendvizsgálatok, backtesting vagy generatív MI alapú pénzügyi elemzések előkészítésére.
