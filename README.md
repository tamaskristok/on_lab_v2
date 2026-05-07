# OnLab v2 - multi-broker market data pipeline

Ez a projekt egy BSc onallo labor feladat elokeszito/adatmernoki resze. A cel tobb brokerbol szarmazo penzugyi idosorok egységes letoltese, validalasa es medallion architekturaban torteno tarolasa.

## Cel

A rendszer kulonbozo adatforrasokbol tolt le 1 perces OHLCV adatokat, majd ezeket kozos semara hozza. A pipeline celja, hogy kesobb trendvizsgalathoz, strategiaepiteshez vagy tovabbi elemzeshez megbizhato, dokumentalt es visszakeresheto idosor alljon rendelkezesre.

Tamogatott brokerek/adatforrasok:

- Binance
- Dukascopy
- Saxo Bank
- Interactive Brokers

Tamogatott assetek:

- BTCUSD
- XAUUSD
- XAGUSD
- EURUSD
- US500
- DAX

## Architektura

A projekt medallion strukturat kovet:

```text
raw -> bronze -> silver
```

### Raw

A raw reteg a forrasbol erkezo eredeti vagy ahhoz nagyon kozeli adatot tarolja.

Pelda:

```text
raw/dukascopy/dax/2024/01/DEUIDXEUR-ticks-2024-01.parquet
raw/interactive_brokers/xauusd/2024/01/XAUUSD-bars-2024-01.parquet
```

### Bronze

A bronze reteg broker-specifikus, de mar egységes OHLCV semara hozott adatot tartalmaz.

Fo oszlopok:

```text
timestamp, open, high, low, close, volume
```

Pelda:

```text
bronze/binance/btcusd/2024/01/BTCUSDT-1m-2024-01.parquet
bronze/dukascopy/xauusd/2024/01/XAUUSD-1m-2024-01.parquet
```

### Silver calendar

A calendar reteg mondja meg, hogy egy adott assetnel mely percekre varunk gyertyat.

BTCUSD esetén 24/7 generalt calendar keszul. A tobbi assetnel Interactive Brokers historical schedule alapjan keszul a kereskedesi calendar.

Pelda:

```text
silver/calendar/btcusd/2024/01/BTCUSD-calendar-1m-2024-01.parquet
silver/calendar/dax/2024/01/DAX-calendar-1m-2024-01.parquet
```

### Silver generated OHLCV

A silver generated OHLCV reteg calendar-alapon szurt, tobb brokerbol osszeallitott, elemzesre alkalmas gyertyakat tartalmaz.

Ket generálasi modszer van:

- `method_0`: median alapu OHLC generalas
- `method_1`: automatikus preferred broker alapu OHLC generalas

Pelda:

```text
silver/generated_ohlcv/method_0/xauusd/2024/01/XAUUSD-generated-1m-2024-01.parquet
silver/generated_ohlcv/method_1/xauusd/2024/01/XAUUSD-generated-1m-2024-01.parquet
```

Mindket modszer havi es asset szerinti bontasban kerul feltoltesre Azure Blob Storage-ba. Minden honaphoz tartozik:

```text
parquet fajl
_MANIFEST.json
_SUCCESS
```

## Silver OHLCV oszlopok

```text
timestamp
asset
year
month
open
high
low
close
volume
is_generated
consensus_quality
generation_method_id
generation_method
broker_count
close_diff
close_diff_pct
candle_quality
return_pct
abs_return_pct
is_outlier
quality_status
```

Fontosabb quality mezok:

- `consensus_quality`: hany brokerbol keszult a gyertya (`multi_source`, `single_source`, `missing`)
- `candle_quality`: broker-broker elteres alapjan szamolt minoseg (`good`, `warning`, `bad`, `missing`)
- `is_outlier`: idosoron beluli gyanus arugras jelolese
- `quality_status`: vegso, egyszeruen szurheto minosegi allapot

## Futtatas

A projekt Docker/Jupyter kornyezetben keszult.

Inditas:

```powershell
docker compose up --build
```

Ezutan a Jupyter kernel a Docker kornyezet Pythonjat hasznalja.

## Notebookok

### `run_ingestion.ipynb`

Brokeres adatok letoltese es bronze retegbe irasa.

### `run_calendars.ipynb`

Silver calendar eloallitasa:

- BTCUSD 24/7 calendar
- IB historical schedule alapu calendar a tobbi assethez

### `run_quality.ipynb`

Silver generated OHLCV eloallitasa es feltoltese:

- bronze + calendar betoltes
- broker wide tabla
- `method_0` es `method_1` generalas
- quality flag-ek
- broker ranking
- Azure feltoltes

### `data_quality_report.ipynb`

Riport es prezentacios kimutatasok:

- coverage
- broker/ticker osszesites
- calendar szerinti lefedettseg
- peldagrafikonok

## Konfiguracios fajlok

Fontosabb configok:

```text
config/broker_strategy.csv
config/broker_asset_matrix.csv
config/broker_asset_settings.csv
config/download_period.csv
config/silver_quality_thresholds.csv
```

A `silver_quality_thresholds.csv` tartalmazza assetenkent a quality es outlier hatarokat, igy a minosites kodmodositas nelkul kalibralhato.

## Azure struktura

A projekt Azure Blob Storage-ban tarolja az adatokat, a `market-data` kontenerben.

Pelda struktura:

```text
raw/...
bronze/...
silver/calendar/...
silver/generated_ohlcv/method_0/...
silver/generated_ohlcv/method_1/...
```

## Eredmeny

A projekt vegere letrejott egy ket eves, tobb brokerbol osszeallitott silver adatreteg. A silver adat:

- calendar szerint szurt,
- UTC timestamp alapu,
- havi es asset szerinti bontasu,
- ket generálasi modszerrel elerheto,
- quality mezokkel ellatott,
- Azure-bol visszaolvashato es grafikonon ellenorzott.

Ez a reteg mar alkalmas tovabbi gold szintu elemzeshez, trendvizsgalathoz vagy strategiafejleszteshez.
