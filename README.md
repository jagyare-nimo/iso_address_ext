# ICI Data, ISO Address Extraction with DeepParse

A highly configurable, Prefect‑driven Python pipeline that:

1. **Extracts** raw address data from Excel files (`ID`, `ADDRESSLINE1`, `ADDRESSLINE2`, `ADDRESSLINE3`).
2. **Parses** each full address into standardized (ISO) components using the Deepparse library.
3. **Enriches** with audit fields (`filename`, `processed_timestamp`).
4. **Stores** results into an embedded H2 database via JayDeBeApi (upsert semantics).
5. **Archives** both raw and processed files with timestamped naming.

---

## 1. Project Description

This solution automates bulk address normalization for downstream analytics or GIS tasks. It leverages:

* **Prefect** for orchestration and logging
* **pandas** + **openpyxl** for Excel I/O
* **Deepparse** for neural address parsing
* **H2** (embedded) + **JayDeBeApi/JPype** for lightweight JDBC persistence
* **PyYAML** for configuration

By defining all paths, JDBC credentials, and table names in `resources/config.yml`, 
you can point the pipeline at any raw folder and have it deployable on any machine or container.


---

## 2. Installation (Tech Stack & Tools)

1. **Python** 3.10+ (we recommend 3.13)
2. **Java** 11+ (for H2 & JPype)
3. **H2 JAR** (provided at `resources/data/h2-2.3.232.jar`)

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

`requirements.txt` includes:

* `pandas`, `openpyxl`  — Excel handling
* `pyyaml`              — YAML config parsing
* `prefect`             — workflow orchestration
* `jaydebeapi`, `JPype1`— JDBC bridge
* `deepparse>=0.9.13`   — address parsing model
* `sqlalchemy`          — DDL & metadata
* `pytest`, `unittest`  — testing frameworks

Ensure your **JAVA\_HOME** is set and that `h2-2.3.232.jar` is accessible in `resources/data/`.

---

## 3. Running the Pipeline

1. **Prepare input**: drop your raw `.xlsx` files into `resources/ici_sheets/raw`.

2. **Configure** (optional): edit `resources/config.yml` to override directories, JDBC URL, or table name.

3. **Launch**:

   ```bash
   python -m pipeline.flow  # runs the Prefect flow
   ```

   Or with Prefect CLI:

   ```bash
   prefect deployment run "DeepParse Workflow/"  # if you’ve registered a deployment
   ```

4. **Monitor**: logs will appear in your console, showing:

   * Number of files processed
   * A dump of each parsed DataFrame
   * Archival moves

5. **Inspect**:

   * **Processed Excel** in `resources/ici_sheets/output/processed/` (timestamped filenames)
   * **H2 Database** in `resources/data/ici_extract.mv.db` via DBeaver or CLI (see docs above)

---

## 4. Testing

We include both **unittest** and **pytest** suites covering all modules.

### Unittest

```bash
python -m unittest discover -v
```

### pytest

```bash
pytest -q tests
```

Test files are organized by component:

* `tests/test_config.py`
* `tests/test_extractor.py`
* `tests/test_parser.py`
* `tests/test_repository.py`
* `tests/test_archiver.py`
* `tests/test_flow.py`

All tests mock external I/O (JDBC, Prefect contexts, Excel files) to ensure fast, reliable CI runs.

---

## 5. Bulk Test Data Generation

To stress-test or validate pipeline performance with large datasets, you can generate a 1 000 000‑row Excel file of mixed-country addresses:

```bash
python script/generate_raw_data.py
```

This will produce:

```
resources/ici_sheet/raw/raw_data_500k.xlsx
```

You can then run the pipeline against this file to observe throughput and resource usage.
