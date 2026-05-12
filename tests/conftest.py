import os
import sys
from pathlib import Path

# Airflow adds dags/ to sys.path at runtime; replicate that for pytest
sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

# Configure Airflow to use SQLite so tests run without Docker
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/airflow_ci.db"
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
