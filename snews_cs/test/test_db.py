import pytest
from pathlib import Path
import json
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text

from snews_cs.database import Database
from snews_cs.snews_hb import HeartBeat, sanity_checks

# Path to the test database (create a temporary one)
TEST_DB_PATH = Path("./test_snews_cs.db")

# Path to the schema file (adjust if needed)
TEST_SCHEMA_PATH = Path(__file__).parent.parent / "db_schema.sql"


@pytest.fixture(scope="function")  # Use function scope for each test
def test_db():
    """Fixture to create and tear down a test database."""
    db = Database(TEST_DB_PATH)
    db.initialize_database(TEST_SCHEMA_PATH)  # Initialize before the test
    yield db  # Provide the database object to the test
    db.drop_tables()  # this is to get rid of tables in the database


# Mock detector properties file
@pytest.fixture(scope="module")
def mock_detector_file(tmp_path_factory):
    """Fixture to create a mock detector properties JSON file."""
    mock_detectors = {"Baksan": {}, "Borexino": {}}
    p = tmp_path_factory.mktemp("data") / "detector_properties.json"
    with open(p, "w") as f:
        json.dump(mock_detectors, f)
    return p


@pytest.fixture(scope="function")
def hb_instance(test_db, mock_detector_file, monkeypatch):
    """Fixture to create a HeartBeat instance with the test database."""
    monkeypatch.setattr("snews_cs.snews_hb.detector_file", str(mock_detector_file))
    hb = HeartBeat(
        store=True, firedrill_mode=False
    )  # Use firedrill mode False to avoid env var issues
    hb.cache_engine = test_db.engine
    return hb


def test_database_initialization(test_db):
    """Test database initialization and table creation."""
    tables = test_db.show_tables()
    table_names = [table[0] for table in tables]

    expected_tables = [
        "all_mgs",
        "sig_tier_archive",
        "time_tier_archive",
        "coincidence_tier_archive",
        "coincidence_tier_alerts",
        "cached_heartbeats",
        "sqlite_sequence",
    ]

    assert set(table_names) == set(
        expected_tables
    ), "Not all expected tables were created."

    for table_name in expected_tables:
        schema = test_db.get_table_schema(table_name)
        assert len(schema) > 0, f"Schema for table '{table_name}' is empty."
        # Add (e.g., column names, types, etc.?)


def test_show_tables(test_db):
    """Test show_tables method."""
    tables = test_db.show_tables()
    assert isinstance(tables, list)
    assert len(tables) == 7


def test_get_table_schema(test_db):
    """Test get_table_schema method."""
    schema = test_db.get_table_schema("all_mgs")
    assert isinstance(schema, list)
    assert len(schema) > 0

    # Check for specific column
    column_names = [col[1] for col in schema]
    assert "message_id" in column_names
    assert "received_time" in column_names
    assert "id" in column_names


def test_drop_tables_all(test_db):
    """Test dropping all tables."""

    test_db.drop_tables()

    tables = test_db.show_tables()
    assert len(tables) == 1, "Tables were not dropped."


def test_drop_tables_specific(test_db):
    tables_to_drop = ["all_mgs", "sig_tier_archive"]
    test_db.drop_tables(tables_to_drop)

    remaining_tables = test_db.show_tables()
    remaining_table_names = [table[0] for table in remaining_tables]

    expected_remaining_tables = [
        "cached_heartbeats",
        "coincidence_tier_alerts",
        "coincidence_tier_archive",
        "time_tier_archive",
        "sqlite_sequence",
    ]

    assert set(remaining_table_names) == set(
        expected_remaining_tables
    ), "Specific tables were not dropped correctly."


def test_database_connection(test_db):
    """Test if the database connection and cursor are properly initialized."""
    assert test_db.connection is not None
    assert test_db.cursor is not None
    # Add more specific connection tests if required.


def test_database_path(test_db):
    """Test if the database path is correctly set."""
    assert test_db.db_file_path == TEST_DB_PATH


# moving into heartbeat stuff
def test_sanity_checks_valid():
    message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": datetime.utcnow().isoformat(),
        "received_time_utc": np.datetime64(datetime.utcnow().isoformat()),
    }
    assert sanity_checks(message) is True


def test_sanity_checks_invalid():
    # Missing key
    message = {
        "detector_name": "Baksan",
        "sent_time_utc": datetime.utcnow().isoformat(),
        "received_time_utc": np.datetime64(datetime.utcnow().isoformat()),
    }
    assert sanity_checks(message) is False

    # Invalid detector status
    message = {
        "detector_name": "Baksan",
        "detector_status": "INVALID",
        "sent_time_utc": datetime.utcnow().isoformat(),
        "received_time_utc": np.datetime64(datetime.utcnow().isoformat()),
    }
    assert sanity_checks(message) is False
    # ... Add more invalid cases. find more about


def test_heartbeat_make_entry(hb_instance):
    message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": datetime.utcnow().isoformat(),
        "received_time_utc": np.datetime64(datetime.utcnow().isoformat()),
    }
    hb_instance.make_entry(message)
    assert len(hb_instance.cache_df) == 1

    assert hb_instance.cache_df["detector"].iloc[0] == "Baksan"

    # Add another entry to test time_after_last
    message2 = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": (
            datetime.utcnow() + timedelta(seconds=10)
        ).isoformat(),  # 10 seconds later
        "received_time_utc": np.datetime64(
            (datetime.utcnow() + timedelta(seconds=10)).isoformat()
        ),
    }
    hb_instance.make_entry(message2)
    assert len(hb_instance.cache_df) == 2
    assert (
        hb_instance.cache_df["time_after_last"].iloc[1] > 0
    )  # Should be greater than zero.


def test_heartbeat_drop_old_messages(hb_instance):
    # Create some old entries
    now = datetime.utcnow()
    old_message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": (now - timedelta(days=8)).isoformat(),  # 8 days ago
        "received_time_utc": np.datetime64((now - timedelta(days=8)).isoformat()),
    }
    hb_instance.make_entry(old_message)
    new_message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": (now - timedelta(days=2)).isoformat(),  # 2 days ago
        "received_time_utc": np.datetime64((now - timedelta(days=2)).isoformat()),
    }
    hb_instance.make_entry(new_message)
    hb_instance.drop_old_messages()
    assert len(hb_instance.cache_df) == 1  # Only the new message should remain.


def test_heartbeat_update_cache(hb_instance):
    message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": datetime.utcnow().isoformat(),
        "received_time_utc": np.datetime64(datetime.utcnow().isoformat()),
    }
    hb_instance.make_entry(message)
    hb_instance.update_cache()

    # Query the database directly to check if the data is there
    with hb_instance.cache_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM cached_heartbeats"))
        # rows = result.fetchall()
        rows = result.mappings().all()
        assert len(rows) == 1
        assert rows[-1]["detector"] == "Baksan"


def test_electrocardiogram_valid(hb_instance):
    message = {
        "detector_name": "Baksan",
        "detector_status": "ON",
        "sent_time_utc": datetime.utcnow().isoformat(),
    }
    assert hb_instance.electrocardiogram(message) is True


def test_electrocardiogram_invalid(hb_instance):
    message = {
        "detector_name": "Baksan",
        "sent_time_utc": datetime.utcnow().isoformat(),  # Missing status
    }
    assert hb_instance.electrocardiogram(message) is False
