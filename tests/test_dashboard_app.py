"""Streamlit smoke test for all RetailFlow 2.0 dashboard pages."""

from pathlib import Path

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parent.parent
APP_PATH = PROJECT_ROOT / "dashboard" / "retailflow_v2_app.py"


def test_every_dashboard_page_renders_without_script_exceptions():
    app = AppTest.from_file(str(APP_PATH), default_timeout=30).run()
    assert not app.exception, (
        "Platform Overview raised an exception: "
        + "; ".join(item.message for item in app.exception)
    )

    pages = [
        "Product Gallery",
        "Olist Analytics",
        "Data Quality & Lineage",
        "Platform Overview",
    ]

    for page in pages:
        app.sidebar.radio[0].set_value(page)
        app.run()
        assert not app.exception, (
            f"{page} raised an exception: "
            + "; ".join(item.message for item in app.exception)
        )
