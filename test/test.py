from wos_builder.conversion import xml_to_sql

from pathlib import Path

current_dir = Path(__file__).parent.resolve()

TEST_XML = current_dir / ".." / "resources" / "sample.xml"
OUT_DIR = "out"
Path(OUT_DIR).mkdir(exist_ok=True)


def test_xml_to_sql():
    xml_to_sql(TEST_XML, OUT_DIR)


if __name__ == "__main__":
    test_xml_to_sql()
