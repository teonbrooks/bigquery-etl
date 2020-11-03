import pathlib
import yaml

from jinja2 import Template

from bigquery_etl.format_sql.formatter import reformat


HERE = pathlib.Path(__file__).parent.absolute()
ROOT = pathlib.Path(__file__).parent.parent.parent.absolute()


with (HERE / "config" / "measures.yaml").open("r") as f:
    data = yaml.safe_load(f)

with (HERE / "templates" / "init.sql").open("r") as f:
    template = Template(f.read())
for source in data["sources"]:
    sql = reformat(template.render(**source))
    destdir = (ROOT / "sql" / "moz-fx-data-shared-prod" / "telemetry_derived" /
               f"desktop_usage_{source['name']}_1pct_v1")
    destdir.mkdir(parents=True, exist_ok=True)
    with (destdir / "init.sql").open("w") as f:
        f.write(sql)

with (HERE / "templates" / "view.sql").open("r") as f:
    template = Template(f.read())
for source in data["sources"]:
    sql = reformat(template.render(**source))
    destdir = (ROOT / "sql" / "moz-fx-data-shared-prod" / "telemetry" /
               f"desktop_usage_{source['name']}_1pct")
    destdir.mkdir(parents=True, exist_ok=True)
    with (destdir / "view.sql").open("w") as f:
        f.write(sql)
