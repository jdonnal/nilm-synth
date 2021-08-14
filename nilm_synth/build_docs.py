#!/usr/bin/python3
from jinja2 import Template
from sqlalchemy import create_engine, select

from nilm_synth.models.library_types import metadata, library_load_table


def main():
    engine = create_engine('sqlite:///database/library.sqlite')
    metadata.create_all(engine)
    conn = engine.connect()
    loads = conn.execute(select([library_load_table])).fetchall()
    with open('docs/index.jinja2', 'r') as f:
        template = Template(f.read())
    with open('docs/index.html', 'w') as f:
        f.write(template.render(loads=loads))


if __name__ == "__main__":
    main()
