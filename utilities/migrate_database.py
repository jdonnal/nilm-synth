import sqlite3
from dataclasses import asdict

import numpy as np
from sqlalchemy.engine import create_engine
from sqlalchemy import select

from nilm_synth.models.library_types import (library_load_table, LibraryLoad,
                                             library_exemplar_table, LibraryExemplar)


def main():
    NEW_DB_PATH = '../database/library.sqlite'
    OLD_DB_PATH = '../database/prep_loads.sqlite'
    orig_db = sqlite3.connect(OLD_DB_PATH)
    engine = create_engine(f'sqlite:///{NEW_DB_PATH}')
    library_db = engine.connect()
    cols = ','.join(['name', 'description', 'stream', 'image',
                     'on_start', 'on_end', 'ss_start', 'ss_end',
                     'off_start', 'off_end'])
    with library_db.connect() as conn:
        old_loads = orig_db.execute(f'SELECT {cols} FROM loads').fetchall()
        for l in old_loads:

            # columns are in the order listed in cols above
            # create a nilm_identify_load record
            load = LibraryLoad(stream=l[2], appliance_type='Appliance',
                               name=l[0], description=l[1], image=l[3])
            # check if this nilm_identify_load is already migrated
            t = library_load_table
            query = select([t]). \
                where(t.c.name == load.name). \
                where(t.c.description == load.description)
            result = conn.execute(query).fetchone()
            if result is not None:
                print(f'{load.name} is already migrated, skipping')
                continue
            query = library_load_table.insert().values(**asdict(load))
            result = conn.execute(query)
            load.id = result.inserted_primary_key[0]
            # create the associated instantiated_load record
            exemplar = LibraryExemplar(on_start=l[4], on_end=l[5], ss_start=l[6],
                                       ss_end=l[7], off_start=l[8], off_end=l[9],
                                       load_id=load.id)
            exemplar.set_delta(np.zeros(8, ))
            query = library_exemplar_table.insert().values(**asdict(exemplar))
            conn.execute(query)
    orig_db.close()


if __name__ == "__main__":
    main()
