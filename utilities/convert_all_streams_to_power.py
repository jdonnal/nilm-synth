import sqlite3
import asyncio
import joule.api

from convert_prep_to_power import main as convert


async def main():
    db = sqlite3.connect('../database/library.sqlite')
    results = db.execute('SELECT loads.stream FROM loads').fetchall()
    folders = [r[0] for r in results]
    folders = set(folders)
    node = joule.api.get_node('hollyberry')
    for folder in folders:
        print(folder, end="\n\t")
        # check to make sure stream is single phase
        # convert the stream
        await convert(folder, node, delete=False)

    await node.close()


if __name__ == "__main__":
    asyncio.run(main())
