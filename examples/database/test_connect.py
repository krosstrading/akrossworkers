import asyncio
import logging
from pymongo import MongoClient
from urllib.parse import quote_plus
from akross.common import env


LOGGER = logging.getLogger(__name__)
DB_NAME = 'throwback'
MONGO_URI = f"mongodb://{quote_plus(env.get_mongo_user())}:{quote_plus(env.get_mongo_password())}" \
            "@" + env.get_mongo_stream_url()


async def main():
    print('mongo uri', MONGO_URI)
    db = MongoClient(MONGO_URI)
    database = db[DB_NAME]
    database['hello'].insert_one({'wording': 'hello'})


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
