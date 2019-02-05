import asyncio


async def test_install(guillotina_rocksdb_requester):  # noqa
    async with guillotina_rocksdb_requester as requester:
        response, _ = await requester('GET', '/db/guillotina/@addons')
        assert 'guillotina_rocksdb' in response['installed']
