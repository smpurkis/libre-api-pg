from libre_api_pg.download_readings import run


def main() -> int:
    import asyncio

    asyncio.run(run())
