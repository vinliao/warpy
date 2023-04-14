import asyncio
from typing import Any, Dict, List

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# from utils.fetcher import EnsdataFetcher
from utils.models import ENSData, User
from utils.new_fetcher import AsyncFetcher
from utils.utils import save_objects


class EnsdataFetcher(AsyncFetcher):
    addresses: List[str] = []
    json_data: List[Dict[str, Any]] = []

    def __init__(self, addresses: List[str]):
        self.addresses = addresses

    async def _fetch_data(self):
        users = await self._get_users_from_ensdata(self.addresses)
        self.json_data = users

    def _get_models(self) -> List[ENSData]:
        extracted_users = [self._extract_data(user) for user in self.json_data]
        return extracted_users

    async def _get_single_user_from_ensdata(self, address: str) -> Dict[str, Any]:
        url = f"https://ensdata.net/{address}"
        json_data = await self._make_async_request_with_retry(
            url, max_retries=3, delay=5, timeout=10
        )
        return json_data if json_data else None

    async def _get_users_from_ensdata(
        self, addresses: List[str]
    ) -> List[Dict[str, Any]]:
        tasks = [
            asyncio.create_task(self._get_single_user_from_ensdata(address))
            for address in addresses
        ]
        users = await asyncio.gather(*tasks)
        return list(filter(None, users))

    def _extract_data(self, data: Dict[str, Any]) -> ENSData:
        return ENSData(
            address=data.get("address"),
            ens=data.get("ens"),
            url=data.get("url"),
            github=data.get("github"),
            twitter=data.get("twitter"),
            telegram=data.get("telegram"),
            email=data.get("email"),
            discord=data.get("discord"),
        )

    async def fetch(self):
        await self._fetch_data()
        return self._get_models()


def get_users(session: Session) -> List[User]:
    """Get all users with an address that is not in the database."""
    return (
        session.query(User)
        .filter(
            User.address.isnot(None),
            ~User.address.in_(session.query(ENSData.address)),
        )
        .all()
    )


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        addresses = [user.address for user in get_users(session)]

        batch_size = 50
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i : i + batch_size]  # noqa: E203
            fetcher = EnsdataFetcher(batch)
            save_objects(session, await fetcher.fetch())
