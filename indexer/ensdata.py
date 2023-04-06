import asyncio
import aiohttp
from typing import List
from sqlalchemy.orm import Session
from models import ExternalAddress, User
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os


async def get_single_user_from_ensdata(address: str):
    url = f'https://ensdata.net/{address}'

    print(f"Fetching {address} from {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            json_data = await response.json()
            return json_data


async def get_users_from_ensdata(addresses: List[str]):
    tasks = [asyncio.create_task(get_single_user_from_ensdata(address))
             for address in addresses]
    users = await asyncio.gather(*tasks)
    return users


def extract_ensdata_user_data(data):
    return ExternalAddress(
        address=data.get('address'),
        ens=data.get('ens'),
        url=data.get('url'),
        github=data.get('github'),
        twitter=data.get('twitter'),
        telegram=data.get('telegram'),
        email=data.get('email'),
        discord=data.get('discord')
    )


def save_users_to_db(session: Session, users: List[dict]):
    for user in users:
        user_data = extract_ensdata_user_data(user)
        existing_user = session.query(ExternalAddress).filter_by(
            address=user_data.address).first()
        if existing_user:
            session.delete(existing_user)
        session.add(user_data)
    session.commit()


async def process_addresses(session, addresses: List[str]):
    users = await get_users_from_ensdata(addresses)
    save_users_to_db(session, users)


async def main():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'datasets', 'datasets.db')
    engine = create_engine('sqlite:///' + db_path)

    with sessionmaker(bind=engine)() as session:
        # get two random user where external address is not none
        users = session.query(User).filter(
            User.external_address != None).limit(10).all()
        addresses = [user.external_address for user in users]

        batch_size = 2
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i+batch_size]
            await process_addresses(session, batch)


if __name__ == "__main__":
    asyncio.run(main())
