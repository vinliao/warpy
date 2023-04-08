import asyncio
import aiohttp
from typing import List
from sqlalchemy.orm import Session
from utils.models import ExternalAddress, User
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine


async def get_single_user_from_ensdata(address: str):
    url = f'https://ensdata.net/{address}'

    print(f"Fetching {address} from {url}")

    async with aiohttp.ClientSession() as session:
        retries = 3
        while retries > 0:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, timeout=timeout) as response:
                    # Sleep between requests to avoid rate limiting
                    await asyncio.sleep(1)

                    response.raise_for_status()
                    json_data = await response.json()
                    if json_data:
                        return json_data
                    else:
                        raise ValueError(
                            f"No results found for address {address}")
            except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                print(f"Error occurred for {address}: {e}. Retrying...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
                retries -= 1
        print(f"Skipping address {address} after 3 retries.")
        return None


async def get_users_from_ensdata(addresses: List[str]):
    tasks = [asyncio.create_task(get_single_user_from_ensdata(address))
             for address in addresses]
    users = await asyncio.gather(*tasks)
    return list(filter(None, users))


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


async def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        users = session.query(User).filter(
            User.external_address != None,
            ~User.external_address.in_(
                session.query(ExternalAddress.address)
            )
        ).all()
        addresses = [user.external_address for user in users]

        batch_size = 50
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i+batch_size]
            await process_addresses(session, batch)
