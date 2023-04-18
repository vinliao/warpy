from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import or_

from utils.models import EthTransaction, User, user_eth_transactions_association
from sqlalchemy.orm import joinedload

from collections import defaultdict
from sqlalchemy import func, text


def delete_duplicate_associations(session: Session):
    # Create a CTE to identify duplicate associations
    duplicates_cte = session.query(
        user_eth_transactions_association.c.user_fid,
        user_eth_transactions_association.c.eth_transaction_unique_id,
        func.row_number()
        .over(
            partition_by=[
                user_eth_transactions_association.c.user_fid,
                user_eth_transactions_association.c.eth_transaction_unique_id,
            ],
            order_by=user_eth_transactions_association.c.user_fid,
        )
        .label("row_number"),
    ).cte("duplicates_cte")

    # Join the CTE with the association table and delete the duplicate rows
    affected_rows = (
        session.query(user_eth_transactions_association)
        .filter(
            text(
                f"{user_eth_transactions_association.c.user_fid} = {duplicates_cte.c.user_fid} AND "
                f"{user_eth_transactions_association.c.eth_transaction_unique_id} = {duplicates_cte.c.eth_transaction_unique_id} AND "
                "duplicates_cte.row_number > 1"
            )
        )
        .delete(synchronize_session=False)
    )

    print(f"Deleted {affected_rows} duplicate rows")
    session.commit()


def process_tx_batch(session, tx_batch, user_address_cache):
    unique_addresses = set(tx.from_address for tx in tx_batch).union(
        tx.to_address for tx in tx_batch
    )

    # Remove addresses already present in the cache
    uncached_addresses = unique_addresses - set(user_address_cache.keys())

    # Query uncached addresses
    if uncached_addresses:
        new_users = (
            session.query(User).filter(User.address.in_(uncached_addresses)).all()
        )

        # Add new users to the cache
        for user in new_users:
            user_address_cache[user.address] = user.fid

    associations_to_create = []
    for tx in tx_batch:
        from_fid = user_address_cache.get(tx.from_address)
        to_fid = user_address_cache.get(tx.to_address)

        if from_fid:
            associations_to_create.append(
                {"user_fid": from_fid, "eth_transaction_unique_id": tx.unique_id}
            )
        if to_fid and to_fid != from_fid:
            associations_to_create.append(
                {"user_fid": to_fid, "eth_transaction_unique_id": tx.unique_id}
            )

    print(f"Creating {len(associations_to_create)} associations...")

    if associations_to_create:
        for association in associations_to_create:
            session.execute(
                user_eth_transactions_association.insert().values(**association)
            )
        session.commit()


def main(engine: Engine):
    batch_size = 5000  # Decrease batch size to reduce unique_addresses count
    user_address_cache = defaultdict(int)

    with sessionmaker(bind=engine)() as session:
        tx_count = session.query(EthTransaction).count()

        for tx_offset in range(370000, tx_count, batch_size):
            tx_batch = (
                session.query(EthTransaction)
                .order_by(EthTransaction.unique_id)
                .offset(tx_offset)
                .limit(batch_size)
                .all()
            )

            print(f"Current offset: {tx_offset}")

            process_tx_batch(session, tx_batch, user_address_cache)
