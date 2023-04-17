from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from utils.models import EthTransaction, User, user_eth_transactions_association


def check_and_associate(
    session: Session, user_fid: int, user_address: str, tx: EthTransaction
):
    associations_to_create = []

    if tx.to_address == user_address or tx.from_address == user_address:
        existing_association = (
            session.query(user_eth_transactions_association)
            .filter_by(user_fid=user_fid, eth_transaction_unique_id=tx.unique_id)
            .first()
        )

        if not existing_association:
            associations_to_create.append(
                {
                    "user_fid": user_fid,
                    "eth_transaction_unique_id": tx.unique_id,
                }
            )
            print(
                f"Association to be created for user {user_fid} and transaction {tx.unique_id}"
            )
        else:
            print(
                f"Association already exists for user {user_fid} and transaction {tx.unique_id}"
            )
    return associations_to_create


def main(engine: Engine):
    with sessionmaker(bind=engine)() as session:
        users = session.query(User).all()
        txs = session.query(EthTransaction).all()

        for user in users:
            for tx in txs:
                check_and_associate(session, user.fid, user.address, tx)
