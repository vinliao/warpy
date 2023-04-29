from typing import Optional

from alembic.config import Config
from alembic.script import ScriptDirectory


def get_schema_hash() -> Optional[str]:
    config = Config("alembic.ini")
    directory = ScriptDirectory.from_config(config)
    return directory.get_current_head()
