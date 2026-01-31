from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class DBConfig:
    host: str
    user: str
    password: str
    database: str
    port: int

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

def get_database_config():
    load_dotenv(override=True)

    return DBConfig(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE"),
        port=os.getenv("PORT")
    )

db_config = get_database_config()
print(db_config)