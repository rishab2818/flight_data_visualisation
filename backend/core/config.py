import os
class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL","sqlite:///./data_store/metadata.db")
    REDIS_URL: str = os.getenv("REDIS_URL","")
    JWT_SECRET: str = os.getenv("JWT_SECRET","dev_secret_change_me")
    JWT_ALG: str = os.getenv("JWT_ALG","HS256")
    JWT_EXPIRE_MINUTES: int = int(os.getenv("JWT_EXPIRE_MINUTES","4320"))
    DATA_DIR: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data_store"))
settings = Settings()
