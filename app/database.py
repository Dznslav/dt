import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Получаем адрес БД из переменных окружения (или используем localhost для тестов)
DB_HOST = os.getenv("DB_HOST", "localhost")
# Данные для входа совпадают с теми, что мы указали в docker-compose.yml
DATABASE_URL = f"postgresql://admin:secretpassword@{DB_HOST}:5432/node_db"

# Создаем движок SQLAlchemy
engine = create_engine(DATABASE_URL)

# Создаем фабрику сессий для работы с БД
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Базовый класс для всех моделей
Base = declarative_base()

# Зависимость для получения сессии БД в эндпоинтах FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()