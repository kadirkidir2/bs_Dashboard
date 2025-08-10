from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Veritabanı bağlantı ayarları (app.py'den taşındı)
DB_USER = 'root'
DB_PASSWORD = 'asdasd'
DB_HOST = 'localhost'
DB_NAME = 'sales_dashboard'

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# SQLAlchemy ayarları
engine = create_engine(SQLALCHEMY_DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency (FastAPI tarzı, Flask'ta da kullanılabilir)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()