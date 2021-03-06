from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError, IntegrityError
from scraper_lib import raw_config

Base = declarative_base()

SCHEMA = 'smbc'
# Used when schema cannot be used
table_prefix = ''

if not raw_config.get('database', 'uri').startswith('postgres'):
    table_prefix = SCHEMA + '_'
    SCHEMA = None


class Comic(Base):
    __tablename__ = table_prefix + 'comic'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True, autoincrement=True)
    time_collected = Column(DateTime)
    posted_at = Column(DateTime)
    comic_id = Column(String(256))
    alt = Column(Text)
    ocr = Column(Text)
    title = Column(String(1024))
    file_path = Column(String(512))


class Setting(Base):
    __tablename__ = table_prefix + 'setting'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True, autoincrement=True)
    comic_last_ran = Column(DateTime)
    bit = Column(Integer, unique=True)


engine = create_engine(raw_config.get('database', 'uri'))

if raw_config.get('database', 'uri').startswith('postgres'):
    try:
        engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError:
        # Schema already exists
        pass

Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = scoped_session(sessionmaker(bind=engine))

db_session = DBSession()

try:
    new_setting = Setting()
    new_setting.bit = 0
    db_session.add(new_setting)
    db_session.commit()
except IntegrityError:
    # Settings row has already been created
    db_session.rollback()
