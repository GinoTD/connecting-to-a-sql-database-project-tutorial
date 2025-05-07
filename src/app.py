import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import datetime

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLAlchemy
def connect():
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


# 2) Create the tables
Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publishers"
    publisher_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

class Author(Base):
    __tablename__ = "authors"
    author_id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    middle_name = Column(String(50), nullable=True)
    last_name = Column(String(100), nullable=True)

class Book(Base):
    __tablename__ = "books"
    book_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    total_pages = Column(Integer, nullable=True)
    rating = Column(DECIMAL(4, 2), nullable=True)
    isbn = Column(String(13), nullable=True)
    published_date = Column(Date, nullable=True)
    publisher_id = Column(Integer, ForeignKey('publishers.publisher_id'), nullable=True)

class BookAuthor(Base):
    __tablename__ = "book_authors"
    book_id = Column(Integer, ForeignKey('books.book_id', ondelete='CASCADE'), primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.author_id', ondelete='CASCADE'), primary_key=True)

# Create tables if they don't exist already
engine = connect()
Base.metadata.create_all(engine)

# 3) Insert data
Session = sessionmaker(bind=engine)
session = Session()

publishers = [
    Publisher(publisher_id=1, name="O Reilly Media"),
    Publisher(publisher_id=2, name="A Book Apart"),
    Publisher(publisher_id=3, name="A K PETERS"),
    Publisher(publisher_id=4, name="Academic Press"),
    Publisher(publisher_id=5, name="Addison Wesley"),
    Publisher(publisher_id=6, name="Albert&Sweigart"),
    Publisher(publisher_id=7, name="Albert A. Knopf")
]

# Use merge to insert or update
for publisher in publishers:
    session.merge(publisher)
session.commit()

authors = [
    Author(author_id=1, first_name='Merritt', middle_name=None, last_name='Eric'),
    Author(author_id=2, first_name='Linda', middle_name=None, last_name='Mui'),
    Author(author_id=3, first_name='Alecos', middle_name=None, last_name='Papadatos'),
    Author(author_id=4, first_name='Anthony', middle_name=None, last_name='Molinaro'),
    Author(author_id=5, first_name='David', middle_name=None, last_name='Cronin'),
    Author(author_id=6, first_name='Richard', middle_name=None, last_name='Blum'),
    Author(author_id=7, first_name='Yuval', middle_name='Noah', last_name='Harari'),
    Author(author_id=8, first_name='Paul', middle_name=None, last_name='Albitz')
]

for author in authors:
    session.merge(author)
session.commit()

# Use datetime.date() for published_date
books = [
    Book(book_id=1, title='Lean Software Development: An Agile Toolkit', total_pages=240, rating=4.17, isbn='9780320000000', published_date=datetime.date(2003, 5, 18), publisher_id=5),
    Book(book_id=2, title='Facing the Intelligence Explosion', total_pages=91, rating=3.87, isbn=None, published_date=datetime.date(2013, 2, 1), publisher_id=7),
    Book(book_id=3, title='Scala in Action', total_pages=419, rating=3.74, isbn='9781940000000', published_date=datetime.date(2013, 4, 10), publisher_id=1),
    Book(book_id=4, title='Patterns of Software: Tales from the Software Community', total_pages=256, rating=3.84, isbn='9780200000000', published_date=datetime.date(1996, 8, 15), publisher_id=1),
    Book(book_id=5, title='Anatomy Of LISP', total_pages=446, rating=4.43, isbn='9780070000000', published_date=datetime.date(1978, 1, 1), publisher_id=3),
    Book(book_id=6, title='Computing machinery and intelligence', total_pages=24, rating=4.17, isbn=None, published_date=datetime.date(2009, 3, 22), publisher_id=4),
    Book(book_id=7, title='XML: Visual QuickStart Guide', total_pages=269, rating=3.66, isbn='9780320000000', published_date=datetime.date(2009, 1, 1), publisher_id=5),
    Book(book_id=8, title='SQL Cookbook', total_pages=595, rating=3.95, isbn='9780600000000', published_date=datetime.date(2005, 12, 1), publisher_id=7),
    Book(book_id=9, title='The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', total_pages=439, rating=4.29, isbn='9781440000000', published_date=datetime.date(2010, 7, 1), publisher_id=6),
    Book(book_id=10, title='Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', total_pages=222, rating=3.54, isbn='9780750000000', published_date=datetime.date(2007, 2, 13), publisher_id=7)
]

for book in books:
    session.merge(book)
session.commit()

book_authors = [
    BookAuthor(book_id=1, author_id=1),
    BookAuthor(book_id=2, author_id=8),
    BookAuthor(book_id=3, author_id=7),
    BookAuthor(book_id=4, author_id=6),
    BookAuthor(book_id=5, author_id=5),
    BookAuthor(book_id=6, author_id=4),
    BookAuthor(book_id=7, author_id=3),
    BookAuthor(book_id=8, author_id=2),
    BookAuthor(book_id=9, author_id=4),
    BookAuthor(book_id=10, author_id=1)
]

for book_author in book_authors:
    session.merge(book_author)
session.commit()

# 4) Use Pandas to read and display a table
df_publishers = pd.read_sql("SELECT * FROM publishers", engine)

print(df_publishers)
