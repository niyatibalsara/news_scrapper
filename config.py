# import os

# BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# class Config:
#     SECRET_KEY = 'd77666cf039d7d6cc2a1810971986837647adce55082372d7d2ec7f023b86e12'
#     DATABASE = os.path.join(BASE_DIR, 'instance', 'users.db')
#     MYSQL_HOST = "localhost"
#     MYSQL_USER = "root"
#     MYSQL_PASSWORD = "Admin"
#     MYSQL_DB = "news_scrapping"
#     MAIL_SERVER   = 'smtp.gmail.com'
#     MAIL_PORT     = 587
#     MAIL_USE_TLS  = True
#     MAIL_USERNAME = 'balsaraniyati@gmail.com'
#     MAIL_PASSWORD = 'owon nhbx nmku lnub'
#     MAIL_FROM     = 'balsaraniyati17@gmail.com'
#     PDF_FOLDER    = os.path.join(BASE_DIR, 'static', 'pdfs')
#     PDF_WORKERS   = 3
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = 'd77666cf039d7d6cc2a1810971986837647adce55082372d7d2ec7f023b86e12'
    DATABASE = os.path.join(BASE_DIR, 'instance', 'users.db')
    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "Admin"
    MYSQL_DB = "news_scrapping"
    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USERNAME = 'balsaraniyati@gmail.com'
    MAIL_PASSWORD = 'owon nhbx nmku lnub'
    MAIL_FROM = 'balsaraniyati17@gmail.com'
    PDF_FOLDER = os.path.join(BASE_DIR, 'app', 'static', 'pdfs')
    PDF_WORKERS = 3