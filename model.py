from peewee import *
from conf import CONF

db = MySQLDatabase(CONF.DB_NAME, user=CONF.DB_USER, password=CONF.DB_PWD, host=CONF.DB_Host)


class BaseModel(Model):
    class Meta:
        database = db


class User(BaseModel):
    px_id = IntegerField(unique=True)
    name = CharField()
    account = CharField()
    profile_img = CharField()


class Tag(BaseModel):
    name = CharField()
    translated_name = CharField(null=True)


class Work(BaseModel):
    px_id = IntegerField(unique=True)
    title = CharField()
    create_date = DateTimeField()
    type = CharField()
    caption = CharField()
    user = ForeignKeyField(User, backref="works")
    width = IntegerField()
    height = IntegerField()
    total_view = IntegerField()
    total_bookmarks = IntegerField()
    large_download_url = CharField()
    page_count = IntegerField()


class TagLink(BaseModel):
    work = ForeignKeyField(Work, backref="tags")
    tag = ForeignKeyField(Tag, backref="works")
