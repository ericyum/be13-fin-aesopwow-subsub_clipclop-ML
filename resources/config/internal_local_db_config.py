class Config:
    SQLALCHEMY_DATABASE_URI = (
        "mysql+pymysql://beyond:beyond@localhost:3306/aesop_db"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False