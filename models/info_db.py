from datetime import datetime
from . import db

class User(db.Model):
    __tablename__ = 'info_db'
    
    info_db_no = db.Column(db.Integer, primary_key=True)
    company_no = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(20), nullable=False)
    nickname = db.Column(db.String(20), nullable=False)
    user = db.Column(db.String(20), nullable=False)
    password = db.Column(db.String(20), nullable=False)
    host = db.Column(db.String(20), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'info_db_no' : self.info_db_no,
            'company_no' : self.company_no,
            'name' : self.name,
            'nickname' : self.nickname,
            'user' : self.user,
            'password' : self.password,
            'host' : self.host,
            'port' : self.port,
            'created_at' : self.created_at.isoformat(),
            'updated_at' : self.updated_at.isoformat()
        }
