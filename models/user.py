from datetime import datetime
from . import db

class User(db.Model):
    __tablename__ = 'user'

    user_no = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), nullable=False)
    role_no = db.Column(db.Integer, nullable=False)
    company_no = db.Column(db.Integer, nullable=False)
    logined_at = db.Column(db.DateTime, nullable=False)
    password = db.Column(db.String(60), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = db.Column(db.Integer, default=0)
    department_name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(30), nullable=False)
    name = db.Column(db.String(20), nullable=False)

    def to_dict(self):
        return {
            'user_no': self.user_no,
            'username': self.username,
            'role_no': self.role_no,
            'company_no': self.company_no,
            'logined_at': self.logined_at.isoformat() if self.logined_at else None,
            'password': self.password,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'is_deleted': self.is_deleted,
            'department_name': self.department_name,
            'email': self.email,
            'name': self.name
        }