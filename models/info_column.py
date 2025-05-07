from datetime import datetime
from . import db

class Info_column(db.Model):
    __tablename__ = 'info_column'
    
    info_column_no = db.Column(db.Integer, primary_key=True)
    info_db_no = db.Column(db.Integer, nullable=False)
    analysis_column = db.Column(db.String, nullable=False)
    origin_column = db.Column(db.String, nullable=False)
    origin_table = db.Column(db.String, nullable=True)
    note = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'info_column_no' : self.info_column_no,
            'info_db_no' : self.info_db_no,
            'analysis_column' : self.analysis_column,
            'origin_column' : self.origin_column,
            'origin_table' : self.origin_table,
            'note' : self.note,
            'created_at' : self.created_at.isoformat(),
            'updated_at' : self.updated_at.isoformat()
        }
