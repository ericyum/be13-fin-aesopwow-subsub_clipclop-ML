from . import db

class Analysis(db.Model):
    __tablename__ = 'analysis'
    
    analysis_no = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20), nullable=True)

    def to_dict(self):
        return {
            'analysis_no' : self.analysis_no,
            'name' : self.name
        }
