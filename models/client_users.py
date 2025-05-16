from datetime import datetime

from pydantic import BaseModel

class ClientUser(BaseModel):
    age: int
    country: str
    favorite_genre: str
    gender: str
    last_login: str
    name: str
    subscription_type: str
    test: int
    user_id: int
    watch_time_hours: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self):
        return {
            'age': self.age,
            'country': self.country,
            'favorite_genre': self.favorite_genre,
            'gender': self.gender,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'name': self.name,
            'subscription_type': self.subscription_type,
            'test': self.test,
            'user_id': self.user_id,
            'watch_time_hours': self.watch_time_hours,
        }