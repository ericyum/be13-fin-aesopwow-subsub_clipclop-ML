from modules.devide.subscription import SubscriptionData
from modules.devide.subscription import get_subscription_data

def get_new_users_char_data(info_db_no: int, origin_table: str) -> SubscriptionData:
    """신규 유저의 구독 모델 퍼센트 비교"""
    return get_subscription_data(info_db_no, origin_table, 'new', False)