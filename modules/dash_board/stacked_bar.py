from models.user import User
from modules.common.user.user_utils import get_canceled_users, get_entire_users

def divide_cancelled_users_by_subscription_model():
    cancelled_users = get_canceled_users()

    basic_users = cancelled_users.filter(User.is_basic_user).all()
    premium_users = cancelled_users.filter(User.is_premium_user).all()
    ultimate_users = cancelled_users.filter(User.is_ultimate_user).all()

    data = [basic_users, premium_users, ultimate_users]

    return data

def divide_entire_users_by_subscription_model():
    entire_users = get_entire_users()

    basic_users = entire_users.filter(User.is_basic_user).all()
    premium_users = entire_users.filter(User.is_premium_user).all()
    ultimate_users = entire_users.filter(User.is_ultimate_user).all()
    data = [basic_users, premium_users, ultimate_users]

    return data