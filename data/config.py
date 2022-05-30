from environs import Env

env = Env()
env.read_env()

BOT_TOKEN = env.str('TOKEN')
ADMINS = list(env.list("ADMINS"))  # Список из админов

DB_USER = env.str('DB_USER')
DB_PASSWORD = env.str('DB_PASSWORD')
DB_NAME = env.str('DB_NAME')
DB_HOST = env.str('DB_HOST')

GROUP_CHAT_ID = env.str('GROUP_CHAT_ID')

API_ID = env.str('API_ID')
API_HASH = env.str('API_HASH')
