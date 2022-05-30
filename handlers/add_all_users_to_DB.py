from aiogram import types
from aiogram.dispatcher.filters import Command
from telethon import TelegramClient

from data import config
from loader import dp, db


@dp.message_handler(Command("all_users"))
async def get_all_users():
    api_id = config.API_ID
    api_hash = config.API_HASH
    channels = []
    for ch in await db.select_all_groups():
        channels.append(ch["chat_username"])

    client = TelegramClient('bot', api_id, api_hash)

    await client.start()
    for channel in channels:
        channel_id = await db.get_id_group_username(channel)
        users = await client.get_participants(channel)
        await db.add_all_users(users, channel_id["id"])

    await client.disconnect()
