import datetime
import logging

from aiogram import types
from aiogram.dispatcher.filters.builtin import CommandStart, Command
from telethon import TelegramClient

from data import config
from loader import dp, db, bot


@dp.message_handler(CommandStart())
async def bot_start(message: types.Message):
    await message.answer("hello from training bot")


@dp.message_handler(Command("check_online"))
async def check_online(message: types.Message):
    api_id = config.API_ID
    api_hash = config.API_HASH
    channels = []
    for ch in await db.select_all_groups():
        channels.append(ch["chat_username"])

    client = TelegramClient('bot', api_id, api_hash)

    await client.start()
    for channel in channels:
        channel_info = await db.get_id_group_username(channel)
        users = await client.get_participants(channel)
        for user in users:
            if user.status is not None:
                online = user.status.was_online
                current_time = datetime.datetime.now().astimezone()

                time_interval = current_time - online

                chat_id = channel_info["chat_id"]
                channel_title = channel_info["title"]
                member = await bot.get_chat_member(chat_id, user.id)

                if time_interval.days > 30:
                    if member["status"] == "member":
                        await bot.ban_chat_member(chat_id=chat_id, user_id=user.id)
                        await bot.unban_chat_member(chat_id=chat_id, user_id=user.id)
                        await db.delete_member_from_bd(group_id=chat_id, user_id=user.id)
                        logging.info(f"Удаление заброшенного аккаунта. id: {user.id}, группа: {channel_title}")

    await client.disconnect()
