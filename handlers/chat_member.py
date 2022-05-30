import datetime

import asyncpg.exceptions
from aiogram import types
from aiogram.dispatcher.filters.builtin import CommandStart

from data import config
from loader import dp, bot, db


@dp.message_handler(content_types=types.ContentType.NEW_CHAT_MEMBERS)
async def new_chat_member(message: types.Message):
    group_id = await db.get_id_group(message.chat.id)
    telegram_id = message.new_chat_members[0].id
    group_title = message.chat.title
    await db.add_group(message.chat.id, message.chat.title, message.chat.username)
    await db.add_user(telegram_id=telegram_id, chat_id=group_id["id"])
    await db.add_action_to_tracker(telegram_id=telegram_id, action="Вступил в группу", group_title=group_title)


@dp.message_handler(content_types=types.ContentType.LEFT_CHAT_MEMBER)
async def left_chat_member(message: types.Message):
    group_id = await db.get_id_group(message.chat.id)
    telegram_id = message.left_chat_member.id
    group_title = message.chat.title
    await db.add_action_to_tracker(telegram_id, "Покинул группу", group_title)
    await db.delete_member_from_bd(group_id=group_id["id"], user_id=telegram_id)
