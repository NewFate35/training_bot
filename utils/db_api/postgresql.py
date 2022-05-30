import datetime
from typing import Union

import asyncpg
from asyncpg import Connection
from asyncpg.pool import Pool

from data import config


class Database:

    def __init__(self):
        self.pool: Union[Pool, None] = None

    async def create(self):
        self.pool = await asyncpg.create_pool(
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            database=config.DB_NAME
        )

    async def execute(self, command, *args, fetch: bool = False, fetchval: bool = False, fetchrow: bool = False,
                      execute: bool = False, executemany: bool = False):
        async with self.pool.acquire() as connection:
            connection: Connection
            async with connection.transaction():
                if fetch:
                    result = await connection.fetch(command, *args)
                elif fetchval:
                    result = await connection.fetchval(command, *args)
                elif fetchrow:
                    result = await connection.fetchrow(command, *args)
                elif execute:
                    result = await connection.execute(command, *args)
                elif executemany:
                    result = await connection.executemany(command, *args)
            return result

    async def create_tables(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Groups(
        id SERIAL PRIMARY KEY,
        chat_id BIGINT UNIQUE,
        title VARCHAR(255),
        chat_username VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS Users_2(
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE 
        );
        
        CREATE TABLE IF NOT EXISTS user_group(
        PRIMARY KEY (telegram_id , chat_id ),
        telegram_id BIGINT REFERENCES Users_2(telegram_id) ON DELETE CASCADE,
        chat_id BIGINT REFERENCES Groups ON DELETE CASCADE,
        entry_date timestamp
        );  
        
        CREATE TABLE IF NOT EXISTS users_tracking(
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT,
        action VARCHAR(20),
        group_title VARCHAR(255),
        date timestamp
        );
        """
        await self.execute(sql, execute=True)

    async def add_user(self, telegram_id, chat_id):
        sql1 = "INSERT INTO Users_2 (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING;"
        sql2 = "INSERT INTO user_group(telegram_id, chat_id, entry_date) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
        await self.execute(sql1, telegram_id, fetchrow=True)
        return await self.execute(sql2, telegram_id, chat_id, datetime.datetime.now(), fetchrow=True)

    async def add_group(self, chat_id, title, chat_username):
        sql = "INSERT INTO Groups(chat_id, title, chat_username) VALUES($1, $2, $3) ON CONFLICT DO NOTHING;"
        return await self.execute(sql, chat_id, title, chat_username, fetchrow=True)

    async def get_id_group(self, chat_id):
        sql = "SELECT * FROM Groups WHERE chat_id=$1;"
        result = await self.execute(sql, chat_id, fetchrow=True)
        return result

    async def get_id_group_username(self, chat_username):
        sql = "SELECT * FROM Groups WHERE chat_username=$1;"
        result = await self.execute(sql, chat_username, fetchrow=True)
        return result

    async def delete_member_from_bd(self, user_id, group_id):
        sql = "DELETE FROM user_group WHERE telegram_id=$1 and chat_id=$2"
        return await self.execute(sql, user_id, group_id, fetchrow=True)

    async def add_all_users(self, users, group_id):
        for user in users:
            await self.add_user(user.id, group_id)
        return

    async def select_all_groups(self):
        sql = "SELECT * FROM Groups;"
        result = await self.execute(sql, fetch=True)
        return result

    async def add_action_to_tracker(self, telegram_id, action, group_title):
        sql = "INSERT INTO users_tracking(telegram_id, action, group_title, date) " \
              "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING;"
        return await self.execute(sql, telegram_id, action, group_title, datetime.datetime.now(), fetch=True)
