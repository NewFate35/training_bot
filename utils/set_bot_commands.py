from aiogram import types


async def set_default_commands(dp):
    await dp.bot.set_my_commands(
        [
            types.BotCommand("start", "Старт"),
            # types.BotCommand("help", "Вывести справку"),
            types.BotCommand("check_online", "Проверка активности пользователей"),
            types.BotCommand("all_users", "Занести в бд всех участников из групп, в которых состоит N-пользователь"),
        ]
    )
