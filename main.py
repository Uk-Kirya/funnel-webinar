import asyncio
from pyrogram import Client
from sqlalchemy import Column, Integer, String, DateTime, select
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from config import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, API_ID, API_HASH

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, default='alive')
    status_updated_at = Column(DateTime(timezone=True), onupdate=func.now())


async def init_db():
    database_url = f'postgresql+asyncpg://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}/{DATABASE_NAME}'
    engine = create_async_engine(database_url, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def check_message(message):
    trigger_words = ["прекрасно", "ожидать"]
    for word in trigger_words:
        if word in message:
            return True
    return False


async def main():
    app = Client("my_account", api_id=API_ID, api_hash=API_HASH)
    await app.start()
    Session = await init_db()

    async with Session() as session:
        while True:
            result = await session.execute(select(User).where(User.status == 'alive'))
            users = result.scalars().all()
            for user in users:
                try:
                    async with app.conversation(user.id) as conv:
                        response = await conv.send_message("Ваше сообщение")
                        if await check_message(response.text):
                            user.status = 'finished'
                            await session.commit()
                            continue
                except Exception as e:
                    user.status = 'dead'
                    await session.commit()
            await asyncio.sleep(60)

    await app.stop()


if __name__ == '__main__':
    asyncio.run(main())
