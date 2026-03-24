"""Bot entrypoint.

This file loads two feature modules:
- attack_feature.py
- activity_feature.py
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import discord
from discord.ext import commands
from dotenv import load_dotenv

from activity_feature import register_activity_feature
from attack_feature import register_attack_feature


BOT_ROOT = Path(__file__).resolve().parent

GIF_KEY_MAP: dict[str, str] = {
    "money-tft": "./gifs/money-tft.gif",
}
DEFAULT_GIF_KEY = "money-tft"
DEFAULT_BATTLE_TEXT = "toàn quân chuẩn bị, đợi tín hiệu tổng tấn công!"
ATTACKER_PRESETS = [
    "MAI HUONG DAY",
    "NPC MONEY",
    "Ê Đê Tộc",
]
COMMON_LINKS = [
    "https://youtube.com/",
    "https://facebook.com/",
    "https://www.tiktok.com/",
]
ANTI_SPAM_SECONDS = 8.0

ACTIVITY_DB_PATH = BOT_ROOT / "activity.db"
PERIOD_VALUES = {"all", "day", "month"}
METRIC_VALUES = {"total", "chat", "attack"}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("money_tft_bot")


load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing. Add it to .env in the bot folder.")

owner_user_id_raw = os.getenv("DISCORD_OWNER_ID", "").strip()
owner_user_id = int(owner_user_id_raw) if owner_user_id_raw.isdigit() else 0


intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


@bot.event
async def on_ready() -> None:
    try:
        synced = await bot.tree.sync()
        logger.info("Synced %s slash command(s).", len(synced))
    except Exception as exc:  # pragma: no cover
        logger.exception("Slash command sync failed: %s", exc)

    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id if bot.user else "unknown")


register_activity_feature(
    bot=bot,
    logger=logger,
    activity_db_path=ACTIVITY_DB_PATH,
    owner_user_id=owner_user_id,
    period_values=PERIOD_VALUES,
    metric_values=METRIC_VALUES,
)

register_attack_feature(
    bot=bot,
    logger=logger,
    bot_root=BOT_ROOT,
    gif_key_map=GIF_KEY_MAP,
    default_gif_key=DEFAULT_GIF_KEY,
    default_battle_text=DEFAULT_BATTLE_TEXT,
    attacker_presets=ATTACKER_PRESETS,
    common_links=COMMON_LINKS,
    anti_spam_seconds=ANTI_SPAM_SECONDS,
)


if __name__ == "__main__":
    bot.run(TOKEN)
