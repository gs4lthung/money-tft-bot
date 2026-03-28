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

sync_guild_id_raw = os.getenv("DISCORD_GUILD_ID", "").strip()
sync_guild_id = int(sync_guild_id_raw) if sync_guild_id_raw.isdigit() else None

bot_prefix = os.getenv("BOT_PREFIX", "m!").strip() or "m!"


intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=commands.when_mentioned_or(bot_prefix), intents=intents, help_command=None)


async def sync_slash_commands() -> tuple[int, str]:
    if sync_guild_id is not None:
        # Clear guild-scoped commands first, then re-copy current globals.
        # This prevents stale command entries from older bot versions.
        target_guild = discord.Object(id=sync_guild_id)
        bot.tree.clear_commands(guild=target_guild)
        bot.tree.copy_global_to(guild=target_guild)
        synced = await bot.tree.sync(guild=target_guild)
        return len(synced), f"guild {sync_guild_id}"

    synced = await bot.tree.sync()
    return len(synced), "global"


@bot.event
async def on_ready() -> None:
    try:
        synced_count, synced_scope = await sync_slash_commands()
        if synced_scope.startswith("guild "):
            logger.info(
                "Synced %s slash command(s) to guild %s (stale guild commands cleared).",
                synced_count,
                sync_guild_id,
            )
        else:
            logger.info(
                "Synced %s global slash command(s). Set DISCORD_GUILD_ID for stricter stale-command cleanup.",
                synced_count,
            )
    except Exception as exc:  # pragma: no cover
        logger.exception("Slash command sync failed: %s", exc)

    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id if bot.user else "unknown")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, commands.CommandNotFound):
        # Ignore unknown prefix commands to avoid noisy logs from other bots.
        return

    logger.exception("Unhandled prefix command error: %s", error)


@bot.command(name="help")
async def help_prefix(ctx: commands.Context) -> None:
    await ctx.reply(
        f"Prefix: {bot_prefix}\n"
        f"Available commands: {bot_prefix}attack, {bot_prefix}activity_export, {bot_prefix}resync, {bot_prefix}help\n"
        "Slash commands: /attack, /activity_export"
    )


@bot.command(name="resync")
async def resync_prefix(ctx: commands.Context) -> None:
    if owner_user_id:
        is_allowed = ctx.author.id == owner_user_id
    else:
        is_allowed = await bot.is_owner(ctx.author)

    if not is_allowed:
        await ctx.reply("[x] You are not allowed to run this command.")
        return

    try:
        synced_count, synced_scope = await sync_slash_commands()
        if synced_scope.startswith("guild "):
            await ctx.reply(
                f"[ok] Resynced {synced_count} slash command(s) for {synced_scope}. Stale guild commands were cleared."
            )
        else:
            await ctx.reply(f"[ok] Resynced {synced_count} global slash command(s).")
    except Exception as exc:
        logger.exception("Manual slash command resync failed: %s", exc)
        await ctx.reply("[x] Resync failed. Check bot logs for details.")


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
