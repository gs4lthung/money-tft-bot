"""
Discord.py local GIF attack bot (single file).

Setup:
1) Install dependencies:
   pip install -U discord.py python-dotenv

2) Create a .env file next to this script:
   DISCORD_TOKEN=your_bot_token_here

3) Put your GIF files in ./gifs (relative to this script), for example:
   ./gifs/money-tft.gif

4) Run:
   python bot.py

Notes:
- Prefix command examples:
  !attack "MAI HUONG DAY"
  !attack 16:20 "MAI HUONG DAY" https://youtube.com/watch?v=abc123
- Slash command: /attack
- The bot always uses local GIF file configured by DEFAULT_GIF_KEY.
"""

from __future__ import annotations

import io
import logging
import os
import re
import shlex
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import discord
from openpyxl import Workbook
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv


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

TIME_REGEX = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("money_tft_bot")


class AttackInputError(ValueError):
    """Raised when user input for the attack command is invalid."""


def _normalize_relative_path(path_text: str) -> Path:
    path_text = path_text.strip().strip('"').strip("'")
    user_path = Path(path_text)

    if user_path.is_absolute():
        raise AttackInputError("Please use a relative path (for example: ./gifs/mygif.gif), not an absolute path.")

    resolved = (BOT_ROOT / user_path).resolve(strict=False)
    try:
        resolved.relative_to(BOT_ROOT)
    except ValueError as exc:
        raise AttackInputError("Path must stay inside the bot folder.") from exc

    return resolved


def resolve_gif_path(gif_reference: str) -> Path:
    ref = gif_reference.strip().strip('"').strip("'")
    if not ref:
        raise AttackInputError("GIF key/path is required.")

    key = ref.lower()
    if key in GIF_KEY_MAP:
        candidate = _normalize_relative_path(GIF_KEY_MAP[key])
    else:
        has_path_hint = "/" in ref or "\\" in ref or ref.startswith(".")
        if has_path_hint or ref.lower().endswith(".gif"):
            candidate = _normalize_relative_path(ref)
        else:
            candidate = _normalize_relative_path(str(Path("gifs") / f"{ref}.gif"))

    if candidate.suffix.lower() != ".gif":
        raise AttackInputError("Only .gif files are allowed.")

    if not candidate.exists() or not candidate.is_file():
        raise AttackInputError(
            "GIF file not found. Check your key/path and ensure the file exists in your local bot folder."
        )

    return candidate


def validate_attack_inputs(attack_time: str, attacker_name: str) -> None:
    if not TIME_REGEX.match(attack_time):
        raise AttackInputError("Invalid time format. Use HH:MM in 24-hour format (example: 16:20).")
    if not attacker_name.strip():
        raise AttackInputError("Attacker name cannot be empty.")


def get_attack_time(attack_time: Optional[str]) -> str:
    if attack_time is None or not attack_time.strip():
        return datetime.now().strftime("%H:%M")

    value = attack_time.strip()
    if not TIME_REGEX.match(value):
        raise AttackInputError("Invalid time format. Use HH:MM in 24-hour format (example: 16:20).")

    return value


def validate_attack_link(attack_link: Optional[str]) -> Optional[str]:
    if attack_link is None:
        return None

    candidate = attack_link.strip()
    if not candidate:
        return None

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise AttackInputError("Invalid link. Use a full URL starting with http:// or https://")

    return candidate


def _looks_like_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def parse_prefix_attack_input(raw_input: str) -> tuple[str, str, Optional[str], bool, str]:
    tokens = shlex.split(raw_input)
    if not tokens:
        raise AttackInputError(
            "Missing arguments. Usage: !attack [HH:MM] \"<name>\" [link] [--tag-all] [--text \"noi dung\"]"
        )

    if TIME_REGEX.match(tokens[0]):
        attack_time = get_attack_time(tokens[0])
        tokens = tokens[1:]
    else:
        attack_time = get_attack_time(None)

    if not tokens:
        raise AttackInputError(
            "Missing attacker name. Usage: !attack [HH:MM] \"<name>\" [link] [--tag-all] [--text \"noi dung\"]"
        )

    tag_everyone = False
    attack_link: Optional[str] = None
    battle_text = DEFAULT_BATTLE_TEXT
    attacker_name_parts: list[str] = []

    i = 0
    while i < len(tokens):
        token = tokens[i]

        if token in {"--tag-all", "--everyone"}:
            tag_everyone = True
            i += 1
            continue

        if token == "--no-everyone":
            tag_everyone = False
            i += 1
            continue

        if token in {"--text", "-t"}:
            if i + 1 >= len(tokens):
                raise AttackInputError("Missing value after --text. Example: --text \"toàn quân chuẩn bị!\"")
            battle_text = tokens[i + 1].strip()
            i += 2
            continue

        if attack_link is None and _looks_like_url(token):
            attack_link = token
            i += 1
            continue

        attacker_name_parts.append(token)
        i += 1

    attacker_name = " ".join(attacker_name_parts).strip()
    if not attacker_name:
        raise AttackInputError("Attacker name cannot be empty.")

    if not battle_text:
        raise AttackInputError("Battle text cannot be empty when using --text.")

    validated_link = validate_attack_link(attack_link)
    return attack_time, attacker_name, validated_link, tag_everyone, battle_text


def build_attack_embed(
    author: discord.abc.User,
    attack_time: str,
    attacker_name: str,
    gif_file_path: Path,
    battle_text: str,
    attack_link: Optional[str] = None,
) -> tuple[discord.Embed, discord.File]:
    description = f"{attack_time} - {attacker_name} {battle_text}"

    embed = discord.Embed(
        title="NPC MONEY TỔNG TIẾN CÔNG!",
        description=description,
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_thumbnail(url=author.display_avatar.url)

    attached_name = gif_file_path.name
    local_file = discord.File(str(gif_file_path), filename=attached_name)
    embed.set_image(url=f"attachment://{attached_name}")

    if attack_link:
        embed.add_field(name="Link", value=attack_link, inline=False)

    embed.set_footer(text="Quỳ xuống dưới chân Ê Đê Tộc !!!")
    return embed, local_file


load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing. Add it to .env in the bot folder.")

OWNER_USER_ID_RAW = os.getenv("DISCORD_OWNER_ID", "").strip()
OWNER_USER_ID = int(OWNER_USER_ID_RAW) if OWNER_USER_ID_RAW.isdigit() else 0

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
_tree_synced = False
_slash_last_used: dict[int, float] = {}


def init_activity_db() -> None:
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_activity (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                chat_count INTEGER NOT NULL DEFAULT 0,
                attack_count INTEGER NOT NULL DEFAULT 0,
                last_active TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        conn.commit()


def is_owner_user(user_id: int) -> bool:
    return OWNER_USER_ID != 0 and user_id == OWNER_USER_ID


def record_activity(
    guild_id: int,
    user_id: int,
    username: str,
    chat_increment: int = 0,
    attack_increment: int = 0,
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO user_activity (guild_id, user_id, username, chat_count, attack_count, last_active)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
                username = excluded.username,
                chat_count = user_activity.chat_count + excluded.chat_count,
                attack_count = user_activity.attack_count + excluded.attack_count,
                last_active = excluded.last_active
            """,
            (guild_id, user_id, username, chat_increment, attack_increment, now_iso),
        )
        conn.commit()


def get_top_activity_rows(guild_id: int, limit: int) -> list[tuple[str, int, int, int, str]]:
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT
                username,
                chat_count,
                attack_count,
                (chat_count + attack_count) AS total_count,
                COALESCE(last_active, '')
            FROM user_activity
            WHERE guild_id = ?
            ORDER BY total_count DESC, chat_count DESC, attack_count DESC
            LIMIT ?
            """,
            (guild_id, limit),
        ).fetchall()
    return [(str(r[0]), int(r[1]), int(r[2]), int(r[3]), str(r[4])) for r in rows]


def build_activity_excel(guild_id: int) -> io.BytesIO:
    rows = get_top_activity_rows(guild_id, 1000)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Activity"
    sheet.append(["Username", "Chat Count", "Attack Count", "Total", "Last Active (UTC)"])
    for row in rows:
        sheet.append(list(row))

    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


def slash_is_on_cooldown(user_id: int) -> float:
    now = time.monotonic()
    last_used = _slash_last_used.get(user_id)
    if last_used is None:
        _slash_last_used[user_id] = now
        return 0.0

    elapsed = now - last_used
    if elapsed >= ANTI_SPAM_SECONDS:
        _slash_last_used[user_id] = now
        return 0.0

    return ANTI_SPAM_SECONDS - elapsed


@bot.event
async def on_ready() -> None:
    global _tree_synced

    init_activity_db()

    if not _tree_synced:
        try:
            synced = await bot.tree.sync()
            logger.info("Synced %s slash command(s).", len(synced))
            _tree_synced = True
        except Exception as exc:  # pragma: no cover
            logger.exception("Slash command sync failed: %s", exc)

    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id if bot.user else "unknown")


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot:
        return

    if message.guild is not None:
        try:
            record_activity(
                guild_id=message.guild.id,
                user_id=message.author.id,
                username=str(message.author),
                chat_increment=1,
            )
        except Exception:
            logger.exception("Failed to record chat activity for user=%s", message.author.id)

    await bot.process_commands(message)


@bot.command(name="attack")
@commands.cooldown(1, ANTI_SPAM_SECONDS, commands.BucketType.user)
async def attack_prefix(
    ctx: commands.Context,
    *,
    raw_input: Optional[str] = None,
) -> None:
    """
    Prefix usage:
    !attack [HH:MM] "<name>" [link] [--tag-all] [--text "noi dung"]
    """
    logger.info(
        "Prefix attack requested by user=%s(%s) guild=%s channel=%s input=%r",
        ctx.author,
        ctx.author.id,
        ctx.guild.id if ctx.guild else "dm",
        ctx.channel.id,
        raw_input,
    )

    try:
        if raw_input is None or not raw_input.strip():
            raise AttackInputError(
                "Missing arguments. Usage: !attack [HH:MM] \"<name>\" [link] [--tag-all] [--text \"noi dung\"]"
            )

        attack_time, attacker_name, validated_link, tag_everyone, battle_text = parse_prefix_attack_input(raw_input)
        validate_attack_inputs(attack_time, attacker_name)
        gif_path = resolve_gif_path(DEFAULT_GIF_KEY)
        embed, local_file = build_attack_embed(
            ctx.author,
            attack_time,
            attacker_name,
            gif_path,
            battle_text,
            validated_link,
        )
    except AttackInputError as exc:
        await ctx.reply(f"[x] {exc}")
        return
    except Exception:
        logger.exception("Unexpected prefix attack error. input=%r", raw_input)
        await ctx.reply("[x] Internal error. Please try again in a few seconds.")
        return

    await ctx.send(
        content="@everyone" if tag_everyone else None,
        embed=embed,
        file=local_file,
        allowed_mentions=discord.AllowedMentions(everyone=tag_everyone),
    )
    if ctx.guild is not None:
        record_activity(
            guild_id=ctx.guild.id,
            user_id=ctx.author.id,
            username=str(ctx.author),
            attack_increment=1,
        )
    logger.info("Prefix attack sent by user=%s(%s)", ctx.author, ctx.author.id)


@attack_prefix.error
async def attack_prefix_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.reply(f"[x] Anti-spam active. Wait {error.retry_after:.1f}s and try again.")
        return

    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("[x] Missing arguments. Usage: !attack [HH:MM] \"<name>\" [link] [--tag-all] [--text \"noi dung\"]")
        return

    if isinstance(error, commands.BadArgument):
        await ctx.reply("[x] Invalid argument. Check optional time, name, and optional link.")
        return

    logger.exception("Unhandled prefix command error: %s", error)
    await ctx.reply("[x] Something went wrong while executing the attack command.")


@bot.tree.command(name="attack", description="Execute an attack with a local GIF")
@app_commands.describe(
    attacker_name="Displayed attacker name",
    attack_time="Optional time HH:MM (24-hour). Leave empty to use current time",
    attack_link="Optional URL (YouTube/Facebook/etc.)",
    tag_everyone="Tag @everyone in this attack message",
    battle_text="Optional custom battle text. Default: toàn quân chuẩn bị, đợi tín hiệu tổng tấn công!",
)
async def attack_slash(
    interaction: discord.Interaction,
    attacker_name: str,
    attack_time: Optional[str] = None,
    attack_link: Optional[str] = None,
    tag_everyone: bool = False,
    battle_text: Optional[str] = None,
) -> None:
    logger.info(
        "Slash attack requested by user=%s(%s) guild=%s channel=%s attacker_name=%r time=%r link=%r tag_everyone=%s",
        interaction.user,
        interaction.user.id,
        interaction.guild_id if interaction.guild_id else "dm",
        interaction.channel_id,
        attacker_name,
        attack_time,
        attack_link,
        tag_everyone,
    )

    try:
        wait_seconds = slash_is_on_cooldown(interaction.user.id)
        if wait_seconds > 0:
            await interaction.response.send_message(
                f"[x] Anti-spam active. Wait {wait_seconds:.1f}s and try again.",
                ephemeral=True,
            )
            return

        resolved_time = get_attack_time(attack_time)
        validate_attack_inputs(resolved_time, attacker_name)
        validated_link = validate_attack_link(attack_link)
        resolved_battle_text = (battle_text or DEFAULT_BATTLE_TEXT).strip()
        if not resolved_battle_text:
            await interaction.response.send_message("[x] Battle text cannot be empty.", ephemeral=True)
            return

        gif_path = resolve_gif_path(DEFAULT_GIF_KEY)
        embed, local_file = build_attack_embed(
            interaction.user,
            resolved_time,
            attacker_name,
            gif_path,
            resolved_battle_text,
            validated_link,
        )
    except AttackInputError as exc:
        await interaction.response.send_message(f"[x] {exc}", ephemeral=True)
        return
    except Exception:
        logger.exception(
            "Unexpected slash attack error. user=%s attacker_name=%r time=%r link=%r",
            interaction.user.id,
            attacker_name,
            attack_time,
            attack_link,
        )
        if interaction.response.is_done():
            await interaction.followup.send("[x] Internal error. Please try again shortly.", ephemeral=True)
        else:
            await interaction.response.send_message("[x] Internal error. Please try again shortly.", ephemeral=True)
        return

    await interaction.response.send_message(
        content="@everyone" if tag_everyone else None,
        embed=embed,
        file=local_file,
        allowed_mentions=discord.AllowedMentions(everyone=tag_everyone),
    )
    if interaction.guild_id is not None:
        record_activity(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            username=str(interaction.user),
            attack_increment=1,
        )
    logger.info("Slash attack sent by user=%s(%s)", interaction.user, interaction.user.id)


@bot.command(name="activity_top")
async def activity_top_prefix(ctx: commands.Context, limit: int = 10) -> None:
    if ctx.guild is None:
        await ctx.reply("[x] This command can only be used in a server.")
        return

    safe_limit = max(1, min(50, limit))
    rows = get_top_activity_rows(ctx.guild.id, safe_limit)
    if not rows:
        await ctx.reply("No activity data yet.")
        return

    lines = []
    for idx, row in enumerate(rows, start=1):
        username, chat_count, attack_count, total_count, _ = row
        lines.append(f"{idx}. {username} | chat={chat_count} | attack={attack_count} | total={total_count}")

    embed = discord.Embed(
        title=f"Top Activity (Top {safe_limit})",
        description="\n".join(lines),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    await ctx.send(embed=embed)


@bot.command(name="activity_export")
async def activity_export_prefix(ctx: commands.Context) -> None:
    if ctx.guild is None:
        await ctx.reply("[x] This command can only be used in a server.")
        return

    if not is_owner_user(ctx.author.id):
        await ctx.reply("[x] This export command is private and only available to the owner.")
        return

    if OWNER_USER_ID == 0:
        await ctx.reply("[x] DISCORD_OWNER_ID is not configured.")
        return

    try:
        excel_bytes = build_activity_excel(ctx.guild.id)
        filename = f"activity_export_{ctx.guild.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await ctx.author.send(file=discord.File(excel_bytes, filename=filename))
        await ctx.reply("I sent the activity Excel file to your DM.")
    except Exception:
        logger.exception("Failed to export activity for guild=%s", ctx.guild.id)
        await ctx.reply("[x] Failed to export activity file.")


@bot.tree.command(name="activity_top", description="Show top active users in this server")
@app_commands.describe(limit="How many users to display (1-50)")
async def activity_top_slash(
    interaction: discord.Interaction,
    limit: app_commands.Range[int, 1, 50] = 10,
) -> None:
    if interaction.guild_id is None:
        await interaction.response.send_message("[x] This command can only be used in a server.", ephemeral=True)
        return

    rows = get_top_activity_rows(interaction.guild_id, int(limit))
    if not rows:
        await interaction.response.send_message("No activity data yet.", ephemeral=True)
        return

    lines = []
    for idx, row in enumerate(rows, start=1):
        username, chat_count, attack_count, total_count, _ = row
        lines.append(f"{idx}. {username} | chat={chat_count} | attack={attack_count} | total={total_count}")

    embed = discord.Embed(
        title=f"Top Activity (Top {int(limit)})",
        description="\n".join(lines),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="activity_export", description="Export activity report to Excel (owner only)")
async def activity_export_slash(interaction: discord.Interaction) -> None:
    if interaction.guild_id is None:
        await interaction.response.send_message("[x] This command can only be used in a server.", ephemeral=True)
        return

    if OWNER_USER_ID == 0:
        await interaction.response.send_message("[x] DISCORD_OWNER_ID is not configured.", ephemeral=True)
        return

    if not is_owner_user(interaction.user.id):
        await interaction.response.send_message(
            "[x] This export command is private and only available to the owner.",
            ephemeral=True,
        )
        return

    try:
        excel_bytes = build_activity_excel(interaction.guild_id)
        filename = f"activity_export_{interaction.guild_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await interaction.response.send_message(
            "Activity report generated.",
            file=discord.File(excel_bytes, filename=filename),
            ephemeral=True,
        )
    except Exception:
        logger.exception("Failed slash activity export for guild=%s", interaction.guild_id)
        if interaction.response.is_done():
            await interaction.followup.send("[x] Failed to export activity file.", ephemeral=True)
        else:
            await interaction.response.send_message("[x] Failed to export activity file.", ephemeral=True)


@attack_slash.autocomplete("attacker_name")
async def attacker_name_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    if not current:
        return [app_commands.Choice(name=name, value=name) for name in ATTACKER_PRESETS[:25]]

    lowered = current.lower()
    filtered = [name for name in ATTACKER_PRESETS if lowered in name.lower()]
    return [app_commands.Choice(name=name, value=name) for name in filtered[:25]]


@attack_slash.autocomplete("attack_link")
async def attack_link_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    if not current:
        return [app_commands.Choice(name=url, value=url) for url in COMMON_LINKS[:25]]

    lowered = current.lower()
    filtered = [url for url in COMMON_LINKS if lowered in url.lower()]
    return [app_commands.Choice(name=url, value=url) for url in filtered[:25]]


if __name__ == "__main__":
    bot.run(TOKEN)
