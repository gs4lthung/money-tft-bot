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
- Prefix command example:
  !attack 16:20 "MAI HUONG DAY"
  !attack 16:20 "MAI HUONG DAY" @SomeUser https://youtube.com/watch?v=abc123
- Slash command: /attack
- The bot always uses local GIF file configured by DEFAULT_GIF_KEY.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv


BOT_ROOT = Path(__file__).resolve().parent

GIF_KEY_MAP: dict[str, str] = {
    "money-tft": "./gifs/money-tft.gif",
}
DEFAULT_GIF_KEY = "money-tft"

TIME_REGEX = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


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


def build_attack_embed(
    author: discord.abc.User,
    attack_time: str,
    attacker_name: str,
    gif_file_path: Path,
    target: Optional[discord.Member] = None,
    attack_link: Optional[str] = None,
) -> tuple[discord.Embed, discord.File]:
    description = f"{attack_time} - {attacker_name} toàn quân chuẩn bị, đợi tín hiệu tổng tấn công! 💥"
    if target is not None:
        description += f"\nTarget: {target.mention}"

    embed = discord.Embed(
        title="🚀 NPC MONEY TỔNG TIẾN CÔNG! 🚀",
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

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
_tree_synced = False


@bot.event
async def on_ready() -> None:
    global _tree_synced

    if not _tree_synced:
        try:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} slash command(s).")
            _tree_synced = True
        except Exception as exc:  # pragma: no cover
            print(f"Slash command sync failed: {exc}")

    print(f"Logged in as {bot.user} (ID: {bot.user.id if bot.user else 'unknown'})")


@bot.command(name="attack")
async def attack_prefix(
    ctx: commands.Context,
    attack_time: str,
    attacker_name: str,
    target: commands.Greedy[discord.Member],
    attack_link: Optional[str] = None,
) -> None:
    """
    Prefix usage:
    !attack <HH:MM> "<name>" [@target] [link]
    """
    try:
        validate_attack_inputs(attack_time, attacker_name)
        validated_link = validate_attack_link(attack_link)
        gif_path = resolve_gif_path(DEFAULT_GIF_KEY)
        selected_target = target[0] if target else None
        embed, local_file = build_attack_embed(
            ctx.author,
            attack_time,
            attacker_name,
            gif_path,
            selected_target,
            validated_link,
        )
    except AttackInputError as exc:
        await ctx.reply(f"❌ {exc}")
        return

    await ctx.send(embed=embed, file=local_file)


@attack_prefix.error
async def attack_prefix_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("❌ Missing arguments. Usage: !attack <HH:MM> \"<name>\" [@target] [link]")
        return

    if isinstance(error, commands.BadArgument):
        await ctx.reply("❌ Invalid argument. Check time, name, optional @target mention, and optional link.")
        return

    print(f"Unhandled prefix command error: {error}")
    await ctx.reply("❌ Something went wrong while executing the attack command.")


@bot.tree.command(name="attack", description="Execute an attack with a local GIF")
@app_commands.describe(
    attack_time="Time in HH:MM format (24-hour), for example: 16:20",
    attacker_name="Displayed attacker name",
    target="Optional target to mention",
    attack_link="Optional URL (YouTube/Facebook/etc.)",
)
async def attack_slash(
    interaction: discord.Interaction,
    attack_time: str,
    attacker_name: str,
    target: Optional[discord.Member] = None,
    attack_link: Optional[str] = None,
) -> None:
    try:
        validate_attack_inputs(attack_time, attacker_name)
        validated_link = validate_attack_link(attack_link)
        gif_path = resolve_gif_path(DEFAULT_GIF_KEY)
        embed, local_file = build_attack_embed(
            interaction.user,
            attack_time,
            attacker_name,
            gif_path,
            target,
            validated_link,
        )
    except AttackInputError as exc:
        await interaction.response.send_message(f"❌ {exc}", ephemeral=True)
        return

    await interaction.response.send_message(embed=embed, file=local_file)


if __name__ == "__main__":
    bot.run(TOKEN)
