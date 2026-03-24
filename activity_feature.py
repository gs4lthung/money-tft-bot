from __future__ import annotations

import io
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

import discord
from discord import app_commands
from discord.ext import commands
from openpyxl import Workbook


CHANNEL_MENTION_REGEX = re.compile(r"^<#(\d+)>$")


def register_activity_feature(
    bot: commands.Bot,
    logger,
    activity_db_path: Path,
    owner_user_id: int,
    period_values: set[str],
    metric_values: set[str],
) -> None:
    def init_activity_db() -> None:
        with sqlite3.connect(activity_db_path) as conn:
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_activity_events (
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL DEFAULT 0,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

            event_columns = {row[1] for row in conn.execute("PRAGMA table_info(user_activity_events)").fetchall()}
            if "channel_id" not in event_columns:
                conn.execute("ALTER TABLE user_activity_events ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0")

            conn.commit()

    def is_owner_user(user_id: int) -> bool:
        return owner_user_id != 0 and user_id == owner_user_id

    def record_activity(
        guild_id: int,
        channel_id: int,
        user_id: int,
        username: str,
        chat_increment: int = 0,
        attack_increment: int = 0,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(activity_db_path) as conn:
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

            if chat_increment > 0:
                conn.execute(
                    """
                    INSERT INTO user_activity_events
                    (guild_id, channel_id, user_id, username, event_type, event_count, created_at)
                    VALUES (?, ?, ?, ?, 'chat', ?, ?)
                    """,
                    (guild_id, channel_id, user_id, username, chat_increment, now_iso),
                )

            if attack_increment > 0:
                conn.execute(
                    """
                    INSERT INTO user_activity_events
                    (guild_id, channel_id, user_id, username, event_type, event_count, created_at)
                    VALUES (?, ?, ?, ?, 'attack', ?, ?)
                    """,
                    (guild_id, channel_id, user_id, username, attack_increment, now_iso),
                )

            conn.commit()

    def period_start_iso(period: str) -> Optional[str]:
        now = datetime.now(timezone.utc)
        if period == "all":
            return None
        if period == "day":
            return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        if period == "month":
            return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        raise ValueError(f"Unsupported period: {period}")

    def order_expr(metric: str) -> str:
        if metric == "chat":
            return "chat_count DESC, total_count DESC, attack_count DESC"
        if metric == "attack":
            return "attack_count DESC, total_count DESC, chat_count DESC"
        return "total_count DESC, chat_count DESC, attack_count DESC"

    def get_top_activity_rows(
        guild_id: int,
        limit: int,
        period: str = "all",
        metric: str = "total",
        channel_id: Optional[int] = None,
    ) -> list[tuple[str, int, int, int, str]]:
        sort_by = order_expr(metric)

        with sqlite3.connect(activity_db_path) as conn:
            if period == "all" and channel_id is None:
                rows = conn.execute(
                    f"""
                    SELECT
                        username,
                        chat_count,
                        attack_count,
                        (chat_count + attack_count) AS total_count,
                        COALESCE(last_active, '')
                    FROM user_activity
                    WHERE guild_id = ?
                    ORDER BY {sort_by}
                    LIMIT ?
                    """,
                    (guild_id, limit),
                ).fetchall()
            else:
                start_iso = period_start_iso(period)
                where_clauses = ["guild_id = ?"]
                params: list[object] = [guild_id]

                if channel_id is not None:
                    where_clauses.append("channel_id = ?")
                    params.append(channel_id)

                if start_iso is not None:
                    where_clauses.append("created_at >= ?")
                    params.append(start_iso)

                where_sql = " AND ".join(where_clauses)
                rows = conn.execute(
                    f"""
                    SELECT
                        username,
                        COALESCE(SUM(CASE WHEN event_type = 'chat' THEN event_count ELSE 0 END), 0) AS chat_count,
                        COALESCE(SUM(CASE WHEN event_type = 'attack' THEN event_count ELSE 0 END), 0) AS attack_count,
                        COALESCE(SUM(event_count), 0) AS total_count,
                        COALESCE(MAX(created_at), '') AS last_active
                    FROM user_activity_events
                    WHERE {where_sql}
                    GROUP BY guild_id, user_id
                    ORDER BY {sort_by}
                    LIMIT ?
                    """,
                    tuple([*params, limit]),
                ).fetchall()

        return [(str(r[0]), int(r[1]), int(r[2]), int(r[3]), str(r[4])) for r in rows]

    def build_activity_excel(guild_id: int, period: str = "all", channel_id: Optional[int] = None) -> io.BytesIO:
        rows = get_top_activity_rows(guild_id, 1000, period=period, metric="total", channel_id=channel_id)
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

    @bot.listen("on_ready")
    async def activity_on_ready() -> None:
        init_activity_db()

    @bot.listen("on_message")
    async def activity_on_message(message: discord.Message) -> None:
        if message.author.bot:
            return

        if message.guild is not None:
            try:
                record_activity(
                    guild_id=message.guild.id,
                    channel_id=message.channel.id,
                    user_id=message.author.id,
                    username=str(message.author),
                    chat_increment=1,
                )
            except Exception:
                logger.exception("Failed to record chat activity for user=%s", message.author.id)

        await bot.process_commands(message)

    @bot.listen("on_attack_event")
    async def activity_on_attack_event(guild_id: int, channel_id: int, user_id: int, username: str) -> None:
        try:
            record_activity(
                guild_id=guild_id,
                channel_id=channel_id,
                user_id=user_id,
                username=username,
                attack_increment=1,
            )
        except Exception:
            logger.exception("Failed to record attack activity for user=%s in guild=%s", user_id, guild_id)

    @bot.command(name="activity_top")
    async def activity_top_prefix(ctx: commands.Context, *args: str) -> None:
        if ctx.guild is None:
            await ctx.reply("[x] This command can only be used in a server.")
            return

        period = "all"
        metric = "total"
        limit = 10
        selected_channel_id: Optional[int] = None

        for arg in args:
            lowered = arg.lower().strip()
            channel_match = CHANNEL_MENTION_REGEX.match(arg)
            if channel_match:
                selected_channel_id = int(channel_match.group(1))
                continue

            if lowered.isdigit():
                limit = int(lowered)
                continue
            if lowered in period_values:
                period = lowered
                continue
            if lowered in metric_values:
                metric = lowered
                continue

            await ctx.reply("[x] Invalid option. Use: all/day/month, total/chat/attack, optional #channel, and optional limit (1-50).")
            return

        safe_limit = max(1, min(50, limit))
        rows = get_top_activity_rows(
            ctx.guild.id,
            safe_limit,
            period=period,
            metric=metric,
            channel_id=selected_channel_id,
        )
        if not rows:
            await ctx.reply("No activity data yet.")
            return

        lines = []
        for idx, row in enumerate(rows, start=1):
            username, chat_count, attack_count, total_count, _ = row
            lines.append(f"{idx}. {username} | chat={chat_count} | attack={attack_count} | total={total_count}")

        channel_label = "ALL CHANNELS"
        if selected_channel_id is not None:
            channel_obj = ctx.guild.get_channel(selected_channel_id)
            channel_label = f"#{channel_obj.name}" if channel_obj is not None else f"channel:{selected_channel_id}"

        embed = discord.Embed(
            title=f"Top Activity ({period.upper()} | {metric.upper()} | {channel_label} | Top {safe_limit})",
            description="\n".join(lines),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        await ctx.send(embed=embed)

    @bot.command(name="activity_export")
    async def activity_export_prefix(ctx: commands.Context, *args: str) -> None:
        if ctx.guild is None:
            await ctx.reply("[x] This command can only be used in a server.")
            return

        if not is_owner_user(ctx.author.id):
            await ctx.reply("[x] This export command is private and only available to the owner.")
            return

        if owner_user_id == 0:
            await ctx.reply("[x] DISCORD_OWNER_ID is not configured.")
            return

        period_value = "all"
        selected_channel_id: Optional[int] = None

        for arg in args:
            lowered = arg.lower().strip()
            channel_match = CHANNEL_MENTION_REGEX.match(arg)
            if channel_match:
                selected_channel_id = int(channel_match.group(1))
                continue

            if lowered in period_values:
                period_value = lowered
                continue

            await ctx.reply("[x] Invalid option. Use period (all/day/month) and optional #channel mention.")
            return

        try:
            excel_bytes = build_activity_excel(ctx.guild.id, period=period_value, channel_id=selected_channel_id)
            channel_suffix = f"_ch{selected_channel_id}" if selected_channel_id is not None else "_allch"
            filename = (
                f"activity_export_{period_value}{channel_suffix}_{ctx.guild.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            await ctx.author.send(file=discord.File(excel_bytes, filename=filename))
            await ctx.reply("I sent the activity Excel file to your DM.")
        except Exception:
            logger.exception("Failed to export activity for guild=%s", ctx.guild.id)
            await ctx.reply("[x] Failed to export activity file.")

    @bot.tree.command(name="activity_top", description="Show top active users in this server")
    @app_commands.describe(
        limit="How many users to display (1-50)",
        period="Time range: all time, today, or this month",
        metric="Sort by total, chat, or attack",
        channel="Optional channel filter; default is all channels",
    )
    async def activity_top_slash(
        interaction: discord.Interaction,
        limit: app_commands.Range[int, 1, 50] = 10,
        period: Literal["all", "day", "month"] = "all",
        metric: Literal["total", "chat", "attack"] = "total",
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        if interaction.guild_id is None:
            await interaction.response.send_message("[x] This command can only be used in a server.", ephemeral=True)
            return

        selected_channel_id = channel.id if channel is not None else None
        rows = get_top_activity_rows(
            interaction.guild_id,
            int(limit),
            period=period,
            metric=metric,
            channel_id=selected_channel_id,
        )
        if not rows:
            await interaction.response.send_message("No activity data yet.", ephemeral=True)
            return

        lines = []
        for idx, row in enumerate(rows, start=1):
            username, chat_count, attack_count, total_count, _ = row
            lines.append(f"{idx}. {username} | chat={chat_count} | attack={attack_count} | total={total_count}")

        channel_label = f"#{channel.name}" if channel is not None else "ALL CHANNELS"

        embed = discord.Embed(
            title=f"Top Activity ({period.upper()} | {metric.upper()} | {channel_label} | Top {int(limit)})",
            description="\n".join(lines),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        await interaction.response.send_message(embed=embed)

    @bot.tree.command(name="activity_export", description="Export activity report to Excel (owner only)")
    @app_commands.describe(
        period="Time range for export: all time, today, or this month",
        channel="Optional channel filter; default is all channels",
    )
    async def activity_export_slash(
        interaction: discord.Interaction,
        period: Literal["all", "day", "month"] = "all",
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        if interaction.guild_id is None:
            await interaction.response.send_message("[x] This command can only be used in a server.", ephemeral=True)
            return

        if owner_user_id == 0:
            await interaction.response.send_message("[x] DISCORD_OWNER_ID is not configured.", ephemeral=True)
            return

        if not is_owner_user(interaction.user.id):
            await interaction.response.send_message(
                "[x] This export command is private and only available to the owner.",
                ephemeral=True,
            )
            return

        try:
            selected_channel_id = channel.id if channel is not None else None
            excel_bytes = build_activity_excel(interaction.guild_id, period=period, channel_id=selected_channel_id)
            channel_suffix = f"_ch{selected_channel_id}" if selected_channel_id is not None else "_allch"
            filename = (
                f"activity_export_{period}{channel_suffix}_{interaction.guild_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            await interaction.response.send_message(
                f"Activity report generated for {period}.",
                file=discord.File(excel_bytes, filename=filename),
                ephemeral=True,
            )
        except Exception:
            logger.exception("Failed slash activity export for guild=%s", interaction.guild_id)
            if interaction.response.is_done():
                await interaction.followup.send("[x] Failed to export activity file.", ephemeral=True)
            else:
                await interaction.response.send_message("[x] Failed to export activity file.", ephemeral=True)
