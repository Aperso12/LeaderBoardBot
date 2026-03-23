#IMPORTS
import datetime
import os
import dotenv
import discord
import logging
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import UpdateOne
import discord.ext.tasks
from database import Server_LeaderBoard_Entry,server_entries,active_sessions,guild_settings,user_settings
import asyncio
from keep_alive import keep_alive
#SETUP VARIABLES

#logging setup
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
#get token
envfile=dotenv.load_dotenv()
raw_token=os.getenv("TOKEN_FOR_DISCORD_BOT")
BOT_TOKEN = raw_token.strip().replace('"', '').replace("'", "") if raw_token else None
#cache(s)
special_channels_cache:dict[int,int]={}
keep_alive()


#HELPER FUNCTIONS
def format_leaderboard(template: str, text_data: list, voice_data: list, role_text_data: list, role_voice_data:list,special_data: list,role_special_data:list) -> str:
    # A tiny internal helper to safely grab a user mention or return a placeholder
    def safe_get(data_list, index):
        if index < len(data_list):
            return f"<@{data_list[index]['u']}>"
        return "*No one yet!*" # Fallback if they don't have a 2nd or 3rd place
    #Format string vars: %1,%2.%3:top 3 in text channels
    #%4,%5,%6 for top 3 voice
    #%7,%8,%9 for top 3 in role filter text 
    #%a,%b,%c for top 3 in role filter voice
    #%d,%e,%f for top 3 in special channel 
    #%g,%h,%i for top 3 in special channel filtered by role
    return template \
        .replace("%1", safe_get(text_data, 0)) \
        .replace("%2", safe_get(text_data, 1)) \
        .replace("%3", safe_get(text_data, 2)) \
        .replace("%4", safe_get(voice_data, 0)) \
        .replace("%5", safe_get(voice_data, 1)) \
        .replace("%6", safe_get(voice_data, 2)) \
        .replace("%7", safe_get(role_text_data, 0)) \
        .replace("%8", safe_get(role_text_data, 1)) \
        .replace("%9", safe_get(role_text_data, 2)) \
        .replace("%a", safe_get(role_voice_data, 0)) \
        .replace("%b", safe_get(role_voice_data, 1)) \
        .replace("%c", safe_get(role_voice_data, 2)) \
        .replace("%d",safe_get(special_data,0)) \
        .replace("%e",safe_get(special_data,1)) \
        .replace("%f",safe_get(special_data,2)) \
        .replace("%g",safe_get(role_special_data,0)) \
        .replace("%h",safe_get(role_special_data,1)) \
        .replace("%i",safe_get(role_special_data,2)) \


#CLASSES
class FormatTextModal(discord.ui.Modal, title='Set Leaderboard Text'):
    # This creates a large, multi-line text box
    format_input = discord.ui.TextInput(
        label='Leaderboard Text Setter',
        style=discord.TextStyle.paragraph,
        placeholder='Type your layout here. Use %1, %2, etc. Press enter for new lines',
        required=True,
        max_length=1000
    )

    async def on_submit(self, interaction: discord.Interaction):
        # We grab the text exactly as they typed it in the pop-up
        text = self.format_input.value
        
        # Save it to the database
        await guild_settings.update_one(
            {"g": interaction.guild_id},
            {"$set": {"t": text}},
            upsert=True
        )
        
        await interaction.response.send_message("Successfully set your new multi-line format!", ephemeral=True)

class CheckTextModal(discord.ui.Modal, title='Test Leaderboard Format'):
    format_input = discord.ui.TextInput(
        label='Paste your format to test',
        style=discord.TextStyle.paragraph,
        placeholder='Rules:\n%1 = 1st in text channels\n%2 = 2nd Text\n%3 = 3rd Text\n%4 = 1st Voice...',
        required=True,
        max_length=1000
    )

    async def on_submit(self, interaction: discord.Interaction):
        text = self.format_input.value
        formatted_text = text \
            .replace("%1", "[1st in text]") \
            .replace("%2", "[2nd in text]") \
            .replace("%3", "[3rd in text]") \
            .replace("%4", "[1st in voice]") \
            .replace("%5", "[2nd in voice]") \
            .replace("%6", "[3rd in voice]")
            
        if interaction.guild_id:
            
            role_filter = await Server_LeaderBoard_Entry.get_roles_filter(interaction.guild_id)
            if role_filter:
                formatted_text = formatted_text \
                    .replace("%7", "[1st in text channels with role constraint]") \
                    .replace("%8", "[2nd in text channels with role constraint]") \
                    .replace("%9", "[3rd text channels with role constraint]") \
                    .replace("%a", "[1st voice channels with role constraint]") \
                    .replace("%b", "[2nd voice channels with role constraint]") \
                    .replace("%c", "[3rd voice channels with role constraint]")
            else:
                formatted_text = formatted_text \
                    .replace("%7", "[No role filter set]") \
                    .replace("%8", "[No role filter set]") \
                    .replace("%9", "[No role filter set]") \
                    .replace("%a", "[No role filter set]") \
                    .replace("%b", "[No role filter set]") \
                    .replace("%c", "[No role filter set]")
                    
            if interaction.guild_id in special_channels_cache:
                formatted_text = formatted_text \
                    .replace("%d", "[1st special]") \
                    .replace("%e", "[2nd special]") \
                    .replace("%f", "[3rd special]") \
                    .replace("%g", "[1st special channels with role constraint]") \
                    .replace("%h", "[2nd special channels with role constraint]") \
                    .replace("%i", "[3rd special channels with role constraint]")
            else:
                formatted_text = formatted_text \
                    .replace("%d", "[No special channel set]") \
                    .replace("%e", "[No special channel set]") \
                    .replace("%f", "[No special channel set]") \
                    .replace("%g", "[No special channel set]") \
                    .replace("%h", "[No special channel set]") \
                    .replace("%i", "[No special channel set]")
        else:
            formatted_text = formatted_text \
                .replace("%7", "[No role filter set]") \
                .replace("%8", "[No role filter set]") \
                .replace("%9", "[No role filter set]") \
                .replace("%a", "[No role filter set]") \
                .replace("%b", "[No role filter set]") \
                .replace("%c", "[No role filter set]") \
                .replace("%d", "[No special channel set]") \
                .replace("%e", "[No special channel set]") \
                .replace("%f", "[No special channel set]") \
                .replace("%g", "[No special channel set]") \
                .replace("%h", "[No special channel set]") \
                .replace("%i", "[No special channel set]")
        # Send them the preview! (ephemeral=True means only they see it)
        await interaction.response.send_message(f"**Here is your preview:**\n\n{formatted_text}", ephemeral=True)

class Client(discord.Client):
    def __init__(self) -> None:
        intents=discord.Intents.default()
        intents.message_content=True
        intents.voice_states=True
        intents.members=True
        super().__init__(intents=intents,max_messages=0,chunk_guilds_at_startup=False)
        self.tree = discord.app_commands.CommandTree(self)
        self.MY_GUILD = discord.Object(id=1480047776812236861)
        self.MY_TEST_BIG_GUILD=discord.Object(id=1444029973043089481)
    async def setup_hook(self):
        await active_sessions.create_index("s", expireAfterSeconds=86400)
        await server_entries.create_index("t", expireAfterSeconds=1209600)
        await guild_settings.create_index([("d", 1), ("h", 1), ("m", 1)])

        self.tree.copy_global_to(guild=self.MY_GUILD)
        await self.tree.sync(guild=self.MY_GUILD)
        print(f"Slash commands synced to guild {self.MY_GUILD.id}")
        self.tree.copy_global_to(guild=self.MY_TEST_BIG_GUILD)
        await self.tree.sync(guild=self.MY_TEST_BIG_GUILD)
        print(f"Slash commands synced to guild {self.MY_TEST_BIG_GUILD.id}")


client=Client()
#Now that client exists, we can get into the meat of discord.py

#Setup all event responses,commands,loops

#LOOPS
@discord.ext.tasks.loop(minutes=1)
async def voice_heartbeat():
    now = discord.utils.utcnow()
    batch_leaderboard = []
    batch_sessions = []

    cursor = active_sessions.find({}) 
    
    async for session in cursor:
        uid, gid, start = session["u"], session["g"], session["s"]
        duration_hours = (now - start).total_seconds() / 3600
        
        batch_leaderboard.append(
            UpdateOne(
                {"g": gid, "u": uid},
                {"$inc": {"v": duration_hours}, "$set": {"t": now}},
                upsert=True
            )
        )
        
        batch_sessions.append(
            UpdateOne({"u": uid, "g": gid}, {"$set": {"s": now}})
        )

        if len(batch_leaderboard) >= 50:
            await server_entries.bulk_write(batch_leaderboard)
            await active_sessions.bulk_write(batch_sessions)
            batch_leaderboard, batch_sessions = [], []
            await asyncio.sleep(0.1)

    if batch_leaderboard:
        await server_entries.bulk_write(batch_leaderboard)
        await active_sessions.bulk_write(batch_sessions)

@discord.ext.tasks.loop(minutes=1)
async def weekly_poster():
    now = discord.utils.utcnow()
    current_day = now.weekday()
    current_hour = now.hour     
    current_minute = now.minute 

    cursor = guild_settings.find({"d": current_day, "h": current_hour, "m": current_minute})
    
    async for settings in cursor:
        guild_id = settings["g"]
        channel_id = settings.get("p") 
        format_template = settings.get("t")
        
        if not channel_id or not format_template:
            continue

        channel = client.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            continue
            
        filter_dict = settings.get("f")
        role_text_top_3, role_voice_top_3, role_special_top_3 = [], [], []
        
        text_top_3 = await Server_LeaderBoard_Entry.get_top_3_text(guild_id)
        voice_top_3 = await Server_LeaderBoard_Entry.get_top_3_voice(guild_id)
        special_top_3 = await Server_LeaderBoard_Entry.get_top_3_special_channel(guild_id)
        
        if filter_dict:
            role_text_top_3 = await Server_LeaderBoard_Entry.get_top_3_text(guild_id, filter_dict["r"], filter_dict.get("a", False))
            role_voice_top_3 = await Server_LeaderBoard_Entry.get_top_3_voice(guild_id, filter_dict["r"], filter_dict.get("a", False))
            role_special_top_3 = await Server_LeaderBoard_Entry.get_top_3_special_channel(guild_id, filter_dict["r"], filter_dict.get("a", False))

        formatted_text = format_leaderboard(format_template, text_top_3, voice_top_3, role_text_top_3, role_voice_top_3, special_top_3, role_special_top_3)
        
        try:
            await channel.send(formatted_text)
            await server_entries.update_many({"g": guild_id}, {"$unset": {"m": "", "v": "", "s": ""}})
        except discord.Forbidden:
            print(f"Missing permissions to post in guild {guild_id}")
            
        await asyncio.sleep(0.5)      

@weekly_poster.before_loop
async def before_weekly_poster():
    await client.wait_until_ready()



#EVENTS
@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot: return

    uid, gid = member.id, member.guild.id
    now = discord.utils.utcnow()
    

    if before.channel is None and after.channel is not None:
        await active_sessions.update_one(
            {"u": uid, "g": gid},
            {"$set": {"s": now}},
            upsert=True
        )


    elif before.channel is not None and after.channel is None:

        session = await active_sessions.find_one_and_delete({"u": uid, "g": gid})
        
        if session:
            duration_seconds = (now - session["s"]).total_seconds() # get duration between them. #NOTE: .total_seconds is only possible on a timedelta, the duration between two datetime objects
            duration_hours = duration_seconds / 3600
            
            # Update the main leaderboard
            await Server_LeaderBoard_Entry.add_voice_hour(gid, uid, duration_hours)

@client.event
async def on_ready():
    print(f"We have logged in as {client.user}")
    
    if not voice_heartbeat.is_running():
        voice_heartbeat.start()
        
    if not weekly_poster.is_running():
        weekly_poster.start()
    
    special_channels_cache.clear() # Clear it first in case of a reconnect
    cursor = guild_settings.find({"c": {"$exists": True}},{"c":1,"g":1})
    async for setting in cursor:
        guild_id = setting["g"]
        channel_id = setting["c"]
        special_channels_cache[guild_id] = channel_id
        
    print(f"We have {len(special_channels_cache)} special channels loaded into RAM")

@client.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if before.bot:
        return

    if before.roles != after.roles:
        current_role_ids = [role.id for role in after.roles]
        
        await Server_LeaderBoard_Entry.update_roles(after.guild.id, after.id, current_role_ids)

@client.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.user.bot:
        return
    if interaction.type == discord.InteractionType.application_command:
        
        if not interaction.guild_id:
            return

        user_id = interaction.user.id
        guild_id = interaction.guild_id
        
        await Server_LeaderBoard_Entry.increment_text(guild_id,user_id)


@client.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    guild_id = message.guild.id
    channel_id = message.channel.id

    designated_channel = special_channels_cache.get(guild_id)

    if designated_channel and channel_id == designated_channel:
        await Server_LeaderBoard_Entry.increment_special_channel(guild_id, message.author.id)

    await Server_LeaderBoard_Entry.increment_text(guild_id, message.author.id)

#COMMANDS

@client.tree.command(name="add-role-filter", description="Add a role to the leaderboard filter array")
@discord.app_commands.default_permissions(manage_guild=True)
async def add_role_filter(interaction: discord.Interaction, role: discord.Role):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry, not for DMs")
        return
    await interaction.response.defer()
    
    # $addToSet pushes the role.id into the "r" array inside the "f" object.
    await guild_settings.update_one(
        {"g": interaction.guild_id},
        {"$addToSet": {"f.r": role.id}}, 
        upsert=True
    )
    
    await interaction.followup.send(f"Added {role.mention} to the leaderboard filter!")

@client.tree.command(name="remove-role-filter",description="Remove a role from the leaderboard filter array")
@discord.app_commands.default_permissions(manage_guild=True)
async def remove_role_filter(interaction:discord.Interaction,role:discord.Role):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry,not for DMs")
        return
    await interaction.response.defer()

    result=await guild_settings.update_one({"g": interaction.guild_id}, {"$pull": {"f.r": role.id}}, upsert=True)

    if result.modified_count>0:
        await interaction.followup.send("Removed role from filter")
    else:
        await interaction.followup.send("That role wasn't in the filter to begin with")

@client.tree.command(name="get-current-role-filter",description="Get the current role filter")
@discord.app_commands.default_permissions(manage_guild=True)
async def get_role_filter(interaction:discord.Interaction,role:discord.Role):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry, not for DMs")
        return
    await interaction.response.defer()

    role_filter = await Server_LeaderBoard_Entry.get_roles_filter(interaction.guild_id)
    
    if not role_filter or not role_filter.get("r"):
        await interaction.followup.send("No role filters have been set yet! Use `/add-role-filter`.")
        return

    # Convert the list of IDs into a string of mentions like @Role1, @Role2
    role_mentions = ", ".join([f"<@&{role_id}>" for role_id in role_filter["r"]])
    require_all = role_filter.get("a", False)

    await interaction.followup.send(
        f"**Current Role Filter:**\n"
        f"**Roles:** {role_mentions}\n"
        f"**Requirement:** {'Must have ALL roles' if require_all else 'Must have ANY of these roles'}"
    )

@client.tree.command(name="check-leaderboard",description="get leaderboard")
async def check_leaderboard(interaction:discord.Interaction):
    if not isinstance(interaction.channel,discord.TextChannel):
        await interaction.response.send_message("This command is only runnable in a text channel")
        return
    if not (interaction.guild and interaction.guild_id):
        await interaction.response.send_message("This is not for DMs")
        return
    await interaction.response.defer()
    #Check if the server is initialized or not
    settings=await Server_LeaderBoard_Entry.get_settings(interaction.guild_id)
    if not settings :
        await interaction.followup.send("Sorry, your server is not initialized")
        return
    if not settings.get("i"):
        await interaction.followup.send("Sorry, your server is not initialized")
        return

    filter:dict|None=(await Server_LeaderBoard_Entry.get_roles_filter(interaction.guild_id))
    role_text_top_3=[]
    role_voice_top_3=[]
    role_special_top_3=[]

    text_top_3=await Server_LeaderBoard_Entry.get_top_3_text(interaction.guild_id)
    voice_top_3=await Server_LeaderBoard_Entry.get_top_3_voice(interaction.guild_id)

    if(filter):
        role_text_top_3=await Server_LeaderBoard_Entry.get_top_3_text(interaction.guild_id,filter.get("r"),True if filter.get("a") else False)
        role_voice_top_3=await Server_LeaderBoard_Entry.get_top_3_voice(interaction.guild_id,filter["r"],True if filter.get("a") else False)
        role_special_top_3=await Server_LeaderBoard_Entry.get_top_3_special_channel(interaction.guild_id,filter["r"],True if filter.get("a") else False)

    special_top_3=await Server_LeaderBoard_Entry.get_top_3_special_channel(interaction.guild_id)

    if not settings.get("t"):
        await interaction.followup.send("Sorry, no special text is set up. Make sure to set that up before running this command")
        return
    formatted_text=format_leaderboard(settings["t"],text_top_3,voice_top_3,role_text_top_3,role_voice_top_3,special_top_3,role_special_top_3)
    await interaction.followup.send(formatted_text)


@client.tree.command(name="check-special-text", description="Test your leaderboard format string with placeholders")
async def check_special_text(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("This command is only runnable in a text channel")
        return
        
    await interaction.response.send_modal(CheckTextModal())
   

@client.tree.command(name="get-special-text-rules",description='The rules for your special text to work')
async def get_special_rules(interaction:discord.Interaction):
    await interaction.response.send_message("Format string vars: **%1,%2.%3:top 3 in text channels \n%4,%5,%6 for top 3 voice \n%7,%8,%9 for top 3 in role filter text \n%a,%b,%c for top 3 in role filter voice \n%d,%e,%f for top 3 in special channel \n%g,%h,%i for top 3 in special channel filtered by role")

@client.tree.command(name="set-special-text",description="Sets the text used for check-leaderboard and posted if post time is set")
@discord.app_commands.default_permissions(manage_guild=True)
async def set_special_text(interaction:discord.Interaction):
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("This command is only runnable in a text channel")
        return
        
    await interaction.response.send_modal(FormatTextModal())

@client.tree.command(name="set-special-channel", description="Set the special channel to be tracked for the leaderboard")
@discord.app_commands.default_permissions(manage_guild=True)
async def set_special_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry, not for DMs")
        return
    await interaction.response.defer()
    
    await guild_settings.update_one(
        {"g": interaction.guild_id}, 
        {"$set": {"c": channel.id}}, 
        upsert=True
    )
    
    special_channels_cache[interaction.guild_id] = channel.id
    
    await interaction.followup.send(f"Successfully set {channel.mention} as the special leaderboard channel!")

@client.tree.command(name="remove-special-channel", description="Remove the special channel tracking")
@discord.app_commands.default_permissions(manage_guild=True)
async def remove_special_channel(interaction: discord.Interaction):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry, not for DMs")
        return
    await interaction.response.defer()
    

    result = await guild_settings.update_one(
        {"g": interaction.guild_id}, 
        {"$unset": {"c": ""}} # $unset removes a specific field completely
    )
    
    if interaction.guild_id in special_channels_cache:
        del special_channels_cache[interaction.guild_id]
        
    if result.modified_count > 0:
        await interaction.followup.send("Successfully removed the special channel tracking.")
    else:
        await interaction.followup.send("No special channel was set to begin with!")

@client.tree.command(name="set-post-schedule", description="Set when and where the weekly leaderboard posts")
@discord.app_commands.choices(day=[
    discord.app_commands.Choice(name="Monday", value=0),
    discord.app_commands.Choice(name="Tuesday", value=1),
    discord.app_commands.Choice(name="Wednesday", value=2),
    discord.app_commands.Choice(name="Thursday", value=3),
    discord.app_commands.Choice(name="Friday", value=4),
    discord.app_commands.Choice(name="Saturday", value=5),
    discord.app_commands.Choice(name="Sunday", value=6)
])
@discord.app_commands.describe(channel="The channel where the bot should post", hour="Hour of the day to post (0-23) in your local time",minute="Minute of the hour to post (0-59)")
@discord.app_commands.default_permissions(manage_guild=True)
async def set_post_schedule(interaction: discord.Interaction, channel: discord.TextChannel, day: discord.app_commands.Choice[int], hour: int, minute: int):
    if not interaction.guild_id:
        await interaction.response.send_message("Sorry, this can only be run in a server.", ephemeral=True)
        return
        
    if not (0 <= hour <= 23) or not (0 <= minute <= 59):
        await interaction.response.send_message("Please provide a valid hour (0-23) and minute (0-59).", ephemeral=True)
        return
        
    await interaction.response.defer()

    offset = await Server_LeaderBoard_Entry.get_current_timezone_offset(interaction.user.id)
    offset = float(offset) if offset is not None else 0.0

    offset_minutes = int(offset * 60)

    local_total_minutes = (day.value * 24 * 60) + (hour * 60) + minute

    utc_total_minutes = local_total_minutes - offset_minutes

    utc_total_minutes = utc_total_minutes % 10080
    utc_day = utc_total_minutes // (24 * 60)
    utc_hour = (utc_total_minutes % (24 * 60)) // 60
    utc_minute = utc_total_minutes % 60

    await guild_settings.update_one(
        {"g": interaction.guild_id},
        {"$set": {
            "d": utc_day, 
            "h": utc_hour, 
            "m": utc_minute,
            "p": channel.id
        }},
        upsert=True
    )
    
    now = discord.utils.utcnow()
    days_ahead = utc_day - now.weekday()
    
    if days_ahead < 0 or (days_ahead == 0 and (now.hour > utc_hour or (now.hour == utc_hour and now.minute >= utc_minute))):
        days_ahead += 7
        
    next_post = now + datetime.timedelta(days=days_ahead)
    next_post = next_post.replace(hour=utc_hour, minute=utc_minute, second=0, microsecond=0)
    timestamp = int(next_post.timestamp())

    timezone_warning = "" if offset != 0.0 else "\n*(Note: You haven't set a timezone with `/set-timezone-auto`, so I assumed UTC!)*"
    
    await interaction.followup.send(f"Schedule set. The leaderboard will post in {channel.mention} every **{day.name}** at your local time of **{hour:02d}:{minute:02d}** (<t:{timestamp}:t>).{timezone_warning}")

@client.tree.command(name="set-timezone-auto", description="Set your timezone by telling the bot your current local time")
@discord.app_commands.describe(hour="The current hour on your clock (0-23 in 24 hour clock)",minute="The current minute on your clock (0-59)")
async def set_timezone_auto(interaction: discord.Interaction, hour: int, minute: int):
    if not (0 <= hour <= 23) or not (0 <= minute <= 59):
        await interaction.response.send_message("Please enter a valid time (Hour: 0-23, Minute: 0-59).", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    utc_now = discord.utils.utcnow()
    
    utc_minutes = (utc_now.hour * 60) + utc_now.minute
    local_minutes = (hour * 60) + minute
    diff_minutes = local_minutes - utc_minutes
    if diff_minutes > (14 * 60):
        diff_minutes -= (24 * 60)
    elif diff_minutes < (-12 * 60):
        diff_minutes += (24 * 60)
    offset = round(diff_minutes / 60, 2)

    await user_settings.update_one({"u": interaction.user.id}, {"$set": {"tz": offset}}, upsert=True)

    await interaction.followup.send(f"Math complete! You told me it is {hour:02d}:{minute:02d}. That means your timezone offset is **UTC {offset:+g}**.\nI've saved this to your profile!")



@client.tree.command(name="initialize",description="This command can only be run once")
@discord.app_commands.default_permissions(manage_guild=True)
async def initialize_guild(interaction:discord.Interaction):
    if not (interaction.guild_id and interaction.guild):
        await interaction.response.send_message("Server ID unavailable")
        return
    await interaction.response.defer()
    settings=(await guild_settings.find_one({"g":interaction.guild_id}))
    if settings and settings.get("i"):
        await interaction.followup.send("Sorry, you're server is already initialized")
        return
    time_check=discord.utils.utcnow()-datetime.timedelta(days=7)
    batch = []
    processed_count = 0
    
    #setup members cache
    current_members = {}
    async for member in interaction.guild.fetch_members(limit=None):
        current_members[member.id] = [role.id for role in member.roles]
    
    for channel in interaction.guild.text_channels:
        try:

            async for message in channel.history(after=time_check, limit=None):
                if message.author.bot:
                    continue

                print(f"User {message.author.name} is a member: {message.author.id in current_members}")

                if not message.author.id in current_members:
                    continue

                roles = current_members[message.author.id]


                batch.append(
                    UpdateOne(
                        {"g": interaction.guild_id, "u": message.author.id},
                        {"$inc": {"m": 1}, "$set": {"t": discord.utils.utcnow(),"r":roles}},
                        upsert=True
                    )
                )

                if len(batch) >= 200:
                    await server_entries.bulk_write(batch)
                    batch = [] # 
                    await asyncio.sleep(0.1)
                
                processed_count += 1
        except discord.Forbidden:
            continue

    if batch:
        await server_entries.bulk_write(batch)

    await guild_settings.update_one({"g": interaction.guild_id}, {"$set": {"i": True}}, upsert=True)
    await interaction.followup.send("Guild initialized!")
    
client.run(str(BOT_TOKEN), log_handler=handler)

#100 MB Free Discloud RAM, 512 MB storage MongoDB Free