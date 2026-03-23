import os
from typing import Any
import dotenv
import discord
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
dotenv.load_dotenv()
MONGODB_URI=os.getenv("MONGODBURI")
cluster=AsyncMongoClient(MONGODB_URI)
database:AsyncDatabase=cluster["ServerLeaderBoard"]
server_entries:AsyncCollection=database["ServerEntries"]
guild_settings:AsyncCollection=database["GuildSettings"]
active_sessions:AsyncCollection=database["ActiveSessions"]
user_settings:AsyncCollection=database["UserSettings"]

class Server_LeaderBoard_Entry:
    
    @staticmethod
    async def update_roles(guild_id:int,user_id:int,current_roles:list[int]):
        await server_entries.update_one({"g":guild_id,"u":user_id},{"$set":{"r":current_roles}},upsert=True)

    @staticmethod
    async def increment_text(guild_id:int,user_id:int,value:int=1):
        await server_entries.update_one({"g":guild_id,"u":user_id},{"$inc":{"m":value},"$set":{"t":discord.utils.utcnow()}},upsert=True)
    
    @staticmethod
    async def increment_special_channel(guild_id:int,user_id:int,value:int=1):
        await server_entries.update_one({"g":guild_id,"u":user_id},{"$inc":{"s":value},"$set":{"t":discord.utils.utcnow()}},upsert=True)
    @staticmethod
    async def add_voice_hour(guild_id:int,user_id:int,value:float):
        await server_entries.update_one({"g":guild_id,"u":user_id},{"$inc":{"v":value},"$set":{"t":discord.utils.utcnow()}},upsert=True)
    
    @staticmethod
    async def get_top_3_text(guild_id:int,role_id:list[int]|None=None,all_roles:bool=False)->list[dict]:
        match_filter:dict[str,Any]={"g": guild_id, "m": {"$gt": 0}}
        if role_id:
            if all_roles:
                match_filter["r"]={"$all":role_id}
            else: 
                match_filter["r"]={"$in":role_id}
        pipeline = [
            {"$match": match_filter}, # Filter server
            {"$sort": {"m": -1}},         # Sort by messages
            {"$limit":3},
            {"$project":{"u":1,"_id":0}}
        ]
        return await (await server_entries.aggregate(pipeline)).to_list()

    @staticmethod
    async def get_top_3_voice(guild_id:int,role_id:list[int]|None=None,all_roles:bool=False)->list[dict]:
        match_filter:dict[str,Any]={"g": guild_id, "v": {"$gt": 0}}
        if role_id:
            if all_roles:
                match_filter["r"]={"$all":role_id}
            else: 
                match_filter["r"]={"$in":role_id}
        pipeline = [
            {"$match": match_filter}, # Filter server
            {"$sort": {"v": -1}},     # Sort by messages
            {"$limit":3},             #Get top 3
            {"$project":{"u":1,"_id":0}} #Only give user
        ]
        return await (await server_entries.aggregate(pipeline)).to_list()
    
    @staticmethod
    async def get_top_3_special_channel(guild_id:int,role_id:list[int]|None=None,all_roles:bool=False)->list[dict]:
        match_filter:dict[str,Any]={"g": guild_id, "s": {"$gt": 0}}
        if role_id:
            if all_roles:
                match_filter["r"]={"$all":role_id}
            else: 
                match_filter["r"]={"$in":role_id}    
        pipeline = [
            {"$match": match_filter}, # Filter server
            {"$sort": {"s": -1}},     # Sort by messages
            {"$limit":3},             #Get top 3
            {"$project":{"u":1,"_id":0}} #Only give user
        ]
        return await (await server_entries.aggregate(pipeline)).to_list()
    @staticmethod
    async def get_roles_filter(guild_id:int)->dict|None:
        settings = await guild_settings.find_one({"g": guild_id},projection={"f": 1, "_id": 0})
        return settings.get("f") if settings else None
    
    @staticmethod
    async def get_current_timezone_offset(user_id:int)->float|None:
        settings= await user_settings.find_one({"u":user_id})
        return settings.get("tz") if settings else None
    @staticmethod
    async def get_settings(guild_id:int)->dict|None:
        return await guild_settings.find_one({"g":guild_id})
#Each server entries document is {"guild_id":g,"user_id":u,"messages_count":m,"voice_time_hours":v,"roles":r,"special_channel_count":s,"last_updated":t}
#Each guild settings document is {"guild_id":g,"initialized":i,"post_channel":p,"post_day":d,"post_hour":h,"post_minute":m, "filter":{"roles":r,"all":a},"special_channel_id":s,"special_text":t}
#Each activity log is {"guild_id":g,"user_id":u,"start":s}
#Each user setting is {"user_id":u,"timezone_offset":tz}
