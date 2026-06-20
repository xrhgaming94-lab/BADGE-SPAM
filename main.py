# [file name]: main.py
# FIXED BY : TG : @STAR_RDP
# TELEGRAM CHANNEL : @STAR_METHODE
import requests , os , psutil , sys , jwt , pickle , json , binascii , time , urllib3 , base64 , datetime , re , socket , threading , ssl , pytz , aiohttp , random
from flask import Flask, request, jsonify
from protobuf_decoder.protobuf_decoder import Parser
from xC4 import * ; from xHeaders import *
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from Pb2 import DEcwHisPErMsG_pb2 , MajoRLoGinrEs_pb2 , PorTs_pb2 , MajoRLoGinrEq_pb2 , sQ_pb2 , Team_msg_pb2
from cfonts import render, say
from queue import Queue

# Add AES imports for encryption
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import asyncio

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  

app = Flask(__name__)

# ============ GLOBAL VARIABLES FOR DUPLICATE PREVENTION ============
active_squads = {}  # team_code -> bool (to track active squads)
squad_bot_count = {}  # team_code -> count of bots in squad

# ============ MULTI BOT SYSTEM WITH ROTATION ============
class MultiBotLoader:
    def __init__(self):
        self.bot_files = ['bots.json'] + [f'bots{i}.json' for i in range(2, 11)]
        self.current_file_index = 0
        self.all_bots = {}
        self.file_bot_map = {}
        self.bot_status = {}
        self.command_queues = {}
        self.bot_keys = {}
        self.bot_writers = {}
        self.bot_tasks = {}
        self.bot_insquad = {}  # account_id -> squad_code
        self.bot_joined_teams = {}  # account_id -> set of team_codes joined
        
    def load_all_bots(self):
        self.all_bots = {}
        self.file_bot_map = {}
        
        for file_name in self.bot_files:
            try:
                with open(file_name, 'r') as f:
                    config = json.load(f)
                
                if isinstance(config, list):
                    for bot in config:
                        account_id = str(bot.get('account_id'))
                        self.all_bots[account_id] = {
                            'account_id': account_id,
                            'uid': str(bot.get('uid')),
                            'password': bot.get('password'),
                            'name': bot.get('name', f'Bot-{account_id}'),
                            'region': bot.get('region', 'auto'),
                            'enabled': bot.get('enabled', True),
                            'file': file_name
                        }
                        self.file_bot_map[account_id] = file_name
                        self.bot_status[account_id] = {'status': 'idle', 'message': 'Loaded'}
                        self.command_queues[account_id] = Queue()
                        self.bot_insquad[account_id] = None
                        self.bot_joined_teams[account_id] = set()
            
            except FileNotFoundError:
                print(f"⚠️ {file_name} not found, skipping...")
            except Exception as e:
                print(f"❌ Error loading {file_name}: {e}")
        
        print(f"✅ Loaded {len(self.all_bots)} bots from {len(set(self.file_bot_map.values()))} files")
        return len(self.all_bots) > 0
    
    def get_enabled_bots(self):
        current_file = self.bot_files[self.current_file_index]
        return {
            aid: data for aid, data in self.all_bots.items() 
            if data.get('enabled', True) and self.file_bot_map.get(aid) == current_file
        }
    
    def get_all_enabled_bots(self):
        return {aid: data for aid, data in self.all_bots.items() if data.get('enabled', True)}
    
    def rotate_file(self):
        old_file = self.bot_files[self.current_file_index]
        self.current_file_index = (self.current_file_index + 1) % len(self.bot_files)
        new_file = self.bot_files[self.current_file_index]
        print(f"🔄 Rotated from {old_file} to {new_file}")
        return new_file
    
    def get_bots_for_request(self):
        return self.get_enabled_bots()
    
    def get_all_status(self):
        return {
            aid: {
                'name': data.get('name', f'Bot-{aid}'),
                'account_id': data.get('account_id'),
                'uid': data.get('uid'),
                'region': data.get('region', 'auto'),
                'file': self.file_bot_map.get(aid, 'unknown'),
                'status': self.bot_status.get(aid, {}).get('status', 'idle'),
                'message': self.bot_status.get(aid, {}).get('message', ''),
                'queue_size': self.command_queues.get(aid, Queue()).qsize() if aid in self.command_queues else 0,
                'in_squad': self.bot_insquad.get(aid) is not None
            }
            for aid, data in self.all_bots.items()
        }
    
    def get_bot_writers(self, account_id):
        return self.bot_writers.get(account_id, {})
    
    def set_bot_writers(self, account_id, online_writer=None, whisper_writer=None):
        if account_id not in self.bot_writers:
            self.bot_writers[account_id] = {}
        if online_writer is not None:
            self.bot_writers[account_id]['online_writer'] = online_writer
        if whisper_writer is not None:
            self.bot_writers[account_id]['whisper_writer'] = whisper_writer
    
    def get_command_queue(self, account_id):
        if account_id not in self.command_queues:
            self.command_queues[account_id] = Queue()
        return self.command_queues[account_id]
    
    def update_status(self, account_id, status, message=""):
        self.bot_status[account_id] = {
            'status': status,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_bot_keys(self, account_id):
        return self.bot_keys.get(account_id, {})
    
    def set_bot_keys(self, account_id, key, iv, region):
        self.bot_keys[account_id] = {'key': key, 'iv': iv, 'region': region}
    
    def get_bot_tasks(self, account_id):
        return self.bot_tasks.get(account_id, [])
    
    def set_bot_tasks(self, account_id, tasks):
        self.bot_tasks[account_id] = tasks
    
    def set_insquad(self, account_id, squad_code):
        self.bot_insquad[account_id] = squad_code
    
    def get_insquad(self, account_id):
        return self.bot_insquad.get(account_id)
    
    def add_joined_team(self, account_id, team_code):
        if account_id not in self.bot_joined_teams:
            self.bot_joined_teams[account_id] = set()
        self.bot_joined_teams[account_id].add(team_code)
    
    def remove_joined_team(self, account_id, team_code):
        if account_id in self.bot_joined_teams:
            self.bot_joined_teams[account_id].discard(team_code)
    
    def has_joined_team(self, account_id, team_code):
        if account_id not in self.bot_joined_teams:
            return False
        return team_code in self.bot_joined_teams[account_id]

bot_manager = MultiBotLoader()

Hr = {
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 13; SHADOW_X Build/TP1A.220624.014)",
    'Connection': "Keep-Alive",
    'Accept-Encoding': "gzip",
    'Content-Type': "application/x-www-form-urlencoded",
    'X-Unity-Version': "2018.4.11f1",
    'X-GA': "v1 1",
    'ReleaseVersion': "OB53"
}

def get_random_color():
    colors = [
        "[FF0000]", "[00FF00]", "[0000FF]", "[FFFF00]", "[FF00FF]", "[00FFFF]", "[FFFFFF]", "[FFA500]",
        "[A52A2A]", "[800080]", "[000000]", "[808080]", "[C0C0C0]", "[FFC0CB]", "[FFD700]", "[ADD8E6]"
    ]
    return random.choice(colors)

BADGE_VALUES = {
    "s1": 1048576,
    "s2": 32768,
    "s3": 2048,
    "s4": 64,
    "s5": 262144
}

# ============ FAST JOIN/LEAVE FUNCTIONS ============

async def FastJoinSquad(team_code, key, iv):
    """Fast squad join with minimal delay"""
    try:
        join_packet = await GenJoinSquadsPacket(team_code, key, iv)
        return join_packet
    except:
        return None

async def FastLeaveSquad(key, iv):
    """Fast squad leave"""
    try:
        leave_packet = await ExiT(None, key, iv)
        return leave_packet
    except:
        return None

async def FastEmote(uid, emote_id, key, iv, region):
    """Fast emote with proper payload"""
    try:
        fields = {
            1: 21,
            2: {
                1: 804266360,
                2: 909000001,
                5: {
                    1: int(uid),
                    3: int(emote_id),
                }
            }
        }
        packet_hex = (await CrEaTe_ProTo(fields)).hex()
        if region.lower() == "ind":
            packet_type = '0514'
        elif region.lower() == "bd":
            packet_type = "0519"
        else:
            packet_type = "0515"
        return await GeneRaTePk(packet_hex, packet_type, key, iv)
    except:
        return None

# ============ BOT FUNCTIONS ============

async def encrypted_proto(encoded_hex):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded_message = pad(encoded_hex, AES.block_size)
    encrypted_payload = cipher.encrypt(padded_message)
    return encrypted_payload
    
async def GeNeRaTeAccEss(uid , password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    headers = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 13; SHADOW_X Build/TP1A.220624.014)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"}
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data) as response:
            if response.status != 200: return "Failed to get access token"
            data = await response.json()
            open_id = data.get("open_id")
            access_token = data.get("access_token")
            return (open_id, access_token) if open_id and access_token else (None, None)

async def EncRypTMajoRLoGin(open_id, access_token):
    major_login = MajoRLoGinrEq_pb2.MajorLogin()
    major_login.event_time = str(datetime.now())[:-7]
    major_login.game_name = "free fire"
    major_login.platform_id = 1
    major_login.client_version = "1.123.1"
    major_login.system_software = "Android OS 9 / API-28 (PQ3B.190801.10101846/G9650ZHU2ARC6)"
    major_login.system_hardware = "Handheld"
    major_login.telecom_operator = "Verizon"
    major_login.network_type = "WIFI"
    major_login.screen_width = 1920
    major_login.screen_height = 1080
    major_login.screen_dpi = "280"
    major_login.processor_details = "ARM64 FP ASIMD AES VMH | 2865 | 4"
    major_login.memory = 3003
    major_login.gpu_renderer = "Adreno (TM) 640"
    major_login.gpu_version = "OpenGL ES 3.1 v1.46"
    major_login.unique_device_id = f"Google|{random.getrandbits(128):032x}"
    major_login.client_ip = "223.191.51.89"
    major_login.language = "en"
    major_login.open_id = open_id
    major_login.open_id_type = "4"
    major_login.device_type = "Handheld"
    memory_available = major_login.memory_available
    memory_available.version = 55
    memory_available.hidden_value = 81
    major_login.access_token = access_token
    major_login.platform_sdk_id = 1
    major_login.network_operator_a = "Verizon"
    major_login.network_type_a = "WIFI"
    major_login.client_using_version = "7428b253defc164018c604a1ebbfebdf"
    major_login.external_storage_total = 36235
    major_login.external_storage_available = 31335
    major_login.internal_storage_total = 2519
    major_login.internal_storage_available = 703
    major_login.game_disk_storage_available = 25010
    major_login.game_disk_storage_total = 26628
    major_login.external_sdcard_avail_storage = 32992
    major_login.external_sdcard_total_storage = 36235
    major_login.login_by = 3
    major_login.library_path = "/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/lib/arm64"
    major_login.reg_avatar = 1
    major_login.library_token = "5b892aaabd688e571f688053118a162b|/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/base.apk"
    major_login.channel_type = 3
    major_login.cpu_type = 2
    major_login.cpu_architecture = "64"
    major_login.client_version_code = "312e3132302e31"
    major_login.graphics_api = "OpenGLES2"
    major_login.supported_astc_bitset = 16383
    major_login.login_open_id_type = 4
    major_login.analytics_detail = b"FwQVTgUPX1UaUllDDwcWCRBpWA0FUgsvA1snWlBaO1kFYg=="
    major_login.loading_time = 13564
    major_login.release_channel = "android"
    major_login.extra_info = "KqsHTymw5/5GB23YGniUYN2/q47GATrq7eFeRatf0NkwLKEMQ0PK5BKEk72dPflAxUlEBir6Vtey83XqF593qsl8hwY="
    major_login.android_engine_init_flag = 110009
    major_login.if_push = 1
    major_login.is_vpn = 1
    major_login.origin_platform_type = "4"
    major_login.primary_platform_type = "4"
    string = major_login.SerializeToString()
    return await encrypted_proto(string)

async def MajorLogin(payload):
    url = "https://loginbp.ggblueshark.com/MajorLogin"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=Hr, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None

async def GetLoginData(base_url, payload, token):
    url = f"{base_url}/GetLoginData"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    headers = Hr.copy()
    headers['Authorization'] = f"Bearer {token}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=headers, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None

async def DecRypTMajoRLoGin(MajoRLoGinResPonsE):
    proto = MajoRLoGinrEs_pb2.MajorLoginRes()
    proto.ParseFromString(MajoRLoGinResPonsE)
    return proto

async def DecRypTLoGinDaTa(LoGinDaTa):
    proto = PorTs_pb2.GetLoginData()
    proto.ParseFromString(LoGinDaTa)
    return proto

async def DecodeWhisperMessage(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = DEcwHisPErMsG_pb2.DecodeWhisper()
    proto.ParseFromString(packet)
    return proto
    
async def decode_team_packet(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = sQ_pb2.recieved_chat()
    proto.ParseFromString(packet)
    return proto
    
async def xAuThSTarTuP(TarGeT, token, timestamp, key, iv):
    uid_hex = hex(TarGeT)[2:]
    uid_length = len(uid_hex)
    encrypted_timestamp = await DecodE_HeX(timestamp)
    encrypted_account_token = token.encode().hex()
    encrypted_packet = await EnC_PacKeT(encrypted_account_token, key, iv)
    encrypted_packet_length = hex(len(encrypted_packet) // 2)[2:]
    if uid_length == 9: headers = '0000000'
    elif uid_length == 8: headers = '00000000'
    elif uid_length == 10: headers = '000000'
    elif uid_length == 7: headers = '000000000'
    else: headers = '0000000'
    return f"0115{headers}{uid_hex}{encrypted_timestamp}00000{encrypted_packet_length}{encrypted_packet}"
     
async def cHTypE(H):
    if not H: return 'Squid'
    elif H == 1: return 'CLan'
    elif H == 2: return 'PrivaTe'
    
async def SEndMsG(H , message , Uid , chat_id , key , iv):
    TypE = await cHTypE(H)
    if TypE == 'Squid': msg_packet = await xSEndMsQsQ(message , chat_id , key , iv)
    elif TypE == 'CLan': msg_packet = await xSEndMsg(message , 1 , chat_id , chat_id , key , iv)
    elif TypE == 'PrivaTe': msg_packet = await xSEndMsg(message , 2 , Uid , Uid , key , iv)
    return msg_packet

async def SEndPacKeT(whisper_writer, online_writer, TypE, PacKeT):
    if TypE == 'ChaT' and whisper_writer:
        whisper_writer.write(PacKeT) 
        await whisper_writer.drain()
    elif TypE == 'OnLine' and online_writer:
        online_writer.write(PacKeT) 
        await online_writer.drain()
    else: 
        return 'Unsupported Type!' 
           
async def TcPOnLine(account_id, ip, port, key, iv, AutHToKen, region, reconnect_delay=0.5):
    global bot_manager, active_squads, squad_bot_count
    bot_uid = int(account_id)
    
    while True:
        try:
            reader , writer = await asyncio.open_connection(ip, int(port))
            bot_manager.set_bot_writers(account_id, online_writer=writer)
            
            bytes_payload = bytes.fromhex(AutHToKen)
            writer.write(bytes_payload)
            await writer.drain()
            
            while True:
                data2 = await reader.read(9999)
                if not data2: break
                
                data_hex = data2.hex()
                
                # ============ SQUAD JOIN DETECTION ============
                if data_hex.startswith("0500") and bot_manager.get_insquad(account_id) is None:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
                        
                        squad_owner = packet_json.get('5', {}).get('data', {}).get('1', {}).get('data')
                        code = packet_json.get('5', {}).get('data', {}).get('8', {}).get('data')
                        uid = packet_json.get('5', {}).get('data', {}).get('2', {}).get('data', {}).get('1', {}).get('data')
                        
                        if not squad_owner or not code:
                            continue
                        
                        # CHECK DUPLICATE - skip if bot already in this squad
                        if bot_manager.has_joined_team(account_id, code):
                            print(f"⚠️ Bot {account_id} already in squad {code}, skipping")
                            continue
                        
                        # Mark squad as active for this bot
                        squad_key = f"{code}_{squad_owner}"
                        active_squads[squad_key] = True
                        bot_manager.set_insquad(account_id, code)
                        bot_manager.add_joined_team(account_id, code)
                        
                        writers = bot_manager.get_bot_writers(account_id)
                        whisper_writer = writers.get('whisper_writer')
                        online_writer = writers.get('online_writer')
                        
                        # Send invite and join
                        SendInv = await RedZed_SendInv(int(account_id), uid, key, iv)
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', SendInv)
                        
                        inv_packet = await RejectMSGtaxt(squad_owner, uid, key, iv)
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', inv_packet)
                        
                        Join = await ArohiAccepted(squad_owner, code, key, iv)
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', Join)
                        
                        await asyncio.sleep(0.5)
                        
                        # Send emote
                        emote_id = 909050008
                        emote_to_sender = await FastEmote(int(uid), emote_id, key, iv, region)
                        if emote_to_sender:
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_to_sender)
                        
                        bot_emote = await FastEmote(int(account_id), emote_id, key, iv, region)
                        if bot_emote:
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', bot_emote)
                        
                        # Welcome message
                        try:
                            welcome_message = f"""[B][C][FFFFFF]Welcome to STAR Bot

[B][C][FFFFFF]Available Features:
[B][C][FFFFFF]• Dance & Emote Commands
[B][C][FFFFFF]• Team & Squad Management
[B][C][FFFFFF]• Player Info & Status 
[B][C][FFFFFF]• Smart Spam Protection
[B][C][FFFFFF]• AI Chat Assistance

[B][C][FFFFFF]Use /help to see all commands

[B][C][FFFFFF]Telegram : @STAR_METHODE
[B][C][FFFFFF]Contact : @STAR_RDP

[B][C][FFFFFF]Status: Online & Active"""
                            P = await SEndMsG(0, welcome_message, squad_owner, squad_owner, key, iv)
                            if whisper_writer:
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                        except:
                            pass
                            
                    except Exception as e:
                        continue
                
                # ============ SQUAD LEAVE DETECTION ============
                if data_hex.startswith('0500') and bot_manager.get_insquad(account_id) is not None:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
                        if packet_json.get('1') in [6, 7]:
                            current_squad = bot_manager.get_insquad(account_id)
                            if current_squad:
                                bot_manager.remove_joined_team(account_id, current_squad)
                            bot_manager.set_insquad(account_id, None)
                            continue
                    except:
                        pass
                
                if data_hex.startswith('0500') and len(data_hex) > 1000:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet = json.loads(packet)
                        OwNer_UiD , CHaT_CoDe , _ = await GeTSQDaTa(packet)
                        
                        writers = bot_manager.get_bot_writers(account_id)
                        whisper_writer = writers.get('whisper_writer')
                        online_writer = writers.get('online_writer')
                        
                        JoinCHaT = await AutH_Chat(3 , OwNer_UiD , CHaT_CoDe, key, iv)
                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', JoinCHaT)
                        
                        message = f'[B][C]{get_random_color()}\n- WeLComE To Emote Bot ! '
                        P = await SEndMsG(0 , message , OwNer_UiD , OwNer_UiD , key , iv)
                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                    except:
                        pass

            writer.close() ; await writer.wait_closed()
            bot_manager.set_bot_writers(account_id, online_writer=None)
            bot_manager.set_insquad(account_id, None)

        except Exception as e: 
            print(f"Bot {account_id} Online Error: {e}")
            bot_manager.update_status(account_id, 'error', str(e))
        await asyncio.sleep(reconnect_delay)
                            
async def TcPChaT(account_id, ip, port, AutHToKen, key, iv, LoGinDaTaUncRypTinG, ready_event, region, reconnect_delay=0.5):
    global bot_manager, active_squads
    
    while True:
        try:
            reader , writer = await asyncio.open_connection(ip, int(port))
            bot_manager.set_bot_writers(account_id, whisper_writer=writer)
            
            bytes_payload = bytes.fromhex(AutHToKen)
            writer.write(bytes_payload)
            await writer.drain()
            ready_event.set()
            
            if LoGinDaTaUncRypTinG.Clan_ID:
                clan_id = LoGinDaTaUncRypTinG.Clan_ID
                clan_compiled_data = LoGinDaTaUncRypTinG.Clan_Compiled_Data
                pK = await AuthClan(clan_id , clan_compiled_data , key , iv)
                if writer: 
                    writer.write(pK) 
                    await writer.drain()
            
            while True:
                data = await reader.read(9999)
                if not data: break
                
                if data.hex().startswith("120000"):
                    try:
                        response = await DecodeWhisperMessage(data.hex()[10:])
                        if response:
                            inPuTMsG = response.Data.msg.lower()
                            
                            writers = bot_manager.get_bot_writers(account_id)
                            whisper_writer = writers.get('whisper_writer')
                            online_writer = writers.get('online_writer')
                            
                            # ============ /join - FAST JOIN ============
                            if inPuTMsG.startswith("/join"):
                                parts = inPuTMsG.strip().split()
                                if len(parts) < 2:
                                    error_msg = f"[B][C][FF0000]❌ Usage: /join (team_code)\n"
                                    P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    CodE = parts[1]
                                    sender_uid = response.Data.uid
                                    
                                    # CHECK DUPLICATE
                                    if bot_manager.has_joined_team(account_id, CodE):
                                        error_msg = f"[B][C][FF0000]❌ Bot already in squad {CodE}!\nUse /exit first."
                                        P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                        continue
                                    
                                    initial_msg = f"[C][B][00FF00]🤖 Joining Team: [00FFFF]{CodE}\n"
                                    P = await SEndMsG(0, initial_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    
                                    try:
                                        # FAST JOIN
                                        join_packet = await FastJoinSquad(CodE, key, iv)
                                        if join_packet:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
                                        
                                        bot_manager.add_joined_team(account_id, CodE)
                                        bot_manager.set_insquad(account_id, CodE)
                                        
                                        await asyncio.sleep(0.3)
                                        
                                        # FAST EMOTE
                                        emote_id = 909050008
                                        emote_to_sender = await FastEmote(int(sender_uid), emote_id, key, iv, region)
                                        if emote_to_sender:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_to_sender)
                                        
                                        bot_emote = await FastEmote(int(account_id), emote_id, key, iv, region)
                                        if bot_emote:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', bot_emote)
                                        
                                        await asyncio.sleep(0.3)
                                        
                                        success_msg = f"[B][C][00FF00]✅ Joined: {CodE}!\n"
                                        P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    except Exception as e:
                                        error_msg = f"[B][C][FF0000]❌ Error: {str(e)}\n"
                                        P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /exit - FAST LEAVE ============
                            elif inPuTMsG.startswith("/exit"):
                                initial_msg = f"[B][C]{get_random_color()}\nLeaving squad...\n"
                                P = await SEndMsG(0, initial_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                
                                # FAST LEAVE
                                leave = await FastLeaveSquad(key, iv)
                                if leave:
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave)
                                
                                current_squad = bot_manager.get_insquad(account_id)
                                if current_squad:
                                    bot_manager.remove_joined_team(account_id, current_squad)
                                bot_manager.set_insquad(account_id, None)
                                
                                success_msg = f"[C][B][00FF00]✅ Left squad!\n"
                                P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /3 - FAST ============
                            elif inPuTMsG.startswith("/3"):
                                PAc = await OpEnSq(key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                await asyncio.sleep(0.1)
                                
                                C = await cHSq(3, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                await asyncio.sleep(0.1)
                                
                                V = await SEnd_InV(3, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                await asyncio.sleep(0.1)
                                
                                E = await FastLeaveSquad(key, iv)
                                if E:
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                                
                                success_msg = f"[B][C][00FF00]✅ 3-Player invite sent!\n"
                                P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /5 - FAST ============
                            elif inPuTMsG.startswith("/5"):
                                PAc = await OpEnSq(key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                await asyncio.sleep(0.1)
                                
                                C = await cHSq(5, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                await asyncio.sleep(0.1)
                                
                                V = await SEnd_InV(5, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                await asyncio.sleep(0.1)
                                
                                E = await FastLeaveSquad(key, iv)
                                if E:
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                                
                                success_msg = f"[B][C][00FF00]✅ 5-Player invite sent!\n"
                                P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /6 - FAST ============
                            elif inPuTMsG.startswith("/6"):
                                PAc = await OpEnSq(key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                await asyncio.sleep(0.1)
                                
                                C = await cHSq(6, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                await asyncio.sleep(0.1)
                                
                                V = await SEnd_InV(6, response.Data.uid, key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                await asyncio.sleep(0.1)
                                
                                E = await FastLeaveSquad(key, iv)
                                if E:
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                                
                                success_msg = f"[B][C][00FF00]✅ 6-Player invite sent!\n"
                                P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /inv - FAST ============
                            elif inPuTMsG.startswith("/inv"):
                                parts = inPuTMsG.strip().split()
                                if len(parts) < 2:
                                    error_msg = f"[B][C][FF0000]❌ Usage: /inv (uid)\n"
                                    P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    target_uid = parts[1]
                                    try:
                                        PAc = await OpEnSq(key, iv, region)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                        await asyncio.sleep(0.1)
                                        
                                        C = await cHSq(5, int(target_uid), key, iv, region)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                        await asyncio.sleep(0.1)
                                        
                                        V = await SEnd_InV(5, int(target_uid), key, iv, region)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                        await asyncio.sleep(0.1)
                                        
                                        E = await FastLeaveSquad(key, iv)
                                        if E:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                                        
                                        success_msg = f"[B][C][00FF00]✅ Invite sent to {target_uid}!\n"
                                        P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    except Exception as e:
                                        error_msg = f"[B][C][FF0000]❌ Error: {str(e)}\n"
                                        P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                            # ============ /e - SINGLE EMOTE ============
                            elif inPuTMsG.startswith("/e"):
                                parts = inPuTMsG.strip().split()
                                if len(parts) < 3:
                                    error_msg = f"[B][C][FF0000]❌ Usage: /e (uid) (emote_id)\n"
                                    P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    try:
                                        target_uid = int(parts[1])
                                        emote_id = int(parts[2])
                                        
                                        H = await FastEmote(target_uid, emote_id, key, iv, region)
                                        if H:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                                        
                                        success_msg = f"[B][C][00FF00]✅ Emote {emote_id} sent to {target_uid}!\n"
                                        P = await SEndMsG(0, success_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    except:
                                        error_msg = f"[B][C][FF0000]❌ Invalid UID/Emote ID!\n"
                                        P = await SEndMsG(0, error_msg, response.Data.uid, response.Data.Chat_ID, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            
                    except:
                        pass

            writer.close() ; await writer.wait_closed()
            bot_manager.set_bot_writers(account_id, whisper_writer=None)

        except Exception as e: 
            print(f"Bot {account_id} Chat Error: {e}")
            bot_manager.update_status(account_id, 'error', str(e))
        await asyncio.sleep(reconnect_delay)

# ============ API COMMAND PROCESSOR ============
async def process_api_commands(account_id, key, iv, region):
    global bot_manager, active_squads
    
    queue = bot_manager.get_command_queue(account_id)
    
    while True:
        try:
            if queue.empty():
                await asyncio.sleep(0.2)
                continue

            writers = bot_manager.get_bot_writers(account_id)
            online_writer = writers.get('online_writer')
            whisper_writer = writers.get('whisper_writer')
            
            if online_writer is None:
                await asyncio.sleep(1)
                continue

            cmd = queue.get()
            
            if cmd.get('command') == 'squad':
                players = cmd["type"]
                target_uid = int(cmd["uid"])
                
                P = await OpEnSq(key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', P)
                await asyncio.sleep(0.1)
                
                A = await cHSq(players, target_uid, key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', A)
                await asyncio.sleep(0.1)
                
                C = await SEnd_InV(players, target_uid, key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                await asyncio.sleep(0.1)
                
                K = await FastLeaveSquad(key, iv)
                if K:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', K)
            
            elif cmd.get('command') == 'join':
                team_code = cmd.get('code')
                uids = cmd.get('uids', [])
                emote_id = cmd.get('emote_id')
                bot_uid = cmd.get('bot_uid')
                
                # CHECK DUPLICATE
                if bot_manager.has_joined_team(account_id, team_code):
                    print(f"⚠️ Bot {account_id} already in squad {team_code}")
                    continue
                
                # FAST JOIN
                join_packet = await FastJoinSquad(team_code, key, iv)
                if join_packet:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
                
                bot_manager.add_joined_team(account_id, team_code)
                bot_manager.set_insquad(account_id, team_code)
                await asyncio.sleep(0.1)
                
                # FAST EMOTES
                for uid_str in uids:
                    uid = int(uid_str)
                    H = await FastEmote(uid, emote_id, key, iv, region)
                    if H:
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                    await asyncio.sleep(0.05)
                
                # FAST LEAVE
                LV = await FastLeaveSquad(key, iv)
                if LV:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', LV)
                await asyncio.sleep(0.03)
                
                bot_manager.remove_joined_team(account_id, team_code)
                bot_manager.set_insquad(account_id, None)

            elif cmd.get('command') == 'spam_invi':
                target_uid = int(cmd["uid"])
                total_invites = 57
                batch_size = 7
                
                for count in range(total_invites):
                    PAc = await OpEnSq(key, iv, region)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                    await asyncio.sleep(0.05)
                    
                    C = await cHSq(5, target_uid, key, iv, region)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                    await asyncio.sleep(0.05)
                    
                    V = await SEnd_InV(5, target_uid, key, iv, region)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                    await asyncio.sleep(0.05)
                    
                    E = await FastLeaveSquad(key, iv)
                    if E:
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                    await asyncio.sleep(0.05)
                    
                    if (count + 1) % batch_size == 0:
                        await asyncio.sleep(10)
            
            elif cmd.get('command') == 'badge':
                target_uid = int(cmd["uid"])
                badge_value = cmd["badge_value"]
                
                LV = await FastLeaveSquad(key, iv)
                if LV:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', LV)
                await asyncio.sleep(0.1)
                
                join_packet = await request_join_with_badge(target_uid, badge_value, key, iv, region)
                for _ in range(5):
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
                    await asyncio.sleep(0.05)
                    
            elif cmd.get('command') == 'spam':
                target_uid = int(cmd["uid"])
                badge_sequence = list(BADGE_VALUES.values())
                
                LV = await FastLeaveSquad(key, iv)
                if LV:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', LV)
                await asyncio.sleep(0.1)
                
                for i in range(99):
                    current_badge = badge_sequence[i % len(badge_sequence)]
                    join_packet = await request_join_with_badge(target_uid, current_badge, key, iv, region)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
                    await asyncio.sleep(0.05)

        except Exception as e:
            print(f"API ERROR for {account_id}: {e}")
            await asyncio.sleep(1)

# ============ START SINGLE BOT ============
async def start_single_bot(account_id, bot_data):
    global bot_manager
    
    try:
        uid = bot_data.get('uid')
        password = bot_data.get('password')
        region = bot_data.get('region', 'auto')
        
        print(f"🚀 Starting {bot_data.get('name')} ({account_id}) from {bot_data.get('file', 'unknown')}")
        bot_manager.update_status(account_id, 'connecting', 'Getting access token...')
        
        open_id, access_token = await GeNeRaTeAccEss(uid, password)
        if not open_id or not access_token:
            bot_manager.update_status(account_id, 'error', 'Invalid credentials')
            return None
        
        PyL = await EncRypTMajoRLoGin(open_id, access_token)
        MajoRLoGinResPonsE = await MajorLogin(PyL)
        if not MajoRLoGinResPonsE:
            bot_manager.update_status(account_id, 'error', 'Account banned')
            return None
        
        MajoRLoGinauTh = await DecRypTMajoRLoGin(MajoRLoGinResPonsE)
        UrL = MajoRLoGinauTh.url
        region = MajoRLoGinauTh.region if region == 'auto' else region
        
        ToKen = MajoRLoGinauTh.token
        TarGeT = MajoRLoGinauTh.account_uid
        key = MajoRLoGinauTh.key
        iv = MajoRLoGinauTh.iv
        timestamp = MajoRLoGinauTh.timestamp
        
        bot_manager.set_bot_keys(account_id, key, iv, region)
        
        LoGinDaTa = await GetLoginData(UrL, PyL, ToKen)
        if not LoGinDaTa:
            bot_manager.update_status(account_id, 'error', 'Failed to get ports')
            return None
        
        LoGinDaTaUncRypTinG = await DecRypTLoGinDaTa(LoGinDaTa)
        OnLinePorTs = LoGinDaTaUncRypTinG.Online_IP_Port
        ChaTPorTs = LoGinDaTaUncRypTinG.AccountIP_Port
        
        OnLineiP, OnLineporT = OnLinePorTs.split(":")
        ChaTiP, ChaTporT = ChaTPorTs.split(":")
        
        acc_name = LoGinDaTaUncRypTinG.AccountName
        bot_manager.update_status(account_id, 'online', f'Online as {acc_name}')
        
        AutHToKen = await xAuThSTarTuP(int(TarGeT), ToKen, int(timestamp), key, iv)
        ready_event = asyncio.Event()
        
        chat_task = asyncio.create_task(
            TcPChaT(account_id, ChaTiP, ChaTporT, AutHToKen, key, iv,
                    LoGinDaTaUncRypTinG, ready_event, region)
        )
        
        await ready_event.wait()
        await asyncio.sleep(0.5)
        
        online_task = asyncio.create_task(
            TcPOnLine(account_id, OnLineiP, OnLineporT, key, iv, AutHToKen, region)
        )
        
        api_task = asyncio.create_task(
            process_api_commands(account_id, key, iv, region)
        )
        
        bot_manager.set_bot_tasks(account_id, [chat_task, online_task, api_task])
        
        print(f"✅ {acc_name} is online!")
        return chat_task, online_task, api_task
        
    except Exception as e:
        print(f"❌ Error for {account_id}: {e}")
        bot_manager.update_status(account_id, 'error', str(e))
        return None

async def start_all_bots():
    tasks = []
    enabled_bots = bot_manager.get_all_enabled_bots()
    
    if not enabled_bots:
        print("⚠ No enabled bots found in any file!")
        return
    
    print(f"\n🚀 Starting {len(enabled_bots)} bots from all files...\n")
    
    for account_id, bot_data in enabled_bots.items():
        result = await start_single_bot(account_id, bot_data)
        if result:
            tasks.extend(result)
        await asyncio.sleep(1.5)
    
    print(f"\n✅ All {len(enabled_bots)} bots started!")
    if tasks:
        await asyncio.gather(*tasks)

# ============ FLASK ROUTES ============

@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "service": "Free Fire Multi-Bot Emote System (10-File Rotation)",
        "total_bots": len(bot_manager.all_bots),
        "enabled_bots": len(bot_manager.get_all_enabled_bots()),
        "current_file": bot_manager.bot_files[bot_manager.current_file_index],
        "bot_files": bot_manager.bot_files,
        "endpoints": {
            "/spam_invi?uid=123": "Batch 57 Invites",
            "/s1?uid=123": "Send S1 Badge Invites",
            "/s2?uid=123": "Send S2 Badge Invites",
            "/s3?uid=123": "Send S3 Badge Invites",
            "/s4?uid=123": "Send S4 Badge Invites",
            "/s5?uid=123": "Send S5 Badge Invites",
            "/spam?uid=123": "Spam all Badges (99x)",
            "/bots": "List ALL bots with status",
            "/rotate": "Rotate to next bot file manually"
        }
    })

@app.route("/bots")
def list_bots():
    return jsonify({
        "total_bots": len(bot_manager.all_bots),
        "bot_files": bot_manager.bot_files,
        "current_file": bot_manager.bot_files[bot_manager.current_file_index],
        "bots": bot_manager.get_all_status()
    })

@app.route("/rotate")
def rotate_bots():
    old_file = bot_manager.bot_files[bot_manager.current_file_index]
    bot_manager.rotate_file()
    new_file = bot_manager.bot_files[bot_manager.current_file_index]
    
    return jsonify({
        "status": "success",
        "message": f"Rotated from {old_file} to {new_file}",
        "current_file": new_file,
        "available_files": bot_manager.bot_files
    })

@app.route("/stopbystar")
def squad3():
    uid = request.args.get("uid")
    if not uid: return jsonify({"error": "uid required"}), 400
    
    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No enabled bots in current file"}), 404
    
    for account_id in current_bots:
        queue = bot_manager.get_command_queue(account_id)
        queue.put({"command": "squad", "type": 3, "uid": uid})
        results[account_id] = "queued"
    
    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"3-player invite sent to {uid}", "total_bots": len(results)})

@app.route("/stopbystar")
def squad5():
    uid = request.args.get("uid")
    if not uid: return jsonify({"error": "uid required"}), 400
    
    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No enabled bots in current file"}), 404
    
    for account_id in current_bots:
        queue = bot_manager.get_command_queue(account_id)
        queue.put({"command": "squad", "type": 5, "uid": uid})
        results[account_id] = "queued"
    
    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"5-player invite sent to {uid}", "total_bots": len(results)})

@app.route("/stopbystar")
def squad6():
    uid = request.args.get("uid")
    if not uid: return jsonify({"error": "uid required"}), 400
    
    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No enabled bots in current file"}), 404
    
    for account_id in current_bots:
        queue = bot_manager.get_command_queue(account_id)
        queue.put({"command": "squad", "type": 6, "uid": uid})
        results[account_id] = "queued"
    
    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"6-player invite sent to {uid}", "total_bots": len(results)})

@app.route("/stopbystar")
def join_team():
    team_code = request.args.get('tc')
    uid1 = request.args.get('uid1')
    emote_id_str = request.args.get('emote_id')

    if not team_code or not emote_id_str:
        return jsonify({"status": "error", "message": "Missing tc or emote_id"}), 400

    try: emote_id = int(emote_id_str)
    except: return jsonify({"status": "error", "message": "emote_id must be integer"}), 400

    uids = [request.args.get(f'uid{i}') for i in range(1, 7) if request.args.get(f'uid{i}')]
    if not uids: return jsonify({"status": "error", "message": "Provide at least one UID"}), 400

    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No enabled bots in current file"}), 404
    
    for account_id, bot_data in current_bots.items():
        bot_uid = int(bot_data.get('account_id', 0))
        queue = bot_manager.get_command_queue(account_id)
        queue.put({
            "command": "join", "code": team_code, "uids": uids, 
            "emote_id": emote_id, "bot_uid": bot_uid
        })
        results[account_id] = "queued"

    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"Emote triggered from bots", "total_bots": len(results)})

@app.route("/spam_invi")
def api_spam_invi():
    uid = request.args.get("uid")
    if not uid: return jsonify({"error": "uid required"}), 400
    
    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No enabled bots in current file"}), 404
        
    for account_id in current_bots:
        queue = bot_manager.get_command_queue(account_id)
        queue.put({"command": "spam_invi", "uid": uid})
        results[account_id] = "queued"
        
    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"Spam Invites started against {uid}", "total_bots": len(results)})

def create_badge_route(route_name, badge_key):
    def route_func():
        uid = request.args.get("uid")
        if not uid: return jsonify({"error": "uid required"}), 400
        
        current_bots = bot_manager.get_bots_for_request()
        results = {}
        if not current_bots: return jsonify({"status": "error", "message": "No active bots"}), 404
            
        for account_id in current_bots:
            queue = bot_manager.get_command_queue(account_id)
            queue.put({
                "command": "badge", 
                "uid": uid, 
                "badge_value": BADGE_VALUES[badge_key]
            })
            results[account_id] = "queued"
            
        bot_manager.rotate_file()
        return jsonify({"status": "success", "message": f"{badge_key.upper()} Badge request sent to {uid}", "total_bots": len(results)})
    route_func.__name__ = f"api_{route_name}"
    return route_func

for badge in BADGE_VALUES.keys():
    app.route(f"/{badge}")(create_badge_route(badge, badge))

@app.route("/spam")
def api_spam():
    uid = request.args.get("uid")
    if not uid: return jsonify({"error": "uid required"}), 400
    
    current_bots = bot_manager.get_bots_for_request()
    results = {}
    if not current_bots: return jsonify({"status": "error", "message": "No active bots"}), 404
        
    for account_id in current_bots:
        queue = bot_manager.get_command_queue(account_id)
        queue.put({"command": "spam", "uid": uid})
        results[account_id] = "queued"
        
    bot_manager.rotate_file()
    return jsonify({"status": "success", "message": f"Full Badge SPAM started against {uid}", "total_bots": len(results)})

@app.route("/status")
def status():
    return jsonify({
        "total_bots": len(bot_manager.all_bots),
        "enabled_bots": len(bot_manager.get_all_enabled_bots()),
        "current_file": bot_manager.bot_files[bot_manager.current_file_index],
        "bot_files": bot_manager.bot_files,
        "bots": bot_manager.get_all_status()
    })

# ============ MAIN ============

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

async def MaiiiinE():
    global loop
    
    print("\n" + "="*50)
    print("🤖 FREE FIRE MULTI-BOT SYSTEM WITH ROTATION (10 FILES)")
    print("="*50)
    
    if not bot_manager.load_all_bots():
        print("❌ No bots found in any file! Please add bots and restart!")
        return None
    
    loop = asyncio.get_running_loop()
    await start_all_bots()

async def StarTinG():
    while True:
        try:
            await asyncio.wait_for(MaiiiinE(), timeout=7 * 60 * 60)
        except asyncio.TimeoutError:
            print("Token Expired! Restarting...")
        except Exception as e:
            print(f"Error - {e} => Restarting...")

if __name__ == '__main__':
    bot_files = ['bots.json'] + [f'bots{i}.json' for i in range(2, 11)]
    existing_files = [f for f in bot_files if os.path.exists(f)]
    
    if not existing_files:
        print("[!] No bot files found! Creating templates for all 10 files...")
        for idx, f in enumerate(bot_files):
            template = [
                {
                    "uid": f"522034437{idx}",
                    "password": f"SSTAR_BYSTARGMR_wRPs9tb{idx}",
                    "account_id": f"1607819954{idx}",
                    "name": f"STAR-EMT{idx}",
                    "region": "BD",
                    "enabled": True
                }
            ]
            with open(f, 'w') as file:
                json.dump(template, file, indent=2)
        print("[✓] Template files created! Please edit them with your credentials.")
        print("Files created: " + ", ".join(bot_files))
        sys.exit(1)
    
    print(f"📁 Found bot files: {', '.join(existing_files)}")
    
    bot_thread = threading.Thread(target=lambda: asyncio.run(StarTinG()), daemon=True)
    bot_thread.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
