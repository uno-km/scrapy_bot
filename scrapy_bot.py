import asyncio
import os
import glob
import json
import datetime
import shutil
from urllib.parse import urlparse
import yt_dlp
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =====================================================================
# [설정 로드 매커니즘]
# =====================================================================

def load_config():
    """config.json 파일에서 설정을 불러옵니다."""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError("config.json 파일을 찾을 수 없습니다. config.json.example을 참고하여 생성해주세요.")
    
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# 설정값 초기화
CONFIG = load_config()

BOT_TOKEN = CONFIG.get("BOT_TOKEN")
DOWNLOAD_ROOT = CONFIG.get("DOWNLOAD_ROOT", "download")
BOT_LOG_FILE = CONFIG.get("BOT_LOG_FILE", "bot_activity.log")
MAX_FILE_SIZE = CONFIG.get("MAX_FILE_SIZE_MB", 49.5) * 1024 * 1024
ALLOWED_USER_IDS = [int(uid) for uid in CONFIG.get("ALLOWED_USER_IDS", [])]
HELP_TEXT = CONFIG.get("HELP_TEXT", "도움말이 설정되지 않았습니다.")


queue = asyncio.Queue()
workers = []

def write_bot_log(event_type, details):
    """봇의 상세 활동 로그를 기록합니다."""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "event": event_type,
            "details": details
        }
        with open(BOT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as log_err:
        print(f"❌ 로그 기록 실패: {log_err}")

# =====================================================================
# [플랫폼 확장성 관리를 위한 Extractor]
# =====================================================================

class BaseExtractor:
    name = "Unknown"
    @classmethod
    def format_input(cls, text: str) -> str: return text
    @classmethod
    def is_match(cls, text: str) -> bool: return False
    @classmethod
    def is_profile(cls, url: str) -> bool: return False

class TikTokExtractor(BaseExtractor):
    name = "TikTok"
    @classmethod
    def format_input(cls, text: str) -> str:
        # tt: 접두사가 있으면 제거 후 처리
        clean_text = text[3:] if text.lower().startswith("tt:") else text
        if clean_text.startswith("@"): return f"https://www.tiktok.com/{clean_text}"
        return clean_text
    @classmethod
    def is_match(cls, text: str) -> bool:
        t = text.lower()
        return "tiktok.com" in t or t.startswith("tt:") or (t.startswith("@") and not t.startswith("ig:"))
    @classmethod
    def is_profile(cls, url: str) -> bool:
        # 단건 링크 키워드 제외
        single_markers = ["/video/", "/photo/", "/story/", "/v/"]
        return not any(m in url for m in single_markers) and "@" in url

class InstagramExtractor(BaseExtractor):
    name = "Instagram"
    @classmethod
    def format_input(cls, text: str) -> str:
        # ig: 접두사가 있으면 제거 후 처리
        clean_text = text[3:] if text.lower().startswith("ig:") else text
        if clean_text.startswith("@"):
            return f"https://www.instagram.com/{clean_text[1:]}/"
        if not clean_text.startswith("http"):
            return f"https://www.instagram.com/{clean_text}/"
        # URL에서 쿼리 파라미터(?igsh=...) 제거
        if "instagram.com" in clean_text:
            return clean_text.split('?')[0].rstrip('/')
        return clean_text
    @classmethod
    def is_match(cls, text: str) -> bool:
        t = text.lower()
        return "instagram.com" in t or t.startswith("ig:")
    @classmethod
    def is_profile(cls, url: str) -> bool:
        # 단건 링크 키워드
        single_markers = ["/p/", "/reel/", "/reels/", "/tv/", "/stories/"]
        # 쿼리 파라미터 제외한 순수 경로 확인
        pure_path = urlparse(url).path.strip('/')
        if any(m in f"/{pure_path}/" for m in single_markers): return False
        # 경로가 존재하고 '/'가 더 이상 없으면 프로필로 간주 (아이디만 있는 경우)
        return pure_path and len(pure_path.split('/')) == 1

SUPPORTED_PLATFORMS = [TikTokExtractor, InstagramExtractor]

# =====================================================================
# [핵심 다운로드 워커 로직]
# =====================================================================

async def download_worker(app):
    """백그라운드 큐 워커: 사용자별 요청 처리"""
    while True:
        # 초기화 (finally 블록 에러 방지)
        actual_file = None
        success_count = 0
        error_msg = ""
        
        try:
            task = await queue.get()
            chat_id, message_id, url, extractor, target_dir, user_text, msg_time = task
        except asyncio.CancelledError:
            break
            
        platform_name = extractor.name
        is_profile = extractor.is_profile(url)

        try:
            # 🕒 시작 상태 알림
            status_msg = await app.bot.send_message(chat_id, "🔍 유효성 확인 중...", reply_to_message_id=message_id)

            # 🍪 쿠키 파일 확인 (인스타그램 차단 방지)
            cookies_file = os.path.join(os.path.dirname(__file__), "cookies.txt")
            cookie_opts = {"cookiefile": cookies_file} if os.path.exists(cookies_file) else {}

            # 1. 정보 추출
            ydl_opts_info = {
                'quiet': True, 
                'no_warnings': True, 
                'extract_flat': True,
                'referer': 'https://www.instagram.com/',
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                **cookie_opts
            }
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(url, download=False)
            
            if not info:
                raise Exception("정보를 가져올 수 없습니다. (비공개 계정이거나 링크 오류)")

            entries = list(info.get('entries', []))
            if not entries:
                # 단건 게시물인 경우 entries가 없을 수 있음
                entries = [info]
            
            # 프로필인 경우 최대 100개, 여러 장의 사진(Carousel)인 경우 전체, 단건 영상인 경우 1개
            if is_profile:
                entries = entries[:100]
            # (is_profile이 아닐 때는 entries 전체를 처리하여 여러 장의 사진 게시물 대응)
            
            # 유효성 확인 성공 알림
            await status_msg.edit_text(f"✅ 확인되었습니다! 다운로드를 시작합니다. (총 {len(entries)}개 예상)")

            # 2. 개별 처리
            for entry in entries:
                if not entry: continue
                entry_url = entry.get('url') or entry.get('webpage_url') or url
                
                # 파일 패턴
                outtmpl = f'{target_dir}/%(id)s_msg{message_id}.%(ext)s'
                # 사진/슬라이드쇼의 경우 bv*+ba/b 형식이 없을 수 있으므로 유연하게 설정
                ydl_opts_down = {
                    'format': 'bestvideo+bestaudio/best', # 형식을 더 포괄적으로 변경
                    'outtmpl': outtmpl,
                    'quiet': True,
                    'no_warnings': True,
                    'noplaylist': True,
                    'referer': 'https://www.instagram.com/',
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    **cookie_opts
                }

                def get_file(bp): return next((f for f in glob.glob(f"{bp}.*") if not f.endswith('.part')), None)

                # 시도 1: 최고 화질
                with yt_dlp.YoutubeDL(ydl_opts_down) as ydl:
                    actual_info = ydl.extract_info(entry_url, download=True)
                    actual_file = get_file(ydl.prepare_filename(actual_info).rsplit(".", 1)[0])

                # ⚠️ 용량 체크 및 재시도
                if actual_file and os.path.exists(actual_file) and os.path.getsize(actual_file) > MAX_FILE_SIZE:
                    os.remove(actual_file)
                    actual_file = None
                    await app.bot.send_message(chat_id, f"⚠️ {platform_name} 용량 초과로 저화질 시도...", reply_to_message_id=message_id)
                    
                    ydl_opts_down['format'] = 'best[filesize<45M]/worst'
                    try:
                        with yt_dlp.YoutubeDL(ydl_opts_down) as ydl:
                            retry_info = ydl.extract_info(entry_url, download=True)
                            actual_file = get_file(ydl.prepare_filename(retry_info).rsplit(".", 1)[0])
                    except Exception:
                        pass # 로그에서 실패 처리

                # ⚠️ 최종 검증 후 전송
                if actual_file and os.path.exists(actual_file) and os.path.getsize(actual_file) <= MAX_FILE_SIZE:
                    ext = actual_file.split('.')[-1].lower()
                    try:
                        with open(actual_file, "rb") as f_obj:
                            if ext in ['jpg', 'jpeg', 'png', 'webp']:
                                await app.bot.send_photo(chat_id, photo=f_obj, caption=f"📸 {platform_name} 사진", reply_to_message_id=message_id)
                            else:
                                await app.bot.send_video(chat_id, video=f_obj, caption=f"✅ {platform_name} 영상", reply_to_message_id=message_id)
                        success_count += 1
                        # 성공 시 삭제하지 않음 (2시 백업용)
                    except Exception as send_err:
                        await app.bot.send_message(chat_id, f"❌ 전송 실패: {send_err}", reply_to_message_id=message_id)
                    finally:
                        # 전송 실패한 찌꺼기만 삭제
                        if success_count == 0 and actual_file and os.path.exists(actual_file):
                            os.remove(actual_file)
                else:
                    if actual_file and os.path.exists(actual_file): os.remove(actual_file)
                    await app.bot.send_message(chat_id, "❌ 전송 불가 (파일 없음/용량 초과)", reply_to_message_id=message_id)

            if is_profile:
                await app.bot.send_message(chat_id, f"🎉 {platform_name} 추출 완료! (총 {success_count}개)", reply_to_message_id=message_id)

        except Exception as e:
            error_msg = str(e)
            if 'status_msg' in locals():
                await status_msg.edit_text(f"❌ 오류: 유효하지 않은 계정이나 링크입니다.\n({error_msg[:50]}...)")
            else:
                await app.bot.send_message(chat_id, f"❌ 처리 에러: {error_msg[:100]}", reply_to_message_id=message_id)
        finally:
            # 상태 기록
            write_bot_log("DOWNLOAD_TASK", {
                "msg_id": message_id,
                "msg_time": msg_time,
                "user_text": user_text,
                "url": url,
                "target_dir": target_dir,
                "success_count": success_count,
                "status": "SUCCESS" if success_count > 0 else "FAILED"
            })
            queue.task_done()
            if queue.empty():
                write_bot_log("QUEUE_EMPTY", {"info": "대기열 작업 완료"})

# =====================================================================
# [명령 및 메시지 핸들러]
# =====================================================================

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message: return
    if u.effective_user.id not in ALLOWED_USER_IDS: 
        await u.message.reply_text("⛔ 등록되지 않은 사용자입니다.") 
        return
    await u.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def help_command(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message: return
    if u.effective_user.id not in ALLOWED_USER_IDS: 
        return # 무시
    await u.message.reply_text(HELP_TEXT, parse_mode="Markdown")

async def handle_message(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.message.text: return
    user_id = u.effective_user.id
    if user_id not in ALLOWED_USER_IDS: return

    text = u.message.text.strip()
    m = next((ext for ext in SUPPORTED_PLATFORMS if ext.is_match(text)), None)
    if not m:
        await u.message.reply_text("📥 지원하지 않는 형식입니다.", reply_to_message_id=u.message.message_id)
        return

    # 📂 저장 경로
    now = datetime.datetime.now()
    target_dir = os.path.join(DOWNLOAD_ROOT, str(user_id), now.strftime("%Y"), now.strftime("%m%d"))
    os.makedirs(target_dir, exist_ok=True)
    msg_time = now.strftime("%H:%M:%S")

    processed_url = m.format_input(text)
    await queue.put((u.effective_chat.id, u.message.message_id, processed_url, m, target_dir, text, msg_time))
    
    # 즉각적인 응답 (타임아웃 방지)
    await u.message.reply_text(f"🚀 서버에 전달완료! 곧 다운로드가 시작됩니다. ({m.name})", reply_to_message_id=u.message.message_id)

async def post_init(app):
    for _ in range(3): workers.append(asyncio.create_task(download_worker(app)))
    print("🚀 Bot Started")

async def post_stop(app):
    for t in workers: t.cancel()
    print("🛑 Bot Stopped")

def main():
    if BOT_TOKEN == "YOUR_NEW_TOKEN_HERE": return
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).post_stop(post_stop).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == "__main__":
    main()