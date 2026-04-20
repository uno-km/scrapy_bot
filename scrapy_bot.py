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

# ❗❗ BotFather에서 받은 토큰을 입력하세요
BOT_TOKEN = "YOUR_NEW_TOKEN_HERE"

DOWNLOAD_ROOT = "download"
BOT_LOG_FILE = "bot_activity.log"

# 텔레그램 봇 API 최대 파일 크기 (50MB) - 여유를 두고 49.5MB로 설정
MAX_FILE_SIZE = 49.5 * 1024 * 1024 

# 🔐 허용된 사용자 ID 리스트 (하드코딩 필수!)
ALLOWED_USER_IDS = [] 

# ✨ 프리미엄 매뉴얼 텍스트
HELP_TEXT = """
✨ **GetItOut Bot 프리미엄 매뉴얼** ✨

안녕하세요! 본 봇은 고화질 소셜 미디어 콘텐츠 다운로더입니다.
허용된 프리미엄 사용자만 모든 기능을 이용하실 수 있습니다.

🚀 **핵심 기능 안내**
━━━━━━━━━━━━━━━━━━━━
1️⃣ **틱톡 (TikTok)**
   • 영상 링크 전달 시 즉시 다운로드 (워터마크 제거)
   • `@아이디` 입력 시 해당 프로필의 최근 영상 100개 추출

2️⃣ **인스타그램 (Instagram)**
   • 릴스 / 게시물 링크 전달 시 즉시 다운로드
   • 프로필 링크 전달 시 최근 게시물 100개 추출

🛠 **정교한 사용 방법**
━━━━━━━━━━━━━━━━━━━━
• **간편 전송**: 단순히 링크를 채팅방에 복사+붙여넣기 하세요.
• **자동 최적화**: 영상이 50MB를 초과하면 자동으로 저화질로 변환하여 전송을 시도합니다.
• **답장 기능**: 결과물은 항상 사용자가 보낸 링크에 '답장' 형태로 전달됩니다.

❓ **사용 가능 명령어**
━━━━━━━━━━━━━━━━━━━━
• `/help` - 지금 보고 계신 매뉴얼을 다시 불러옵니다.
• `/start` - 봇 시작 및 접근 권한을 확인합니다.

⚠️ **보안 및 정책**
━━━━━━━━━━━━━━━━━━━━
• 본 봇은 화이트리스트제로 운영됩니다. 
• 등록되지 않은 아이디는 모든 요청이 무시됩니다.
• 서버 용량 확보를 위해 매일 새벽 데이터 백업 및 정리가 수행됩니다.
"""

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
        if text.startswith("@"): return f"https://www.tiktok.com/{text}"
        return text
    @classmethod
    def is_match(cls, text: str) -> bool:
        return "tiktok.com" in text or text.startswith("@")
    @classmethod
    def is_profile(cls, url: str) -> bool:
        # 단건 링크 키워드 제외
        single_markers = ["/video/", "/photo/", "/story/", "/v/"]
        return not any(m in url for m in single_markers) and "@" in url

class InstagramExtractor(BaseExtractor):
    name = "Instagram"
    @classmethod
    def is_match(cls, text: str) -> bool: return "instagram.com" in text
    @classmethod
    def is_profile(cls, url: str) -> bool:
        # 단건 링크 키워드
        single_markers = ["/p/", "/reel/", "/reels/", "/tv/", "/stories/"]
        if any(m in url for m in single_markers): return False
        path = urlparse(url).path.strip('/')
        return path and len(path.split('/')) == 1

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
            # 1. 정보 추출
            ydl_opts_info = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(url, download=False)
            
            entries = list(info.get('entries', [info]))
            max_items = 100 if is_profile else 1
            entries = entries[:max_items]

            # 2. 개별 처리
            for entry in entries:
                if not entry: continue
                entry_url = entry.get('url') or entry.get('webpage_url') or url
                
                # 파일 패턴
                outtmpl = f'{target_dir}/%(id)s_msg{message_id}.%(ext)s'
                ydl_opts_down = {
                    'format': 'bv*+ba/b',
                    'outtmpl': outtmpl,
                    'quiet': True,
                    'no_warnings': True,
                    'noplaylist': True
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
    
    if m.is_profile(processed_url):
        await u.message.reply_text(f"⏳ {m.name} 프로필 추출 대기열 추가...", reply_to_message_id=u.message.message_id)
    else:
        await u.message.reply_text(f"⏳ 대기열 추가됨 ({m.name})", reply_to_message_id=u.message.message_id)

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