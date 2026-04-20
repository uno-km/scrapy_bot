import time
import datetime
import subprocess
import os
import shutil
import smtplib
import zipfile
import glob
import json
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

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
CONFIG_ALL = load_config()
CONFIG_BOT = CONFIG_ALL
CONFIG_MGR = CONFIG_ALL.get("MANAGER", {})

SMTP_SERVER = CONFIG_MGR.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = CONFIG_MGR.get("SMTP_PORT", 587)
EMAIL_USER = CONFIG_MGR.get("EMAIL_USER")
EMAIL_PASS = CONFIG_MGR.get("EMAIL_PASS")
RECEIVER_EMAIL = CONFIG_MGR.get("RECEIVER_EMAIL")
BOT_FILE = CONFIG_MGR.get("BOT_FILE", "scrapy_bot.py")
DOWNLOAD_ROOT = CONFIG_BOT.get("DOWNLOAD_ROOT", "download")
MAX_EMAIL_SIZE = CONFIG_MGR.get("MAX_EMAIL_SIZE_MB", 20) * 1024 * 1024
MANAGER_LOG_FILE = CONFIG_MGR.get("MANAGER_LOG_FILE", "manager_history.log")


CURRENT_YTDLP_VERSION = "Unknown"

def write_log(event_type, details):
    """관리 로그 기록"""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "log_id": int(time.time() * 1000),
            "timestamp": timestamp,
            "event": event_type,
            "ytdlp_version": CURRENT_YTDLP_VERSION,
            "details": details
        }
        with open(MANAGER_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"❌ 로그 기록 실패: {e}")

def update_ytdlp():
    global CURRENT_YTDLP_VERSION
    try:
        subprocess.run(["pip", "install", "--upgrade", "yt-dlp"], capture_output=True, text=True)
        result = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True)
        CURRENT_YTDLP_VERSION = result.stdout.strip()
        print(f"✅ yt-dlp 버전 확인: {CURRENT_YTDLP_VERSION}")
    except Exception as e:
        print(f"⚠️ 업데이트 오류: {e}")

def zip_by_date(path, target_mmdd, zip_filename):
    """특정 날짜(MMDD)의 폴더 내용만 압축합니다."""
    count = 0
    if not os.path.exists(path): return 0
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path):
            if os.path.basename(root) == target_mmdd:
                for file in files:
                    file_path = os.path.join(root, file)
                    # 압축 파일 내 경로: download/ID/YYYY/MMDD/file
                    arcname = os.path.relpath(file_path, os.path.join(path, ".."))
                    zipf.write(file_path, arcname)
                    count += 1
    return count

def split_file(filename, chunk_size):
    """지정 크기로 파일 분할"""
    if os.path.getsize(filename) <= chunk_size: return [filename]
    part_files = []
    with open(filename, 'rb') as f:
        part_num = 1
        while True:
            chunk = f.read(chunk_size)
            if not chunk: break
            p_name = f"{filename}.part{part_num}"
            with open(p_name, 'wb') as pf: pf.write(chunk)
            part_files.append(p_name)
            part_num += 1
    return part_files

def send_email_with_file(filename, subject):
    """이메일 발송"""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = subject
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(filename)}")
            msg.attach(part)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        write_log("EMAIL_ERROR", {"file": filename, "error": str(e)})
        return False

def run_maintenance():
    """새벽 메인터넌스: 어제 데이터 추출 -> 압축 -> 메일"""
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    t_mmdd = yesterday.strftime("%m%d")
    t_date_str = yesterday.strftime("%Y-%m-%d")
    
    zip_name = f"Backup_{t_date_str}.zip"
    print(f"📦 [{t_date_str}] 데이터 백업 중...")
    
    f_count = zip_by_date(DOWNLOAD_ROOT, t_mmdd, zip_name)
    
    if f_count == 0:
        if os.path.exists(zip_name): os.remove(zip_name)
        write_log("MAINTENANCE_SKIP", f"No data found for {t_mmdd}")
        print("ℹ️ 백업할 데이터가 없습니다.")
        return

    parts = split_file(zip_name, MAX_EMAIL_SIZE)
    success_all = True
    for i, part in enumerate(parts):
        sub = f"[Backup] {t_date_str} ({i+1}/{len(parts)})" if len(parts) > 1 else f"[Backup] {t_date_str}"
        if not send_email_with_file(part, sub):
            success_all = False
            break
            
    if success_all:
        write_log("MAINTENANCE_COMPLETE", {"date": t_date_str, "files": f_count})
        print(f"✅ [{t_date_str}] 백업 메일 발송 완료!")
        # 임시 zip 파일 삭제 (원본은 7일간 유지)
        if len(parts) > 1:
            for p in parts: 
                if os.path.exists(p): os.remove(p)
        if os.path.exists(zip_name): os.remove(zip_name)
    else:
        print("⚠️ 백업 메일 발송 중 일부 실패")

def cleanup_old_files(days=7):
    """7일 이상 된 모든 찌꺼기 및 원본 폴더 삭제"""
    now = datetime.datetime.now()
    cutoff_ts = (now - datetime.timedelta(days=days)).timestamp()
    
    print(f"🧹 {days}일 이상 지난 데이터 정리 중...")
    
    # 1. 이전 백업 찌꺼기 삭제
    for f in glob.glob("Backup_*.zip*"):
        if os.path.getmtime(f) < cutoff_ts:
            os.remove(f)

    # 2. 7일이 지난 원본 디렉토리 정밀 삭제
    if os.path.exists(DOWNLOAD_ROOT):
        for root, dirs, files in os.walk(DOWNLOAD_ROOT, topdown=False):
            for file in files:
                f_path = os.path.join(root, file)
                if os.path.getmtime(f_path) < cutoff_ts:
                    try: os.remove(f_path)
                    except: pass
            for d in dirs:
                d_path = os.path.join(root, d)
                # 폴더 자체가 오래되었거나 비어있으면 삭제
                if os.path.getmtime(d_path) < cutoff_ts:
                    try: shutil.rmtree(d_path)
                    except: pass

def main():
    print("🛡️ Bot Manager 실행 중...")
    update_ytdlp()
    bot_proc = None
    
    while True:
        now = datetime.datetime.now()
        
        # 봇 프로세스 자동 재시작
        if bot_proc is None or bot_proc.poll() is not None:
            print(f"🚀 {BOT_FILE} 가동 시작...")
            bot_proc = subprocess.Popen(["python", BOT_FILE])
            write_log("BOT_START", {"time": now.strftime("%H:%M:%S")})
            
        # 유지보수 (새벽 2시)
        if now.hour == 2 and now.minute == 0:
            print("🕒 정기 메인터넌스 시간...")
            if bot_proc:
                bot_proc.terminate()
                bot_proc.wait()
                bot_proc = None
                write_log("BOT_STOP", "Maintenance Start")
            
            update_ytdlp()
            run_maintenance()   # 어제 날짜 데이터 백업
            cleanup_old_files() # 7일 지난 데이터 영구 삭제
            
            print("💤 메인터넌스 완료. 1분 후 재가동합니다.")
            time.sleep(50)
            continue
            
        time.sleep(10)

if __name__ == "__main__":
    main()

