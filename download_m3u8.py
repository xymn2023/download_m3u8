# -*- coding: utf-8 -*-
import os
import re
import shutil
import subprocess
import concurrent.futures
from urllib.parse import urlparse, unquote, urljoin
import hashlib
import logging
import time
import random
import sys # 导入sys模块以检查操作系统

# --- 配置 (一键安装最终版 v1.0) ---
# (这部分配置与上一版完全相同)
OUTPUT_DIR = 'video'
TEMP_DIR = 'temp'
SOURCE_EXTENSION = '.m3u'
LOG_FILE = 'download.log'
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_WORKERS = 32
MERGE_RETRY_COUNT = 2
VERIFY_SSL = False
REQUEST_TIMEOUT = (5, 10)
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60

# --- 日志和环境检查 ---
def setup_logging():
    # (此函数无变化)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers(): logger.handlers.clear()
    fh = logging.FileHandler(LOG_FILE, 'w', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

# --- ★★★★★★★ 【核心改动 - 终极版】 ★★★★★★★ ---
# 再次重写环境依赖检查函数，为Windows提供自动化安装命令
def check_dependencies():
    """
    检查所有必要的依赖，并在缺失时提供详细的、可操作的安装指导。
    """
    logging.info("正在检查环境依赖...")
    all_deps_ok = True
    
    # 1. 检查 Python 依赖库
    try:
        import requests
        import tqdm
        import urllib3
        logging.info("[√] Python 依赖库 (requests, tqdm) 已安装。")
    except ImportError as e:
        missing_module = e.name
        logging.critical(f"[X] 致命错误: 缺少必要的 Python 库 '{missing_module}'。")
        print("\n" + "="*70)
        print("--- Python 依赖安装指南 ---")
        print("请在您的终端(命令行)中运行以下命令来安装所有必需的库:")
        print(">>> pip install requests tqdm")
        print("="*70 + "\n")
        all_deps_ok = False

    # 2. 检查 FFmpeg 程序
    if not shutil.which('ffmpeg'):
        logging.critical("[X] 致命错误: 系统路径中未找到 ffmpeg 程序。")
        print("\n" + "="*70)
        print("--- FFmpeg 安装指南 ---")
        print("FFmpeg 是合并视频分片的核心工具，请根据您的操作系统进行安装：")
        
        # --- 针对 Windows 系统的特别优化 ---
        if sys.platform == 'win32':
            print("\n[ Windows 系统 ]")
            print("\n--- 方法1 (推荐): 使用 PowerShell 自动安装 (无需管理员权限) ---")
            print("1. 打开 'PowerShell' (不是 CMD)。")
            print("2. 复制下面这【整段】命令，粘贴到 PowerShell 中并按回车执行：")
            print("-" * 60)
            # 使用三重引号定义多行字符串，然后用分号连接成单行
            ps_command = (
                '$ffmpegDir = "C:\\FFmpeg"; $zipPath = "$env:TEMP\\ffmpeg.zip"; '
                'if (-not (Test-Path $ffmpegDir)) { New-Item -ItemType Directory -Path $ffmpegDir }; '
                'Write-Host "正在下载 FFmpeg..."; '
                'Invoke-WebRequest -Uri "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip" -OutFile $zipPath; '
                'Write-Host "下载完成，正在解压..."; '
                'Expand-Archive -Path $zipPath -DestinationPath $ffmpegDir -Force; '
                '$binPath = (Get-ChildItem -Path $ffmpegDir -Directory).FullName | Where-Object { $_ -like "*bin" } | Select-Object -First 1; '
                'if ($binPath) { '
                '  Write-Host "找到bin目录: $binPath"; '
                '  $currentPath = [System.Environment]::GetEnvironmentVariable("Path", "User"); '
                '  if ($currentPath -notlike "*$binPath*") { '
                '    [System.Environment]::SetEnvironmentVariable("Path", "$currentPath;$binPath", "User"); '
                '    Write-Host "成功将FFmpeg添加到用户环境变量!"; '
                '  } else { Write-Host "FFmpeg已存在于环境变量中。"; } '
                '} else { Write-Host "错误：未能在解压文件中找到bin目录。"; }; '
                'Remove-Item $zipPath; '
                'Write-Host "安装完成! 请【务必关闭并重新打开】当前的终端窗口，然后再次运行本脚本。"'
            )
            print(ps_command)
            print("-" * 60)
            
            print("\n--- 方法2 (备选): 手动安装 ---")
            print("  1. 访问 FFmpeg 官网下载: https://ffmpeg.org/download.html")
            print("  2. 下载后解压 (例如解压到 C:\\ffmpeg)。")
            print("  3. 将解压后文件夹内的 `bin` 目录 (例如 C:\\ffmpeg\\bin) 添加到系统的'环境变量'的'Path'中。")

        # --- 针对非 Windows 系统的指南 ---
        else:
            if sys.platform == 'darwin': # macOS
                print("\n[ macOS ]")
                print("  如果您安装了 Homebrew，请运行: brew install ffmpeg")
            elif sys.platform.startswith('linux'): # Linux
                print("\n[ Linux (Debian / Ubuntu) ]")
                print("  请运行: sudo apt-get update && sudo apt-get install ffmpeg")
                print("\n[ Linux (CentOS / Fedora) ]")
                print("  请运行: sudo dnf install ffmpeg  (或 sudo yum install ffmpeg)")
        
        print("\n【重要提示】: 安装完成后，请【关闭并重新打开】一个新的终端窗口再运行此脚本。")
        print("="*70 + "\n")
        all_deps_ok = False
    else:
        logging.info("[√] FFmpeg 已安装并可在系统路径中找到。")

    if all_deps_ok:
        logging.info("环境依赖检查通过，脚本可以正常运行。")
    else:
        logging.error("环境检查失败，请根据以上提示安装所需依赖后再试。")
        
    return all_deps_ok

# --- 再次导入依赖 ---
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    from tqdm import tqdm
except ImportError:
    exit()

# --- 后续所有代码 (网络会话、文件名处理、下载、合并、主函数) ---
# ... (此处省略和上一版完全相同的代码，直接复制即可) ...
def create_session_with_retries(retries, pool_size):
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry_strategy = Retry(total=retries, connect=retries, read=retries, redirect=retries, status_forcelist=None)
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def find_unique_filename(output_dir, base_name, extension=".mp4"):
    base_name = os.path.splitext(base_name)[0]
    original_filepath = os.path.join(output_dir, f"{base_name}{extension}")
    if not os.path.exists(original_filepath):
        return f"{base_name}{extension}"
    counter = 1
    while True:
        new_name = f"{base_name}{counter:03d}{extension}"
        new_filepath = os.path.join(output_dir, new_name)
        if not os.path.exists(new_filepath):
            return new_name
        counter += 1

def sanitize_text_for_filename(text):
    if not text: return ""
    return re.sub(r'[\\/*?:"<>|]', "_", text).strip()

def create_fallback_filename(url):
    try:
        decoded_url = unquote(url)
        path = urlparse(decoded_url).path
        name_part, _ = os.path.splitext(os.path.basename(path))
        if not name_part or 'index' in name_part.lower():
            path_parts = [part for part in path.split('/') if part and part.lower() != 'hls']
            if len(path_parts) > 1: name_part = f"{path_parts[-2]}"
            else: name_part = hashlib.md5(url.encode()).hexdigest()[:12]
        return sanitize_text_for_filename(name_part)
    except Exception:
        return hashlib.md5(url.encode()).hexdigest()

def find_links_with_names():
    tasks = {}
    line_pattern = re.compile(r"^(.*?)(?:,)?(https?://[^\s'\"<>()]+\.m3u8)")
    for filename in [f for f in os.listdir('.') if f.endswith(SOURCE_EXTENSION)]:
        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    match = line_pattern.search(line.strip())
                    if match:
                        raw_name, url = match.group(1).strip(), match.group(2).strip()
                        if url in tasks: continue
                        sanitized_name = sanitize_text_for_filename(raw_name) or create_fallback_filename(url)
                        if not sanitized_name:
                            sanitized_name = "untitled_" + hashlib.md5(url.encode()).hexdigest()[:8]
                        tasks[url] = sanitized_name
        except Exception as e:
            logging.error(f"读取文件 {filename} 时出错: {e}")
    return [(name, url) for url, name in tasks.items()]

def resolve_master_playlist(m3u8_url, session):
    logging.debug(f"正在解析M3U8链接: {m3u8_url}")
    try:
        r = session.get(m3u8_url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        r.raise_for_status()
        content = r.text
        if "#EXT-X-STREAM-INF" in content:
            logging.info("识别到大师列表, 自动选择最高码率...")
            lines, best_stream_url, max_bandwidth = content.strip().split('\n'), None, 0
            for i, line in enumerate(lines):
                if line.startswith("#EXT-X-STREAM-INF"):
                    bw_match = re.search(r'BANDWIDTH=(\d+)', line)
                    if bw_match and (i + 1 < len(lines)):
                        bw = int(bw_match.group(1))
                        stream_url = urljoin(m3u8_url, lines[i+1].strip())
                        if bw > max_bandwidth: max_bandwidth, best_stream_url = bw, stream_url
            if best_stream_url:
                logging.info(f"已选择最高码率流: {best_stream_url}")
                return best_stream_url
        logging.info("识别为媒体列表。")
        return m3u8_url
    except requests.exceptions.RequestException as e:
        logging.error(f"解析M3U8失败 (链接: {m3u8_url}): {type(e).__name__}")
        return None

def download_segment(args):
    segment_url, temp_dir, index, session = args
    filepath = os.path.join(temp_dir, f"{index:06d}.ts")
    current_delay = INITIAL_RETRY_DELAY
    while True:
        try:
            r = session.get(segment_url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL, stream=True)
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                first_chunk = next(r.iter_content(chunk_size=8192), None)
                if first_chunk is None: raise requests.exceptions.RequestException("下载到了空的响应体")
                f.write(first_chunk)
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
            if os.path.getsize(filepath) == 0: raise requests.exceptions.RequestException(f"文件下载后大小为0。")
            return True
        except requests.exceptions.RequestException as e:
            logging.warning(f"分片 {index} 下载失败: {type(e).__name__}。将在 {current_delay:.1f} 秒后重试...")
            time.sleep(current_delay)
            current_delay = min(current_delay * 2, MAX_RETRY_DELAY) + random.uniform(0, 1)

def download_video(base_output_name, m3u8_url, task_num, total_tasks, session, segment_download_session):
    progress_prefix = f"[{task_num}/{total_tasks}]"
    final_output_filename = find_unique_filename(OUTPUT_DIR, base_output_name)
    logging.info(f"{progress_prefix} 开始处理: {final_output_filename} (URL: {m3u8_url})")
    media_playlist_url = resolve_master_playlist(m3u8_url, session)
    if not media_playlist_url:
        return ("failed", f"无法解析或连接M3U8链接: {m3u8_url}")
    output_filepath = os.path.abspath(os.path.join(OUTPUT_DIR, final_output_filename))
    task_temp_dir = os.path.join(TEMP_DIR, f"task_{hashlib.md5(m3u8_url.encode()).hexdigest()[:10]}")
    try:
        if os.path.exists(task_temp_dir): shutil.rmtree(task_temp_dir)
        os.makedirs(task_temp_dir, exist_ok=True)
        r = session.get(media_playlist_url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        r.raise_for_status()
        content = r.text; lines = content.strip().split('\n')
        target_duration_line = next((line for line in lines if line.startswith("#EXT-X-TARGETDURATION")), "")
        segments, segment_infos, key_info = [], [], {}
        for line in lines:
            if line.startswith('#EXTINF:'): segment_infos.append(line)
            elif line.startswith('#EXT-X-KEY'): key_info['line'] = line
            elif not line.startswith('#') and line.strip(): segments.append(urljoin(media_playlist_url, line.strip()))
        if key_info.get('line'):
            uri_match = re.search(r'URI="([^"]+)"', key_info['line'])
            if uri_match:
                key_url = urljoin(media_playlist_url, uri_match.group(1))
                try:
                    r_key = session.get(key_url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
                    r_key.raise_for_status()
                    key_filename = os.path.basename(urlparse(key_url).path)
                    key_filepath = os.path.join(task_temp_dir, key_filename)
                    with open(key_filepath, 'wb') as f: f.write(r_key.content)
                    key_info['filename'] = key_filename
                except requests.exceptions.RequestException:
                    raise Exception(f"加密视频密钥下载失败: {key_url}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [(segments[i], task_temp_dir, i, segment_download_session) for i in range(len(segments))]
            list(tqdm(executor.map(download_segment, tasks), total=len(segments), desc=f"{progress_prefix} 下载分片 {os.path.splitext(final_output_filename)[0][:20]:<20}", unit="seg", bar_format='{l_bar}{bar:30}{r_bar}'))
        local_m3u8_path = os.path.join(task_temp_dir, "local.m3u8")
        with open(local_m3u8_path, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n#EXT-X-VERSION:3\n")
            if target_duration_line: f.write(target_duration_line + '\n')
            if key_info.get('filename'):
                f.write(re.sub(r'URI="[^"]+"', f'URI="{key_info["filename"]}"', key_info['line']) + '\n')
            for i in range(len(segments)):
                if i < len(segment_infos): f.write(segment_infos[i] + '\n')
                f.write(f"{i:06d}.ts" + '\n')
            f.write("#EXT-X-ENDLIST\n")
        logging.info(f"{progress_prefix} 所有分片下载完毕, 正在调用FFmpeg进行合并...")
        command = ['ffmpeg','-y','-protocol_whitelist','file,crypto,tcp,tls,http,https','-allowed_extensions','ALL','-i','local.m3u8','-c','copy','-bsf:a','aac_adtstoasc',output_filepath]
        last_error = ""
        for attempt in range(MERGE_RETRY_COUNT):
            if attempt > 0: logging.warning(f"{progress_prefix} 合并失败, 正在进行第 {attempt + 1}/{MERGE_RETRY_COUNT} 次重试...")
            process = subprocess.run(command,cwd=task_temp_dir,capture_output=True,text=True,encoding='utf-8',errors='ignore')
            if process.returncode == 0:
                logging.info(f"{progress_prefix} ✅ 成功保存到: {final_output_filename}")
                return ("success", final_output_filename)
            else:
                last_error = f"FFmpeg合并失败，返回码: {process.returncode}\n--- FFmpeg 详细错误信息 ---\n{(process.stderr.strip() or 'FFmpeg未返回任何错误信息。')}\n--------------------------"
        raise Exception(last_error)
    except Exception as e:
        logging.error(f"{progress_prefix} ❌ 下载任务失败: {e}")
        return ("failed", str(e))
    finally:
        if os.path.exists(task_temp_dir):
            shutil.rmtree(task_temp_dir)

def main():
    setup_logging()
    logging.info("============================================================")
    logging.info("开始执行任务：M3U8批量下载器 (一键安装最终版)")
    
    if not check_dependencies():
        logging.info("脚本已终止。请在完成上述环境配置后再次运行。")
        return
    
    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logging.warning("SSL证书验证已禁用。")
    os.makedirs(OUTPUT_DIR, exist_ok=True); os.makedirs(TEMP_DIR, exist_ok=True)
    logging.info(f"视频输出目录: {os.path.abspath(OUTPUT_DIR)}/")
    logging.info(f"临时文件目录: {os.path.abspath(TEMP_DIR)}/")
    logging.info(f"文件名策略: 智能重命名 (例如: video001.mp4, video002.mp4)")
    logging.info(f"分片下载策略: 无限重试 (初始延迟{INITIAL_RETRY_DELAY}s, 最大延迟{MAX_RETRY_DELAY}s)")

    m3u8_parse_session = create_session_with_retries(retries=0, pool_size=10)
    segment_download_session = create_session_with_retries(retries=0, pool_size=MAX_WORKERS)
    tasks_to_download = find_links_with_names()
    if not tasks_to_download:
        logging.warning(f"未在任何 .{SOURCE_EXTENSION} 文件中找到有效的 .m3u8 链接。")
        return
    tasks_to_download.sort(key=lambda x: x[0])
    total_tasks = len(tasks_to_download)
    logging.info(f"共发现 {total_tasks} 个不重复的 .m3u8 任务，准备开始下载...")
    success_count, failed_count = 0, 0
    for i, (base_name, url) in enumerate(tasks_to_download):
        status, message = download_video(base_name, url, i + 1, total_tasks, m3u8_parse_session, segment_download_session)
        if status == "success": success_count += 1
        else: failed_count += 1
    logging.info("============================================================")
    logging.info("所有任务已处理完毕！")
    logging.info(f"    - 成功: {success_count}, 失败: {failed_count}")
    logging.info(f"[*] 所有视频文件均保存在 '{OUTPUT_DIR}' 文件夹中。")
    logging.info(f"[*] 遗留的空临时文件夹 '{TEMP_DIR}' 如不需要可手动删除。")
    logging.info(f"[*] 详细的运行日志已保存在 '{LOG_FILE}' 文件中。")
    logging.info("============================================================")

if __name__ == "__main__":
    main()
