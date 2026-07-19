import os
import time
import sys
import shutil
import configparser
import logging
from logging.handlers import RotatingFileHandler
import psycopg2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- LOAD CONFIGURATION & DIRECTORIES ---
config = configparser.ConfigParser()
if getattr(sys, 'frozen', False):
    script_dir = os.path.dirname(sys.executable)
else:
    script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'conf.ini')

if not os.path.exists(config_path):
    raise FileNotFoundError(f"Could not find config.ini at expected path: {config_path}")

config.read(config_path)
WATCH_DIRECTORY = config.get('MONITOR', 'watch_directory')
CONVERTED_DIRECTORY = os.path.join(WATCH_DIRECTORY, 'converted')

# --- ERROR-ONLY LOGGING SETUP ---
log_file_path = os.path.join(script_dir, 'pipeline.log')

logging.basicConfig(
    level=logging.ERROR,  
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    ]
)

DB_PARAMS = {
    'host': config.get('DATABASE', 'host'),
    'database': config.get('DATABASE', 'database'),
    'user': config.get('DATABASE', 'user'),
    'password': config.get('DATABASE', 'password'),
    'port': config.get('DATABASE', 'port')
}

TARGET_KEYS = [
    "SpoolCodePO", "SpoolCodeTU", "StartTime", "EndTime", "Operator", 
    "SetLength", "RealLength", "MachineStopReasonT", "StartDate", 
    "EndDate", "RunSpeed", "MachineTotalTime", "MachineTotallength", 
    "IdleTime", "Runtime"
]

def wait_until_not_written(filepath, check_interval=1.0, timeout=15.0):
    start_time = time.time()
    last_size = -1
    while (time.time() - start_time) < timeout:
        try:
            if not os.path.exists(filepath):
                return False
            current_size = os.path.getsize(filepath)
            if current_size == last_size and current_size > 0:
                return True
            last_size = current_size
        except (IOError, OSError):
            pass
        time.sleep(check_interval)
    return False

def parse_txt_file(filepath):
    extracted_data = {}
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if '=' in line:
                    key, value = line.split('=', 1)
                    key, value = key.strip(), value.strip()
                    if key in TARGET_KEYS:
                        extracted_data[key] = value
        return extracted_data
    except Exception as e:
        logging.error(f"Parsing file failed for {filepath}: {e}")
        return None

def send_to_postgres(data):
    query = """
        INSERT INTO pt_machine_logs (
            spool_code_tu, spool_code_po, start_time, end_time, operator, 
            set_length, real_length, machine_stop_reason_t, start_date, 
            end_date, run_speed, machine_total_time, machine_total_length, 
            idle_time, runtime
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (spool_code_tu) DO UPDATE SET
            spool_code_po = EXCLUDED.spool_code_po,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            operator = EXCLUDED.operator,
            set_length = EXCLUDED.set_length,
            real_length = EXCLUDED.real_length,
            machine_stop_reason_t = EXCLUDED.machine_stop_reason_t,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            run_speed = EXCLUDED.run_speed,
            machine_total_time = EXCLUDED.machine_total_time,
            machine_total_length = EXCLUDED.machine_total_length,
            idle_time = EXCLUDED.idle_time,
            runtime = EXCLUDED.runtime,
            processed_at = CURRENT_TIMESTAMP;
    """
    
    start_time_clean = data.get("StartTime").replace('-', ':') if data.get("StartTime") else None
    end_time_clean = data.get("EndTime").replace('-', ':') if data.get("EndTime") else None
    
    values = (
        data.get("SpoolCodeTU"), data.get("SpoolCodePO"), start_time_clean, end_time_clean,     
        data.get("Operator"), data.get("SetLength"), data.get("RealLength"), 
        data.get("MachineStopReasonT"), data.get("StartDate"), data.get("EndDate"), 
        data.get("RunSpeed"), data.get("MachineTotalTime"), data.get("MachineTotallength"),
        data.get("IdleTime"), data.get("Runtime")
    )
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logging.error(f"Database sync failed for TU {data.get('SpoolCodeTU')}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def move_to_converted(filepath):
    try:
        os.makedirs(CONVERTED_DIRECTORY, exist_ok=True)
        filename = os.path.basename(filepath)
        destination = os.path.join(CONVERTED_DIRECTORY, filename)
        
        if os.path.exists(destination):
            base, ext = os.path.splitext(filename)
            destination = os.path.join(CONVERTED_DIRECTORY, f"{base}_{int(time.time())}{ext}")
            
        shutil.move(filepath, destination)
    except Exception as e:
        logging.error(f"Could not move file {filepath} to converted directory: {e}")

def process_file(filepath):
    try:
        filename = os.path.basename(filepath)
        if "converted" in filepath.replace('\\', '/').split('/'):
            return

        if filename.endswith('.txt') and '_' not in filename:
            if wait_until_not_written(filepath):
                data = parse_txt_file(filepath)
                if data and data.get("SpoolCodeTU"):
                    if send_to_postgres(data):
                        move_to_converted(filepath)
    except Exception as e:
        logging.critical(f"Critical execution block failure on file {filepath}: {e}")

def process_existing_files():
    try:
        if os.path.exists(WATCH_DIRECTORY):
            for entry in os.scandir(WATCH_DIRECTORY):
                if entry.is_file() and entry.name.endswith('.txt') and '_' not in entry.name:
                    process_file(entry.path)
    except Exception as e:
        logging.error(f"Error during backlog directory scan: {e}")

class TextFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            process_file(event.src_path)
    def on_modified(self, event):
        if not event.is_directory:
            process_file(event.src_path)

if __name__ == "__main__":
    observer = None  # Explicit baseline target declaration to protect context scope
    
    while True:
        try:
            # 1. Wait until the watch directory is available
            while not os.path.exists(WATCH_DIRECTORY):
                logging.error(f"Target directory '{WATCH_DIRECTORY}' missing. Retrying in 10 seconds...")
                time.sleep(10)
                
            process_existing_files()
            
            # 2. Setup and start Watchdog Observer
            event_handler = TextFileHandler()
            observer = Observer()
            observer.schedule(event_handler, path=WATCH_DIRECTORY, recursive=False)
            observer.start()
            
            # 3. Monitor folder health mid-operation
            while os.path.exists(WATCH_DIRECTORY):
                time.sleep(2)
                
            # If folder drops out mid-run
            logging.error(f"Directory '{WATCH_DIRECTORY}' disappeared during operation! Re-initializing standby mode...")
            if observer:
                observer.stop()
                observer.join()
                observer = None

        except KeyboardInterrupt:
            if observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception:
                    pass
            break
        except Exception as e:
            logging.critical(f"Internal supervisor exception intercepted: {e}. Restarting engine stack...")
            if observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception:
                    pass
                observer = None
            time.sleep(5)
