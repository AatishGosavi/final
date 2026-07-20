import os
import sys
import time
import shutil
import configparser
from datetime import datetime
import psycopg2
from psycopg2 import pool
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIGURATION & LOGGING SETUP ---
LOG_FILE = "monitor_service_log.txt"

# Direct standard outputs to append to the log file (critical for windowed background EXEs)
sys.stdout = open(LOG_FILE, "a", buffering=1, encoding="utf-8")
sys.stderr = sys.stdout

config = configparser.ConfigParser()
if getattr(sys, 'frozen', False):
    script_dir = os.path.dirname(sys.executable)
else:
    script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'conf1.ini')

if not os.path.exists(config_path):
    print(f"[{time.ctime()}] FATAL CRITICAL ERROR: Could not find config.ini at: {config_path}")
    sys.exit(1)

config.read(config_path)
WATCH_DIRECTORY = config.get('MONITOR', 'watch_directory')
CONVERT_DIRECTORY = os.path.join(WATCH_DIRECTORY, 'convert')

DB_PARAMS = {
    'host': config.get('DATABASE', 'host'),
    'database': config.get('DATABASE', 'database'),
    'user': config.get('DATABASE', 'user'),
    'password': config.get('DATABASE', 'password'),
    'port': config.get('DATABASE', 'port')
}

PROCESSED_FILES = {}
DUPLICATE_COOLDOWN_SECONDS = 5  
db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, **DB_PARAMS)
        print("--- Database connection pool created successfully. ---")
    except Exception as e:
        print(f"--- Warning: Initial Database pool setup failed ({e}). Will attempt to reconnect dynamically. ---")

# Try parsing connection configuration at startup
init_db_pool()


def safe_python_numeric(value_str):
    try:
        clean_str = value_str.strip()
        if clean_str.lower() in ['nan', '-----', '', 'null']:
            return None
        # Handle scientific E-notation safely
        return float(clean_str)
    except (ValueError, TypeError):
        return None


def format_db_date(date_str):
    try:
        clean_date = date_str.strip()
        parsed_date = datetime.strptime(clean_date, "%m/%d/%Y")
        return parsed_date.strftime("%Y-%m-%d")
    except Exception:
        return date_str


def wait_for_file_to_be_free(file_path, timeout=10, check_interval=1):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with open(file_path, 'r+'):
                return True
        except (IOError, PermissionError):
            time.sleep(check_interval)
    return False


def archive_processed_file(file_path):
    try:
        if not os.path.exists(CONVERT_DIRECTORY):
            os.makedirs(CONVERT_DIRECTORY)
        
        base_name = os.path.basename(file_path)
        destination = os.path.join(CONVERT_DIRECTORY, base_name)

        if os.path.exists(destination):
            name, ext = os.path.splitext(base_name)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            destination = os.path.join(CONVERT_DIRECTORY, f"{name}_{timestamp}{ext}")

        shutil.move(file_path, destination)
        print(f"[{time.ctime()}] File cleanly archived to: {destination}")
    except Exception as e:
        print(f"[{time.ctime()}] Non-Fatal Error: Failed archiving file {file_path} -> {e}")


def save_cd_pmd_data_to_db(data):
    global db_pool
    conn = None
    try:
        if db_pool is None:
            init_db_pool()
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        db_date = format_db_date(data["date"]) if data["date"] else None

        # Value mapping representing the exact columns requested for the qc_entry_tmp summary update
        val_mapping = {
            "bobbin_no": data["fiber_id"],
            "zero_disp_wave": data["zero_disp_wave"],
            "slope_zero_disp": data["slope_zero_disp"],
            "disp_1550": data["disp_1550"],
            "disp_1285_1330": data["disp_1285_1330"],
            "disp_1270_1340": data["disp_1270_1340"],
            "disp_1270_1360": data["disp_1270_1360"],
            "disp_1575": data["disp_1575"],
            "cd_1460": data["cd_1460"],
            "disp_1460": data["disp_1460"],
            "disp_1490": data["disp_1490"],
            "disp_1625": data["disp_1625"],
            "disp_1570": data["disp_1570"],
            "disp_1260": data["disp_1260"],
            "pmd_1310": data["pmd_1310"],
            "pmd_1550": data["pmd_1550"],
            "disp_slope": data["disp_slope"],
            "slope_1550": data["slope_1550"],
            "slope_1290": data["slope_1290"],
            "slope_1490": data["slope_1490"]
        }

        columns = ["bobbin_fid"]
        placeholders = ["%s"]
        params = [data["fiber_id"]]
        set_clauses = []

        for col, val in val_mapping.items():
            if val is not None:
                columns.append(col)
                placeholders.append("%s")
                params.append(val)
                set_clauses.append(f"{col} = EXCLUDED.{col}")

        set_str = ", ".join(set_clauses) if set_clauses else "bobbin_fid = EXCLUDED.bobbin_fid"

        upsert_query = f"""
            INSERT INTO qc_entry_temp ({", ".join(columns)}) 
            VALUES ({", ".join(placeholders)})
            ON CONFLICT (bobbin_fid) 
            DO UPDATE SET {set_str};
        """
        cursor.execute(upsert_query, tuple(params))

        # --- Sub-Table History Insertion 1: f_pmd_history ---
        if data["pmd_records"]:
            insert_pmd_query = """
                INSERT INTO f_pmd_history (
                    bobbin_id, length, measurement_date, measurement_time, 
                    reported_wavelength, pmd, pmd_coefficient, gaussian_compliance, 
                    second_order_pmd, second_order_pmd_coeff
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            pmd_tuples = [
                (
                    data["fiber_id"], data["length"], db_date, data["time"],
                    record["wl"], record["pmd"], record["pmd_coeff"], 
                    record["gauss"], record["so_pmd"], record["so_pmd_coeff"]
                )
                for record in data["pmd_records"]
            ]
            cursor.executemany(insert_pmd_query, pmd_tuples)

        # --- Sub-Table History Insertion 2: f_cd_history ---
        if data["raw_cd_points"]:
            insert_cd_query = """
                INSERT INTO f_cd_history (
                    bobbin_id, length, measurement_date, measurement_time, 
                    wavelength, delay, dispersion, slope
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            cd_tuples = [
                (data["fiber_id"], data["length"], db_date, data["time"], wave, delay, disp, slope)
                for wave, delay, disp, slope in data["raw_cd_points"]
            ]
            cursor.executemany(insert_cd_query, cd_tuples)

        # --- Sub-Table History Insertion 3: f_length_history ---
        if data["length_measurements"]:
            insert_length_query = """
                INSERT INTO f_length_history (
                    bobbin_id, length, measurement_date, measurement_time,
                    measured_length, measured_time_us, wavelength
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            length_tuples = [
                (
                    data["fiber_id"], data["length"], db_date, data["time"],
                    record.get("measured_length"), record.get("measured_time_us"), record.get("wavelength")
                )
                for record in data["length_measurements"]
            ]
            cursor.executemany(insert_length_query, length_tuples)

        conn.commit()
        cursor.close()
        print(f"[{time.ctime()}] Parsed and saved CD/PMD Records for Bobbin FID '{data['fiber_id']}' cleanly.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[{time.ctime()}] Database Exception Error on CD/PMD Commit: {e}")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)


def process_csv(file_path):
    global PROCESSED_FILES
    current_time = time.time()

    if not os.path.exists(file_path):
        return
    if file_path in PROCESSED_FILES and (current_time - PROCESSED_FILES[file_path] < DUPLICATE_COOLDOWN_SECONDS):
        return

    if not wait_for_file_to_be_free(file_path):
        print(f"--- Timed out waiting for file release: {file_path}. Skipping. ---")
        return

    print(f"\n[{time.ctime()}] Safely processing file: {file_path}")
    PROCESSED_FILES[file_path] = time.time()

    try:
        # Structure payload initialization
        parsed_data = {
            "fiber_id": None, "length": None, "date": None, "time": None,
            "zero_disp_wave": None, "slope_zero_disp": None, "disp_1550": None,
            "disp_1285_1330": None, "disp_1270_1340": None, "disp_1270_1360": None, "disp_1575": None,
            "cd_1460": None, "disp_1460": None, "disp_1490": None, "disp_1625": None, "disp_1570": None, "disp_1260": None,
            "pmd_1310": None, "pmd_1550": None, "disp_slope": None,
            "slope_1550": None, "slope_1290": None, "slope_1490": None,
            "pmd_records": [], "raw_cd_points": [], "length_measurements": []
        }

        # Parsing states
        state = "HEADER"
        pmd_blocks = []
        length_measurement_temp = {}
        cd_wavelength_headers = False

        with open(file_path, "r", errors='ignore') as f:
            for line in f:
                # Standardize parsing split regardless of tab-delimited or comma structural logs
                parts = [p.strip() for p in line.replace("\t", ",").split(",")]
                if not parts or parts[0] == "":
                    continue

                key = parts[0].strip()
                key_lower = key.lower()

                # --- STATE TRANSITION CHECKS ---
                if "pmd interferometric" in key_lower:
                    state = "PMD"
                    continue
                elif "length measurement" in key_lower:
                    state = "LENGTH_MEASUREMENT"
                    length_measurement_temp = {}
                    continue
                elif "chromatic dispersion" in key_lower:
                    state = "CD"
                    continue
                elif "key wavelengths" in key_lower:
                    state = "KEY_WAVELENGTHS"
                    cd_wavelength_headers = True
                    continue

                # --- 1. HEADER SECTION PROCESSING ---
                if state == "HEADER":
                    if key_lower == "fiber id":
                        parsed_data["fiber_id"] = parts[1].strip().upper()
                    elif key_lower.startswith("length"):
                        parsed_data["length"] = safe_python_numeric(parts[1])
                    elif key_lower.startswith("time of measurement"):
                        dt = parts[1]
                        if " " in dt:
                            date_part, time_part = dt.split(" ", 1)
                            parsed_data["date"] = date_part
                            parsed_data["time"] = time_part
                        else:
                            parsed_data["date"] = dt

                # --- 2. INTERFEROMETRIC PMD PROCESSING ---
                elif state == "PMD":
                    # Build structured row list arrays to pivot dynamic horizontal indices
                    if key_lower.startswith("pmd (ps)"):
                        pmd_blocks.append({"pmd_w1": safe_python_numeric(parts[1]), "pmd_w2": safe_python_numeric(parts[2]) if len(parts) > 2 else None})
                    elif key_lower.startswith("pmd coefficient"):
                        pmd_blocks.append({"coeff_w1": safe_python_numeric(parts[1]), "coeff_w2": safe_python_numeric(parts[2]) if len(parts) > 2 else None})
                    elif key_lower.startswith("gaussian compliance"):
                        pmd_blocks.append({"gauss_w1": safe_python_numeric(parts[1]), "gauss_w2": safe_python_numeric(parts[2]) if len(parts) > 2 else None})
                    elif key_lower.startswith("second order pmd coefficient"):
                        pmd_blocks.append({"so_coeff_w1": safe_python_numeric(parts[1]), "so_coeff_w2": safe_python_numeric(parts[2]) if len(parts) > 2 else None})
                    elif key_lower.startswith("second order pmd"):
                        pmd_blocks.append({"so_w1": safe_python_numeric(parts[1]), "so_w2": safe_python_numeric(parts[2]) if len(parts) > 2 else None})
                    
                    elif key_lower.startswith("reported wavelength region"):
                        w1 = int(safe_python_numeric(parts[1]) or 1310)
                        w2 = int(safe_python_numeric(parts[2]) or 1550) if len(parts) > 2 and safe_python_numeric(parts[2]) else None
                        
                        # Extract metrics for column aggregation out of sequential list block dictionaries
                        p_vals = pmd_blocks[0] if len(pmd_blocks) > 0 else {}
                        c_vals = pmd_blocks[1] if len(pmd_blocks) > 1 else {}
                        g_vals = pmd_blocks[2] if len(pmd_blocks) > 2 else {}
                        s_vals = pmd_blocks[3] if len(pmd_blocks) > 3 else {}
                        sc_vals = pmd_blocks[4] if len(pmd_blocks) > 4 else {}

                        # Assign targeted root components
                        if w1 == 1310:
                            parsed_data["pmd_1310"] = p_vals.get("pmd_w1")
                        elif w1 == 1550:
                            parsed_data["pmd_1550"] = p_vals.get("pmd_w1")

                        parsed_data["pmd_records"].append({
                            "wl": w1, "pmd": p_vals.get("pmd_w1"), "pmd_coeff": c_vals.get("coeff_w1"),
                            "gauss": g_vals.get("gauss_w1"), "so_pmd": s_vals.get("so_w1"), "so_pmd_coeff": sc_vals.get("so_coeff_w1")
                        })

                        if w2:
                            if w2 == 1310:
                                parsed_data["pmd_1310"] = p_vals.get("pmd_w2")
                            elif w2 == 1550:
                                parsed_data["pmd_1550"] = p_vals.get("pmd_w2")

                            parsed_data["pmd_records"].append({
                                "wl": w2, "pmd": p_vals.get("pmd_w2"), "pmd_coeff": c_vals.get("coeff_w2"),
                                "gauss": g_vals.get("gauss_w2"), "so_pmd": s_vals.get("so_w2"), "so_pmd_coeff": sc_vals.get("so_coeff_w2")
                            })

                # --- 3. LENGTH MEASUREMENT PROCESSING ---
                elif state == "LENGTH_MEASUREMENT":
                    if key_lower.startswith("measurer type"):
                        # Ignore instrument name parameter as requested
                        continue
                    elif key_lower.startswith("length"):
                        length_measurement_temp["measured_length"] = safe_python_numeric(parts[1])
                    elif key_lower.startswith("time"):
                        length_measurement_temp["measured_time_us"] = safe_python_numeric(parts[1])
                    elif key_lower.startswith("wavelength"):
                        length_measurement_temp["wavelength"] = safe_python_numeric(parts[1])
                        # Wavelength is the last field in the block; close out this record
                        parsed_data["length_measurements"].append(dict(length_measurement_temp))
                        length_measurement_temp = {}

                # --- 4. CHROMATIC DISPERSION METRICS PROCESSING ---
                elif state == "CD":
                    if "slope at lambda zero" in key_lower:
                        parsed_data["slope_zero_disp"] = safe_python_numeric(parts[1])
                    elif "lambda zero" in key_lower:
                        parsed_data["zero_disp_wave"] = safe_python_numeric(parts[1])

                # --- 5. KEY WAVELENGTH MATRIX SCANNING ---
                elif state == "KEY_WAVELENGTHS":
                    if cd_wavelength_headers:
                        # Skip string column array descriptions
                        cd_wavelength_headers = False
                        continue
                    
                    w_num = safe_python_numeric(parts[0])
                    delay_num = safe_python_numeric(parts[1]) if len(parts) > 1 else None
                    disp_num = safe_python_numeric(parts[2]) if len(parts) > 2 else None
                    slope_num = safe_python_numeric(parts[3]) if len(parts) > 3 else None

                    if w_num is not None:
                        # Store detailed matrix rows to the history list collection
                        parsed_data["raw_cd_points"].append((w_num, delay_num, disp_num, slope_num))
                        
                        # Match targeted single check wavelengths
                        w_int = int(w_num)
                        if w_int == 1260:
                            parsed_data["disp_1260"] = disp_num
                        elif w_int == 1290:
                            parsed_data["slope_1290"] = slope_num
                        elif w_int == 1310:
                            parsed_data["disp_slope"] = slope_num  
                        elif w_int == 1460:
                            parsed_data["cd_1460"] = disp_num
                            parsed_data["disp_1460"] = disp_num
                        elif w_int == 1490:
                            parsed_data["disp_1490"] = disp_num
                            parsed_data["slope_1490"] = slope_num
                        elif w_int == 1550:
                            parsed_data["disp_1550"] = disp_num
                            parsed_data["slope_1550"] = slope_num
                        elif w_int == 1570:
                            parsed_data["disp_1570"] = disp_num
                        elif w_int == 1575:
                            parsed_data["disp_1575"] = disp_num
                        elif w_int == 1625:
                            parsed_data["disp_1625"] = disp_num

        # --- POST-PROCESSING CRITICAL MAX VALUE EVALUATIONS ---
        max_1285_1330 = -999999.0
        max_1270_1340 = -999999.0
        max_1270_1360 = -999999.0
        
        for w_num, _, disp_num, _ in parsed_data["raw_cd_points"]:
            if disp_num is not None:
                if 1285 <= w_num <= 1330:
                    if disp_num > max_1285_1330:
                        max_1285_1330 = disp_num
                if 1270 <= w_num <= 1340:
                    if disp_num > max_1270_1340:
                        max_1270_1340 = disp_num
                if 1270 <= w_num <= 1360:
                    if disp_num > max_1270_1360:
                        max_1270_1360 = disp_num

        if max_1285_1330 != -999999.0:
            parsed_data["disp_1285_1330"] = max_1285_1330
        if max_1270_1340 != -999999.0:
            parsed_data["disp_1270_1340"] = max_1270_1340
        if max_1270_1360 != -999999.0:
            parsed_data["disp_1270_1360"] = max_1270_1360

        # Push to core transactional database handler if identity data fields exist
        if parsed_data["fiber_id"]:
            save_cd_pmd_data_to_db(parsed_data)
        else:
            print(f"Skipped file: No Valid Fiber ID structural string discovered.")

    except Exception as e:
        print(f"[{time.ctime()}] Gracefully Handled Parser Error on file {file_path}: {e}")
    finally:
        archive_processed_file(file_path)


def scan_and_process_existing_files():
    print(f"[{time.ctime()}] Executing initial startup directory sweep inside: {WATCH_DIRECTORY}")
    try:
        for item in os.listdir(WATCH_DIRECTORY):
            item_path = os.path.join(WATCH_DIRECTORY, item)
            if os.path.isfile(item_path) and item.lower().endswith('.csv'):
                process_csv(item_path)
    except Exception as e:
        print(f"[{time.ctime()}] Gracefully Handled Exception during directory sweep: {e}")


class CSVWatchHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            if 'convert' not in event.src_path:
                process_csv(event.src_path)
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            if 'convert' not in event.src_path:
                process_csv(event.src_path)


if __name__ == "__main__":
    # --- NETWORK DRIVE RECOVERY LOOP ---
    # Continuously checks if the network path is available before spinning up the Watchdog engine.
    while True:
        if os.path.exists(WATCH_DIRECTORY):
            print(f"[{time.ctime()}] Target watch directory found and validated: {WATCH_DIRECTORY}")
            break
        else:
            print(f"[{time.ctime()}] Mapped drive not ready yet. Retrying in 10 seconds...")
            time.sleep(10)

    # Ensure internal archive path is available
    if not os.path.exists(CONVERT_DIRECTORY):
        try:
            os.makedirs(CONVERT_DIRECTORY)
        except Exception as e:
            print(f"[{time.ctime()}] Warning creating convert folder: {e}")

    # Process outstanding backlogs
    scan_and_process_existing_files()

    event_handler = CSVWatchHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_DIRECTORY, recursive=False)
    
    print(f"[{time.ctime()}] Headless Monitor Service Live & Online.")
    observer.start()

    try:
        while True:
            time.sleep(1)
            
            # Additional safety: If the drive disconnects mid-operation, prevent loop explosion
            if not os.path.exists(WATCH_DIRECTORY):
                print(f"[{time.ctime()}] Warning: Mapped drive went offline during runtime.")
                
            now = time.time()
            PROCESSED_FILES = {k: v for k, v in PROCESSED_FILES.items() if now - v < 60}
    except Exception as e:
        print(f"[{time.ctime()}] Service experienced an execution exception: {e}")
    finally:
        if db_pool:
            db_pool.closeall()
        observer.stop()
        observer.join()
