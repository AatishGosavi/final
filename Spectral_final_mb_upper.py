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
config_path = os.path.join(script_dir, 'conf.ini')

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
        return float(clean_str) if '.' in clean_str else int(clean_str)
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


def save_spectral_data_to_db(data):
    global db_pool
    conn = None
    try:
        if db_pool is None:
            init_db_pool()
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        db_date = format_db_date(data["date"]) if data["date"] else None

        # qc_entry_temp always gets bobbin_fid / bobbin_no in upper case
        fid_upper = data["fiber_id"].strip().upper() if data["fiber_id"] else data["fiber_id"]

        val_mapping = {
            "bobbin_no": fid_upper,
            "spec_1310": data["spec_1310"],
            "spec_1550": data["spec_1550"],
            "spec_1285_1330": data["spec_1285_1330"]
        }

        columns = ["bobbin_fid"]
        placeholders = ["%s"]
        params = [fid_upper]
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

        if data["raw_spectral_points"]:
            insert_history_query = """
                INSERT INTO f_spectral_history (
                    bobbin_id, length, measurement_date, measurement_time, location, wavelength, attenuation, Operator
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            history_tuples = [
                (data["fiber_id"], data["length"], db_date, data["time"], data["location"], wave, atten, data["opr"])
                for wave, atten in data["raw_spectral_points"]
            ]
            cursor.executemany(insert_history_query, history_tuples)

        conn.commit()
        cursor.close()
        print(f"[{time.ctime()}] Saved Spectral File for Bobbin FID '{data['fiber_id']}' cleanly.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[{time.ctime()}] Database Exception Error on Spectral Commit: {e}")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)


def save_data_to_db(data):
    global db_pool
    conn = None
    try:
        if db_pool is None:
            init_db_pool()
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        db_date = format_db_date(data["date"]) if data["date"] else None
        is_bot = (str(data["position"]).strip().upper() == "BOT")
        suffix = "_bottom" if is_bot else "_top"

        # qc_entry_temp always gets bobbin_fid / bobbin_no in upper case
        fid_upper = data["fiber_id"].strip().upper() if data["fiber_id"] else data["fiber_id"]

        mfd_1310 = data["mfd_records"].get("1310", {"wavelength": None, "gaussian": None, "petermann": None, "area": None})
        mfd_1550 = data["mfd_records"].get("1550", {"wavelength": None, "gaussian": None, "petermann": None, "area": None})

        val_mapping = {
            "bobbin_no": fid_upper,
            f"core_dia{suffix}": data["core_25_diameter"],
            f"core_ovality{suffix}": data["core_non_circularity"],
            f"core_clad_concentricity{suffix}": data["core_concentricity"],
            f"clad_dia{suffix}": data["cladding_dia"],
            f"clad_ovality{suffix}": data["cladding_non_circularity"],
            f"cut_off{suffix}": data["cutoff_wavelength"],
            f"mfd_1310{suffix}": mfd_1310["petermann"],
            f"effective_area_1310": mfd_1310["area"] if suffix == "_top" else None,
            f"mfd_1550{suffix}": mfd_1550["petermann"],
            f"effective_area_1550": mfd_1550["area"] if suffix == "_top" else None,
            f"secondary_coating_dia{suffix}": data["coating_outer_dia"],
            f"secondary_coating_concentricity{suffix}": data["coating_outer_concentricity"],
            f"coating_ovality{suffix}": data["coating_outer_non_circularity"],
            f"primary_coating_dia{suffix}": data["coating_inner_dia"],
            f"primary_coating_concentricity{suffix}": data["coating_inner_concentricity"],
            f"fiber_curl{suffix}": data["fiber_curl"]
        }
        
        if suffix == "_bottom":
            val_mapping["effective_area_1310"] = mfd_1310["area"]
            val_mapping["effective_area_1550"] = mfd_1550["area"]

        columns = ["bobbin_fid"]
        placeholders = ["%s"]
        params = [fid_upper]
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

        # --- Sub-table Insertions ---
        if data["core_25_diameter"] or data["cladding_dia"]:
            cursor.execute("""
                INSERT INTO f_geometry_history (
                    bobbin_id, length, measurement_date, measurement_time, Operator,
                    core_dia_top, core_ovality_top, core_clad_concentricity_top, clad_dia_top, clad_ovality_top,
                    core_dia_bottom, core_ovality_bottom, core_clad_concentricity_bottom, clad_dia_bottom, clad_ovality_bottom
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
                None if is_bot else data["core_25_diameter"], None if is_bot else data["core_non_circularity"], None if is_bot else data["core_concentricity"], None if is_bot else data["cladding_dia"], None if is_bot else data["cladding_non_circularity"],
                data["core_25_diameter"] if is_bot else None, data["core_non_circularity"] if is_bot else None, data["core_concentricity"] if is_bot else None, data["cladding_dia"] if is_bot else None, data["cladding_non_circularity"] if is_bot else None
            ))
            
        if data["cutoff_wavelength"]:
            cursor.execute("""
                INSERT INTO f_cutoff_history (fiber_id, length, measurement_date, measurement_time, Operator, cut_off_top, cut_off_bottom)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
                None if is_bot else data["cutoff_wavelength"], data["cutoff_wavelength"] if is_bot else None
            ))
            
        if data["mfd_records"]:
            cursor.execute("""
                INSERT INTO f_mfd_history (
                    fiber_id, length, measurement_date, measurement_time, Operator,
                    mfd_wavelength_top_1310, gaussian_mfd_top_1310, mfd_1310_top, effective_area_1310,
                    mfd_wavelength_bottom_1310, gaussian_mfd_bottom_1310, mfd_1310_bottom, effective_area_bottom_1310,
                    mfd_wavelength_top_1550, gaussian_mfd_top_1550, mfd_1550_top, effective_area_1550,
                    mfd_wavelength_bottom_1550, gaussian_mfd_bottom_1550, mfd_1550_bottom, effective_area_bottom_1550
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
                None if is_bot else mfd_1310["wavelength"], None if is_bot else mfd_1310["gaussian"], None if is_bot else mfd_1310["petermann"], None if is_bot else mfd_1310["area"],
                mfd_1310["wavelength"] if is_bot else None, mfd_1310["gaussian"] if is_bot else None, mfd_1310["petermann"] if is_bot else None, mfd_1310["area"] if is_bot else None,
                None if is_bot else mfd_1550["wavelength"], None if is_bot else mfd_1550["gaussian"], None if is_bot else mfd_1550["petermann"], None if is_bot else mfd_1550["area"],
                mfd_1550["wavelength"] if is_bot else None, mfd_1550["gaussian"] if is_bot else None, mfd_1550["petermann"] if is_bot else None, mfd_1550["area"] if is_bot else None
            ))
            
        if data["coating_outer_dia"] or data["coating_inner_dia"]:
            cursor.execute("""
                INSERT INTO f_coating_history (
                    fiber_id, length, measurement_date, measurement_time, Operator,
                    secondary_coating_dia_top, secondary_coating_concentricity_top, coating_ovality_top, primary_coating_dia_top, primary_coating_concentricity_top,
                    secondary_coating_dia_bottom, secondary_coating_concentricity_bottom, coating_ovality_bottom, primary_coating_dia_bottom, primary_coating_concentricity_bottom
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
                None if is_bot else data["coating_outer_dia"], None if is_bot else data["coating_outer_concentricity"], None if is_bot else data["coating_outer_non_circularity"], None if is_bot else data["coating_inner_dia"], None if is_bot else data["coating_inner_concentricity"],
                data["coating_outer_dia"] if is_bot else None, data["coating_outer_concentricity"] if is_bot else None, data["coating_outer_non_circularity"] if is_bot else None, data["coating_inner_dia"] if is_bot else None, data["coating_inner_concentricity"] if is_bot else None
            ))
            
        if data["fiber_curl"]:
            cursor.execute("""
                INSERT INTO f_curl_history (fiber_id, length, measurement_date, measurement_time, Operator, fiber_curl_top, fiber_curl_bottom)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
                None if is_bot else data["fiber_curl"], data["fiber_curl"] if is_bot else None
            ))

        conn.commit()
        cursor.close()
        print(f"[{time.ctime()}] Saved Bobbin FID '{data['fiber_id']}' [{data['position']}] successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[{time.ctime()}] Database Exception Error on Standard Commit: {e}")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)


def save_cable_cutoff_to_db(data):
    """
    Handles the new 'Cable Cut-Off' file type.

    Rule:
      - CABLECUTOFF == YES  -> store the cut-off wavelength value in qc_entry_temp.cable_cut_off
      - CABLECUTOFF == NO   -> store the cut-off wavelength value in qc_entry_temp.fiber_cut_off_top

    Also writes a full audit row to the new f_cable_cable_cutoff log table.
    """
    global db_pool
    conn = None
    try:
        if db_pool is None:
            init_db_pool()
        conn = db_pool.getconn()
        cursor = conn.cursor()

        db_date = format_db_date(data["date"]) if data["date"] else None
        flag = (data["cable_cutoff_flag"] or "").strip().upper()

        # qc_entry_temp always gets bobbin_fid / bobbin_no in upper case
        fid_upper = data["fiber_id"].strip().upper() if data["fiber_id"] else data["fiber_id"]

        target_column = None
        if flag == "YES":
            target_column = "cable_cut_off"
        elif flag == "NO":
            target_column = "fiber_cut_off_top"

        val_mapping = {
            "bobbin_no": fid_upper
        }
        if target_column and data["cutoff_wavelength"] is not None:
            val_mapping[target_column] = data["cutoff_wavelength"]

        columns = ["bobbin_fid"]
        placeholders = ["%s"]
        params = [fid_upper]
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

        # --- Audit log insertion ---
        cursor.execute("""
            INSERT INTO f_cable_cable_cutoff (
                fiber_id, length, measurement_date, measurement_time, Operator,
                cable_cutoff_flag, cutoff_wavelength, target_column
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            data["fiber_id"], data["length"], db_date, data["time"], data["opr"],
            flag if flag else None, data["cutoff_wavelength"], target_column
        ))

        conn.commit()
        cursor.close()

        if target_column is None:
            print(f"[{time.ctime()}] Warning: Unrecognized CABLECUTOFF value '{data['cable_cutoff_flag']}' for Bobbin FID '{data['fiber_id']}'. Logged, but qc_entry_temp value column was not updated.")
        else:
            print(f"[{time.ctime()}] Saved Cable Cut-Off File for Bobbin FID '{data['fiber_id']}' cleanly (column: {target_column}).")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[{time.ctime()}] Database Exception Error on Cable Cut-Off Commit: {e}")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)


def save_mbend_data_to_db(data):
    """
    Handles the new 'MBend' (Macrobend loss) file type.

    The file may contain multiple test rows (different Turn / Mandrel Diameter / Sample
    length combinations) sharing the same set of wavelength columns (e.g. 1310, 1550, 1625).

    qc_entry_temp has one dedicated column per (Turn, Mandrel Diameter, Wavelength)
    combination, named using the convention:
        m_<turns>t_<mandrel_diameter>mm_<wavelength>
    e.g. Turn=1, Mandrel=32mm, Wavelength=1310  ->  m_1t_32mm_1310

    Each test row's actual attenuation value is upserted directly into its matching
    column using the same Insert/Update (ON CONFLICT) logic used elsewhere in this file.

    Every individual row/wavelength reading is also preserved in full in the new
    f_mbend_history log table.
    """
    global db_pool
    conn = None
    try:
        if db_pool is None:
            init_db_pool()
        conn = db_pool.getconn()
        cursor = conn.cursor()

        db_date = format_db_date(data["date"]) if data["date"] else None

        # qc_entry_temp always gets bobbin_fid / bobbin_no in upper case
        fid_upper = data["fiber_id"].strip().upper() if data["fiber_id"] else data["fiber_id"]

        def _fmt_num_for_colname(num):
            # Renders whole-number floats/ints without a decimal point (32.0 -> "32"),
            # matching the qc_entry_temp column naming convention (e.g. m_1t_32mm_1310).
            if num is None:
                return None
            if float(num) == int(num):
                return str(int(num))
            return str(num).replace(".", "_")

        # --- Build one qc_entry_temp column per (Turn, Mandrel, Wavelength) test condition ---
        val_mapping = {
            "bobbin_no": fid_upper
        }
        for row in data["rows"]:
            turn_str = _fmt_num_for_colname(row["turn"])
            mandrel_str = _fmt_num_for_colname(row["mandrel"])
            if turn_str is None or mandrel_str is None:
                continue
            for wl, val in row["values"].items():
                if val is not None:
                    wl_str = _fmt_num_for_colname(safe_python_numeric(wl)) or str(wl).strip()
                    col_name = f"m_{turn_str}t_{mandrel_str}mm_{wl_str}"
                    val_mapping[col_name] = val

        columns = ["bobbin_fid"]
        placeholders = ["%s"]
        params = [fid_upper]
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

        # --- Full-detail history insertion (one row per test condition / wavelength) ---
        if data["rows"]:
            insert_history_query = """
                INSERT INTO f_mbend_history (
                    fiber_id, measurement_date, measurement_time,
                    -- Operator,  -- commented out for now: MBend file has no OPID field yet.
                    -- Uncomment above (and the qc_entry_temp column, once added) when ready.
                    sample_type, turn, mandrel_diameter, sample_length, wavelength, attenuation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            history_tuples = []
            for row in data["rows"]:
                for wl, val in row["values"].items():
                    if val is not None:
                        history_tuples.append((
                            data["fiber_id"], db_date, data["time"],
                            # data["opr"],  -- commented out for now, see note above
                            row["sample_type"], row["turn"], row["mandrel"], row["sample_length"],
                            safe_python_numeric(wl), val
                        ))
            if history_tuples:
                cursor.executemany(insert_history_query, history_tuples)

        conn.commit()
        cursor.close()
        print(f"[{time.ctime()}] Saved MBend File for Bobbin FID '{data['fiber_id']}' cleanly.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[{time.ctime()}] Database Exception Error on MBend Commit: {e}")
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
        is_spectral_file = False
        is_cable_cutoff_file = False
        is_mbend_file = False
        with open(file_path, "r", errors='ignore') as test_f:
            for _ in range(15):
                line = test_f.readline()
                if not line:
                    break
                low_line = line.replace("\t", ",").lower()
                if "spectral attenuation" in low_line:
                    is_spectral_file = True
                    break
                if "cablecutoff" in low_line:
                    is_cable_cutoff_file = True
                    break
                if "macrobend loss" in low_line:
                    is_mbend_file = True
                    break

        # --- ROUTE A: SPECTRAL ATTENUATION PROCESSING ---
        if is_spectral_file:
            spectral_data = {
                "fiber_id": None, "length": None, "date": None, "time": None, "location": None, "opr": None,
                "spec_1310": None, "spec_1550": None, "spec_1285_1330": None, "raw_spectral_points": []
            }
            current_section = None
            max_1285_1330_val = -1.0

            with open(file_path, "r", errors='ignore') as f:
                for line in f:
                    parts = [p.strip() for p in line.replace("\t", ",").split(",")]
                    if len(parts) == 0 or parts[0] == "":
                        continue

                    key = parts[0].strip().lower()
                    if key == "spectral attenuation":
                        current_section = "spectral"
                        continue

                    if key == "fiber id":
                        spectral_data["fiber_id"] = parts[1]
                    elif key == "location":
                        spectral_data["location"] = parts[1].strip().upper()
                    elif key == "operator":
                        spectral_data["opr"] = parts[1]
                    elif key.startswith("length"):
                        spectral_data["length"] = safe_python_numeric(parts[1])
                    elif key.startswith("time of measurement"):
                        dt = parts[1]
                        if " " in dt:
                            date_part, time_part = dt.split(" ", 1)
                            spectral_data["date"] = date_part
                            spectral_data["time"] = time_part
                        else:
                            spectral_data["date"] = dt

                    elif current_section == "spectral":
                        w_num = safe_python_numeric(parts[0])
                        a_num = safe_python_numeric(parts[1]) if len(parts) > 1 else None

                        if w_num is not None and a_num is not None:
                            spectral_data["raw_spectral_points"].append((w_num, a_num))
                            if int(w_num) == 1310:
                                spectral_data["spec_1310"] = a_num
                            if int(w_num) == 1550:
                                spectral_data["spec_1550"] = a_num
                            if 1285 <= w_num <= 1330:
                                if a_num > max_1285_1330_val:
                                    max_1285_1330_val = a_num

            if max_1285_1330_val >= 0:
                spectral_data["spec_1285_1330"] = max_1285_1330_val

            if spectral_data["fiber_id"]:
                save_spectral_data_to_db(spectral_data)
            else:
                print(f"Skipped file: No Fiber ID found.")

        # --- ROUTE C: CABLE CUT-OFF FILE PROCESSING ---
        elif is_cable_cutoff_file:
            cutoff_data = {
                "fiber_id": None, "length": None, "date": None, "time": None, "opr": None,
                "cable_cutoff_flag": None, "cutoff_wavelength": None
            }
            current_section = None

            with open(file_path, "r", errors='ignore') as f:
                for line in f:
                    parts = [p.strip() for p in line.replace("\t", ",").split(",")]
                    if len(parts) == 0 or parts[0] == "":
                        continue

                    key = parts[0].strip().lower()
                    if key == "cut-off wavelength":
                        current_section = "cutoff"
                        continue

                    if key == "fiber id":
                        cutoff_data["fiber_id"] = parts[1]
                    elif key == "opid":
                        cutoff_data["opr"] = parts[1]
                    elif key == "cablecutoff":
                        cutoff_data["cable_cutoff_flag"] = parts[1].strip().upper() if len(parts) > 1 else None
                    elif key.startswith("length"):
                        cutoff_data["length"] = safe_python_numeric(parts[1])
                    elif key.startswith("time of measurement"):
                        dt = parts[1]
                        if " " in dt:
                            date_part, time_part = dt.split(" ", 1)
                            cutoff_data["date"] = date_part
                            cutoff_data["time"] = time_part
                        else:
                            cutoff_data["date"] = dt
                    elif current_section == "cutoff":
                        if key.startswith("cut-off wavelength"):
                            cutoff_data["cutoff_wavelength"] = safe_python_numeric(parts[1])

            if cutoff_data["fiber_id"]:
                save_cable_cutoff_to_db(cutoff_data)
            else:
                print(f"Skipped file: No Fiber ID found.")

        # --- ROUTE D: MBEND (MACROBEND LOSS) FILE PROCESSING ---
        elif is_mbend_file:
            mbend_data = {
                "fiber_id": None, "date": None, "time": None,
                "wavelength_cols": [], "rows": []
            }
            current_section = None

            with open(file_path, "r", errors='ignore') as f:
                for line in f:
                    parts = [p.strip() for p in line.replace("\t", ",").split(",")]
                    if len(parts) == 0 or parts[0] == "":
                        continue

                    key = parts[0].strip().lower()
                    if key == "macrobend loss":
                        current_section = "mbend"
                        continue

                    if key == "fiber id":
                        mbend_data["fiber_id"] = parts[1]
                        continue
                    elif key.startswith("time of measurement"):
                        dt = parts[1]
                        if " " in dt:
                            date_part, time_part = dt.split(" ", 1)
                            mbend_data["date"] = date_part
                            mbend_data["time"] = time_part
                        else:
                            mbend_data["date"] = dt
                        continue

                    if current_section == "mbend":
                        if key == "instrument sn":
                            continue
                        if key == "sample type":
                            # Header row: Sample Type, Turn (N), Mandrel Diameter (mm), Sample length (m), <wavelengths...>
                            mbend_data["wavelength_cols"] = [p.strip() for p in parts[4:] if p.strip() != ""]
                            continue
                        if mbend_data["wavelength_cols"]:
                            sample_type = parts[0]
                            turn = safe_python_numeric(parts[1]) if len(parts) > 1 else None
                            mandrel = safe_python_numeric(parts[2]) if len(parts) > 2 else None
                            sample_len = safe_python_numeric(parts[3]) if len(parts) > 3 else None
                            values = {}
                            for i, wl in enumerate(mbend_data["wavelength_cols"]):
                                idx = 4 + i
                                if idx < len(parts):
                                    values[wl] = safe_python_numeric(parts[idx])
                            mbend_data["rows"].append({
                                "sample_type": sample_type, "turn": turn, "mandrel": mandrel,
                                "sample_length": sample_len, "values": values
                            })

            if mbend_data["fiber_id"]:
                save_mbend_data_to_db(mbend_data)
            else:
                print(f"Skipped file: No Fiber ID found.")

        # --- ROUTE B: ORIGINAL GEOMETRY / MEASUREMENT PROCESSING ---
        else:
            row_data = {
                "fiber_id": None, "length": None, "date": None, "time": None, "position": "TOP", "opr": None,
                "core_25_diameter": None, "core_non_circularity": None, "core_concentricity": None,
                "cladding_dia": None, "cladding_non_circularity": None, "cutoff_wavelength": None,
                "coating_outer_dia": None, "coating_outer_concentricity": None, "coating_outer_non_circularity": None,
                "coating_inner_dia": None, "coating_inner_concentricity": None, "coating_inner_non_circularity": None,
                "coating_fiber_dia": None, "coating_fiber_concentricity": None, "coating_fiber_non_circularity": None,
                "fiber_curl": None, "mfd_records": {}
            }
            current_section = None

            with open(file_path, "r", errors='ignore') as f:
                for line in f:
                    parts = [p.strip() for p in line.replace("\t", ",").split(",")]
                    if len(parts) == 0 or parts[0] == "":
                        continue

                    key = parts[0].strip().lower()
                    if key == "fiber geometry":
                        current_section = "geometry"
                        continue
                    elif key == "cut-off wavelength":
                        current_section = "cutoff"
                        continue
                    elif key == "mode field diameter":
                        current_section = "mfd"
                        continue
                    elif key == "coating geometry":
                        current_section = "coating"
                        continue
                    elif key == "fiber curl":
                        current_section = "curl"
                        continue

                    if key == "fiber id":
                        row_data["fiber_id"] = parts[1]
                    elif key == "top/bot":
                        row_data["position"] = parts[1].strip().upper()
                    elif key == "opid":
                        row_data["opr"] = parts[1]
                    elif key.startswith("length"):
                        row_data["length"] = safe_python_numeric(parts[1])
                    elif key.startswith("time of measurement"):
                        dt = parts[1]
                        if " " in dt:
                            date_part, time_part = dt.split(" ", 1)
                            row_data["date"] = date_part
                            row_data["time"] = time_part
                        else:
                            row_data["date"] = dt

                    elif current_section == "geometry":
                        if key.startswith("core"):
                            row_data["core_25_diameter"] = safe_python_numeric(parts[1])
                            row_data["core_non_circularity"] = safe_python_numeric(parts[2])
                            row_data["core_concentricity"] = safe_python_numeric(parts[3])
                        elif key.startswith("cladding"):
                            row_data["cladding_dia"] = safe_python_numeric(parts[1])
                            row_data["cladding_non_circularity"] = safe_python_numeric(parts[2])

                    elif current_section == "cutoff":
                        if key.startswith("cut-off wavelength"):
                            row_data["cutoff_wavelength"] = safe_python_numeric(parts[1])

                    elif current_section == "mfd":
                        if key.isdigit():
                            row_data["mfd_records"][key] = {
                                "wavelength": safe_python_numeric(parts[0]),
                                "gaussian": safe_python_numeric(parts[1]),
                                "petermann": safe_python_numeric(parts[2]),
                                "area": safe_python_numeric(parts[3])
                            }

                    elif current_section == "coating":
                        if key == "fiber":
                            row_data["coating_fiber_dia"] = safe_python_numeric(parts[1])
                            row_data["coating_fiber_concentricity"] = safe_python_numeric(parts[2])
                            row_data["coating_fiber_non_circularity"] = safe_python_numeric(parts[3])
                        elif key == "inner":
                            row_data["coating_inner_dia"] = safe_python_numeric(parts[1])
                            row_data["coating_inner_concentricity"] = safe_python_numeric(parts[2])
                            row_data["coating_inner_non_circularity"] = safe_python_numeric(parts[3])
                        elif key == "outer":
                            row_data["coating_outer_dia"] = safe_python_numeric(parts[1])
                            row_data["coating_outer_concentricity"] = safe_python_numeric(parts[2])
                            row_data["coating_outer_non_circularity"] = safe_python_numeric(parts[3])

                    elif current_section == "curl":
                        if key.startswith("fiber curl"):
                            row_data["fiber_curl"] = safe_python_numeric(parts[1])

            if row_data["fiber_id"]:
                save_data_to_db(row_data)
            else:
                print(f"Skipped file: No Fiber ID found.")

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
