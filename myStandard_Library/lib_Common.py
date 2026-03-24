import os
import sys
import yaml
import dotenv
import time
import datetime
import csv
import psutil
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from lib_ContextLogger import ContextLogger


# -----------------------------------------------------------------------------------
# Get location of current .py/.exe file
# -----------------------------------------------------------------------------------
def get_current_dir() -> Path:
    if getattr(sys, "frozen", False):
        current_dir = Path(sys.executable).parent  # next to the .exe
    else:
        current_dir = Path(sys.argv[0]).resolve().parent  # normal script run
    print(f"Current working directory: \"{current_dir}\".")
    return current_dir


# -----------------------------------------------------------------------------------
# Get program name of current .py/.exe file
# -----------------------------------------------------------------------------------
# def get_current_program_name() -> str:
#     if getattr(sys, "frozen", False):
#         current_program_name = Path(sys.executable).name  # next to the .exe
#     else:
#         current_program_name = Path(__file__).resolve().name  # normal script run
#     print(f"Current program name: {current_program_name}")
#     return current_program_name

def get_current_program_name() -> str:
    if getattr(sys, "frozen", False):
        current_program_name = Path(sys.executable).name
    else:
        if sys.argv and sys.argv[0]:
            current_program_name = Path(sys.argv[0]).name
        else:
            current_program_name = "<interactive>"

    print(f"Current program name: {current_program_name}")
    return current_program_name



# -----------------------------------------------------------------------------------
# Check config type validity
# -----------------------------------------------------------------------------------
def check_config_type(config_type: str, logger: ContextLogger | None = None) -> bool:
    # guard line
    if config_type not in ["env", "yaml"]:
        if logger:
            logger.error2("Initialize", f"Invalid config type: {config_type}. Must be 'env' or 'yaml'.")
        else:
            print(f"Invalid config type: {config_type}. Must be 'env' or 'yaml'.")
        time.sleep(5)
        sys.exit(1)
    else:
        return True


# -----------------------------------------------------------------------------------
# Load .env file into OS environment variables
# -----------------------------------------------------------------------------------
def load_dotenv_file(env_dir: Path, name_with_suffix: str = ".env") -> Path:
    print ("Loading .env configuration...")

    # Load environment variables from .env file if present
    dotenv_filepath = env_dir / name_with_suffix
    
    if dotenv_filepath.exists():    
        dotenv.load_dotenv(dotenv_filepath)
        print(f"{dotenv_filepath} loaded successfully!")
        return dotenv_filepath
    else:
        print(f"{dotenv_filepath} not found.")
        time.sleep(5)
        sys.exit(1)


# -----------------------------------------------------------------------------------
# Load environment variables from system
# -----------------------------------------------------------------------------------
def os_get_env(variable_name: str, logger: ContextLogger | None = None):
    variable = os.getenv(variable_name)
    if variable is not None:
        if logger is not None:
            logger.info2("Dotenv", f"Loaded -> {variable_name} : {variable}")
        else:
            print(f"Loaded -> {variable_name} : {variable}")
        return variable
    else:
        if logger is not None:
            logger.error2("Dotenv", f"{variable_name} not found in OS variables.")
        else:
            print(f"{variable_name} not found in OS variables.")
        time.sleep(5)
        sys.exit()
        return 


# -----------------------------------------------------------------------------------
# Load .yaml file into memory
# -----------------------------------------------------------------------------------
def load_yaml_file(yaml_dir: Path | None = None, name_with_suffix: str = "config.yaml") -> dict:
    print ("Loading .yaml configuration...")
   
    # Load environment variables from .yaml file
    if not yaml_dir:
        yaml_dir = get_current_dir()
    yaml_filepath = yaml_dir / name_with_suffix
    
    if yaml_filepath.exists():    
        with open(yaml_filepath, "r", encoding="utf-8") as file:
            yaml_config = yaml.safe_load(file)
        print(f"{yaml_filepath} loaded successfully!")
        return yaml_config
    else:
        print(f"{yaml_filepath} not found.")
        time.sleep(5)
        sys.exit(1)

# YAML_CONFIG = load_yaml_file()


# -----------------------------------------------------------------------------------
# Load yaml variables from nested dict
# -----------------------------------------------------------------------------------
def yaml_get_var(config: dict, *keys, logger: ContextLogger | None = None):
    config_dict = config
    key_path = []

    for k in keys:
        key_path.append(k)
        if not isinstance(config_dict, dict):
            if logger is not None:
                logger.error2("yaml", f"Invalid config structure at {'.'.join(key_path[:-1])}")
            else:
                print(f"Invalid config structure at {'.'.join(key_path[:-1])}")
            raise KeyError(f"Invalid config structure at {'.'.join(key_path[:-1])}")
        if k not in config_dict:
            if logger is not None:
                logger.error2("yaml", f"Config key not found: {'.'.join(key_path)}")
            else:
                print(f"Config key not found: {'.'.join(key_path)}")
            raise KeyError(f"Config key not found: {'.'.join(key_path)}")
        config_dict = config_dict[k]
        config_value = config_dict
    
    if logger is not None:
        logger.info2("yaml", f"Loaded -> {key_path} : {config_value}")
    else:
        print(f"Loaded -> {key_path} : {config_value}")
    return config_value


# -----------------------------------------------------------------------------------
# Initialize Main Context Logger 
# -----------------------------------------------------------------------------------
def init_main_logger(code_dir: Path, 
                     file_prefix_overwrite: str | None = None, 
                     config_type: str = "env", 
                     yaml_config: dict | None = None,
                     context: str = "Main") -> ContextLogger:
    
    check_config_type(config_type)
    # create logs directory
    logs_dir = Path(code_dir, "logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    # get env variable 
    if not file_prefix_overwrite:
        if config_type == "env":
            file_prefix = os_get_env('FILE_PREFIX') 
        elif config_type == "yaml":
            if not yaml_config:
                print("YAML config must be provided when config_type is 'yaml'.")
                time.sleep(5)
                sys.exit(1)
            file_prefix = yaml_config['GLOBAL']['GENERAL']['FILE_PREFIX']
    else: 
        file_prefix = file_prefix_overwrite

    if config_type == "env":
        log_level = os_get_env('LOG_LEVEL')
    elif config_type == "yaml":
        if not yaml_config:
            print("YAML config must be provided when config_type is 'yaml'.")
            time.sleep(5)
            sys.exit(1)
        log_level = yaml_config['GLOBAL']['GENERAL']['LOG_LEVEL']

    # Instantiate the ContextLogger implementation with daily rotation
    logger = ContextLogger(name=file_prefix, log_dir=str(logs_dir), context=context,
                                console_log_level=log_level, file_log_level=log_level)
    logger.info2("Initialize", "Main logger initialized.")
    logger.info2("Initialize", f"Log files saved in \"{logs_dir}\".")
    return logger


# -----------------------------------------------------------------------------------
# Function to save single line string data to CSV
# -----------------------------------------------------------------------------------
def save_to_csv(data: str, 
                file_dir: Path, 
                file_name: str, 
                header_list: list, 
                logger: ContextLogger, 
                with_date: bool = True) -> None:
    """
    Save data to a CSV file.

    Parameters:
    - data: Data to save.
    - file_path: The Path to the CSV file where data will be appended.
    - logger: ContextLogger instance for logging.
    """
    # construct file path, with or without date
    if with_date:
        date_suffix = datetime.datetime.now().strftime("%Y%m%d")  # Get current date and time
        file_path = file_dir / f"{file_name}_{date_suffix}.csv"
    else:
        file_path = file_dir / f"{file_name}.csv"

    # create folder if doesnt exist
    if not file_dir.is_dir():
        os.makedirs(file_dir, exist_ok=True) 

    # create file and header, if doesnt exist
    if not file_path.exists():
        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            header = header_list
            writer.writerow(header)  # header
            logger.info2("Data", f"Created new CSV file at \"{file_path}\".")
    
    # write 
    try:
        with open(file_path, 'a') as f:
            f.write(f"{data}\n")
        logger.info2("Data", f"{data} saved to \"{file_path}\".")
    except Exception as e:
        logger.error2("Data", f"Failed to save data: {e}.")


# -----------------------------------------------------------------------------------
# Function to get CSV header
# -----------------------------------------------------------------------------------
def get_csv_header(file_path: Path) -> list[str]:
    with open(file_path, mode="r", newline="") as file: 
        reader = csv.reader(file)
        header = next(reader)
    return header


# -----------------------------------------------------------------------------------
# Function to save dict data to CSV
# -----------------------------------------------------------------------------------
def save_dict_to_csv(data: dict, 
                    file_dir: Path, 
                    file_name: str, 
                    logger: ContextLogger, 
                    with_date: bool = True) -> None:
    """
    Save dict data to a CSV file.

    Parameters:
    - data: dict to save.
    - file_path: The Path to the CSV file where data will be appended.
    - logger: ContextLogger instance for logging.
    """
    # construct file path, with or without date
    if with_date:
        date_suffix = datetime.datetime.now().strftime("%Y%m%d")  # Get current date and time
        file_path = file_dir / f"{file_name}_{date_suffix}.csv"
    else:
        file_path = file_dir / f"{file_name}.csv"

    # create folder if doesnt exist
    if not file_dir.is_dir():
        os.makedirs(file_dir, exist_ok=True) 

    new_header_list = list(data.keys())

    # create file and header, if doesnt exist
    if not file_path.exists():
        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            header = new_header_list
            writer.writerow(header)  # header
            logger.info2("Data", f"Created new CSV file at \"{file_path}\".")

    # get existing header
    existing_header_list = get_csv_header(file_path)

    # new header exist!
    if set(new_header_list) - set(existing_header_list): 
        # read with pandas, append then save again
        df = pd.read_csv(file_path)
        df_new = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
        df_new.to_csv(file_path, index=False)
        logger.info2("Data", f"New header detected. Dict data saved to \"{file_path}\".")

    else:  # headers match
        # write 
        try:
            with open(file_path, mode='a', newline="") as f:
                writer = csv.DictWriter(f, fieldnames = existing_header_list)
                writer.writerow(data)

            logger.info2("Data", f"Dict data saved to \"{file_path}\".")
        except Exception as e:
            logger.error2("Data", f"Failed to save dict data: {e}.")


# -----------------------------------------------------------------------------------
# Function to check if the current python script is running
# -----------------------------------------------------------------------------------
def is_program_running(target_name: str) -> bool:

    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline", "exe"]):
        try:
            proc_name = (proc.info["name"] or "").lower()
            cmdline = proc.info.get("cmdline") or []
            exe_path = proc.info.get("exe")

            # ---- Case 1: compiled executable (.exe)
            if target_name.endswith(".exe"):
                if proc_name == target_name:
                    print("Program is running. Aborting initialization.")
                    print(f"Running EXE: PID={proc.pid}, EXE={exe_path}")
                    return True

                if exe_path and Path(exe_path).name.lower() == target_name:
                    print("Program is running. Aborting initialization.")
                    print(f"Running EXE: PID={proc.pid}, EXE={exe_path}")
                    return True

            # ---- Case 2: python script (.py)
            if target_name.endswith(".py"):
                if "python" in proc_name:
                    if any(Path(arg).name.lower() == target_name for arg in cmdline):
                        print(f"Running PY: PID={proc.pid}, CMD={cmdline}")
                        return True

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    print("Program is NOT running. Proceed to initialize.")
    return False

