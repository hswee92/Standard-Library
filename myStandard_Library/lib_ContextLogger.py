"""
lib_ContextLogger.py

Purpose:
    Provide context-aware logging utilities with rotation and retention.

Changelog: 
- 2.0.0: 
    - Added method debug2, info2, warning2, error2, critical2. 
    - Added size and daily rotation modes.
- 1.0.0: 
    - Creates a ContextLogger class to log console and file logs. 
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, timedelta
from typing import Optional
import threading
import queue

__version__ = "2.0.0"

# Module-level logger cache to prevent duplicate handlers
_loggers = {}


class LibFileHandlerFormatter(logging.Formatter):
    """
    Format log records in the legacy lib_FileHandler.py style.
    """

    def __init__(self, include_context: bool = True):
        """
        Initialize the formatter.

        Args:
            include_context (bool): If True, include the [context] field.

        Returns: None
        """
        self.include_context = include_context
        # Don't call super().__init__() - we handle formatting manually


    @property
    def version(self) -> str:
        """
        Library version identifier.

        Returns:
            str: Semantic version string for this module.
        """
        return __version__
    

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record to the legacy style.

        Args:
            record (logging.LogRecord): LogRecord from Python logging.

        Returns:
            str: Formatted log string.
        """
        # Format timestamp in exact lib_FileHandler style
        timestamp = self.formatTime(record, datefmt='%Y-%m-%d %H:%M:%S')

        # Get log level name
        level = record.levelname

        # Extract context from record (fallback to logger name if not provided)
        context = getattr(record, 'context', record.name)

        # Build message
        message = record.getMessage()

        # Add exception info if present
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)

        if self.include_context:
            formatted = f"{timestamp} [{level}] [{context}]: {message}"
        else:
            formatted = f"{timestamp} [{level}]: {message}"

        # Append exception traceback if exists
        if record.exc_text:
            formatted = f"{formatted}\n{record.exc_text}"

        return formatted


class ContextLogger:
    """
    General-purpose logger with context-aware formatting and rotation.
    """

    def __init__(self, 
                 name: str, 
                 log_dir: str, 
                 context: str | None = None,
                 console_log_level: str = "DEBUG", 
                 file_log_level: str = "DEBUG",
                 rotation_mode: str = "size",
                 max_bytes: int = 10 * 1024 * 1024,
                 backup_count: int = 5, 
                 retention_days: int = 30):
        """
        Initialize ContextLogger (date-driven).

        Args:
            name (str): Logger name (usually program name).
            log_dir (str): Directory for log files.
            context (str): Default context for all messages (uses name if None).
            console_log_level (str): Minimum console log level.
            file_log_level (str): Minimum file log level.
            rotation_mode (str): "size" or "daily".
            max_bytes (int): Optional max bytes per file for size rotation.
            backup_count (int): Number of rotated files to keep.
            retention_days (int): Days to retain log files.

        Returns: None

        Raises:
            ValueError: If rotation_mode is not "size" or "daily".
        """

        # -----------------------------------------------------------------------------------
        # Load instance variables
        # -----------------------------------------------------------------------------------
        self.name = name
        self.log_dir = log_dir
        self.default_context = context or name
        self.console_log_level = console_log_level.upper()
        self.file_log_level = file_log_level.upper()
        self.retention_days = retention_days
        self._file_handler = None 

        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)

        # -----------------------------------------------------------------------------------
        # Load instance variables
        # -----------------------------------------------------------------------------------
        rotation_mode = rotation_mode.lower()
        # Validate rotation mode
        if rotation_mode not in ("size", "daily"):
            raise ValueError("rotation_mode must be 'size' or 'daily'")
        self._rotation_mode = rotation_mode

        # -----------------------------------------------------------------------------------
        # Load rotate settings
        # -----------------------------------------------------------------------------------
        # Set max_bytes based on rotation mode
        if rotation_mode == "size":
            if max_bytes is None:
                self.max_bytes = 10 * 1024 * 1024
            else:
                self.max_bytes = int(max_bytes)
        elif rotation_mode == "daily":
            # "daily" mode -> no size cap
            self.max_bytes = 0

        # Size-rotation backup count
        if backup_count is not None:
            self.backup_count = int(backup_count)
        else:
            self.backup_count = 5

        # -----------------------------------------------------------------------------------
        # Check logger cache & create logger
        # -----------------------------------------------------------------------------------
        # Check if logger already exists (avoid duplicate handlers)
        cache_key = f"{name}_{os.path.abspath(log_dir)}"
        if cache_key in _loggers:
            cached = _loggers[cache_key]
            self.logger = cached["logger"]
            self._file_handler = cached["file_handler"]
            self._rotation_mode = cached["rotation_mode"]
            self.file_log_level = cached["file_log_level"]
            self.retention_days = cached["retention_days"]
            self.max_bytes = cached["max_bytes"]
            self.backup_count = cached["backup_count"]

            self._current_date = datetime.now().date()
            self._date_lock = threading.Lock()

            # Purge expired logs on initialization
            self.purge_expired_logs()   
            return

        # Create new Python logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Capture all, filter at handler level
        self.logger.propagate = False        # Prevent duplicate logs to root logger

        # -----------------------------------------------------------------------------------
        # Console Handler & File Handler Setup
        # -----------------------------------------------------------------------------------
        # Setup Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, self.console_log_level))
        console_formatter = LibFileHandlerFormatter(include_context=True)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # Setup GUI Queue Handler
        gui_queue = queue.Queue(maxsize=1000)
        guihandler = GUIQueueHandler(gui_queue)


        # Current date for file naming
        self._current_date: date = datetime.now().date()

        # Setup File Handler & lock for handler switching
        self._file_handler: Optional[logging.Handler] = None
        self._date_lock = threading.Lock()
        try:
            self._file_handler = self._build_dated_file_handler()
            self.logger.addHandler(self._file_handler)
        except Exception as e:
            self.logger.error(f"Failed to initialize file handler: {e}", exc_info=True)

        # Purge expired logs on initialization
        self.purge_expired_logs()


    @property
    def version(self) -> str:
        """
        Library version identifier.

        Returns:
            str: Semantic version string for this module.
        """
        return __version__


    # -----------------------------------------------------------------------------------
    # Build File Handlers for current date
    # -----------------------------------------------------------------------------------
    def _build_dated_file_handler(self) -> logging.Handler:
        """
        Build a file handler for the current date.

        Args: None

        Returns:
            logging.Handler: Configured file handler for the current date.
        """
        date_str = self._current_date.strftime("%Y%m%d")
        log_path = os.path.join(self.log_dir, f"{self.name}_{date_str}.log")

        level = getattr(logging, self.file_log_level.upper())
        formatter = LibFileHandlerFormatter(include_context=True)

        if self.max_bytes and self.max_bytes > 0:
            handler = RotatingFileHandler(
                log_path,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count,
                encoding="utf-8"
            )
        else:
            handler = logging.FileHandler(
                log_path,
                encoding="utf-8"
            )

        handler.setLevel(level)
        handler.setFormatter(formatter)
        return handler
    
    
    # ------------------------------------------------------------------
    # Check if date has changed, and rotate file if needed
    # ------------------------------------------------------------------
    def _check_date_rollover(self):
        """
        Rotate the file handler when the date changes.

        Args: None

        Returns: None
        """
        today = datetime.now().date()
        if today != self._current_date:
            # Date changed -> rotate file handler
            with self._date_lock:
                # Double-check inside lock
                if today == self._current_date:
                    return

                # Remove old handler
                if self._file_handler is not None:
                    try:
                        self.logger.removeHandler(self._file_handler)
                    except Exception:
                        pass
                    try:
                        self._file_handler.close()
                    except Exception:
                        pass

                # Update date and attach new handler
                self._current_date = today
                try: 
                    self._file_handler = self._build_dated_file_handler()
                    self.logger.addHandler(self._file_handler)
                except Exception as e:
                    self.logger.error(f"Failed to initialize file handler: {e}", exc_info=True)
                
                # purge expired logs after date change
                self.purge_expired_logs()

    
    # ------------------------------------------------------------------
    # Purge expired logs by last modified date (mtime)
    # ------------------------------------------------------------------
    def purge_expired_logs(self):
        """
        Remove log files older than the retention window.

        Args: None

        Returns: None
        """

        now = datetime.now()
        today_str = self._current_date.strftime("%Y%m%d")
        cutoff_time = now - timedelta(days=self.retention_days)

        for fname in os.listdir(self.log_dir):
            # Only target .log base files
            if not (fname.lower().endswith(".log") or ".log." in fname.lower()):
                continue

            # Never delete today's active log
            if fname.endswith(f"_{today_str}.log"):
                continue   

            fpath = os.path.join(self.log_dir, fname)
            # Ensure it's a normal file
            if not os.path.isfile(fpath):
                continue
            try:
                mtime_epoch = os.path.getmtime(fpath)
                file_mtime = datetime.fromtimestamp(mtime_epoch)
            except OSError:
                # Cannot access file metadata, skip safely
                continue

            # Retention decision
            if file_mtime < cutoff_time:
                try:
                    os.remove(fpath)
                    self.logger.info(f"Deleted expired log: {fname}", extra={"context": "Purge"})
                except PermissionError:
                    # File is locked / in use
                    self.logger.warning(f"Skipped (locked): {fname}", extra={"context": "Purge"})
                except Exception as e:
                    self.logger.warning(f"Failed to delete {fname}: {e}", extra={"context": "Purge"})


    # -----------------------------------------------------------------------------------
    # GUI Handler Management
    # -----------------------------------------------------------------------------------
    def add_gui_handler(self, gui_queue, handler_name: str = "gui"):
        """
        Attach a GUI queue handler to this logger.
        Safe to call multiple times with the same handler_name.
        """
        attr_name = f"_handler_{handler_name}"

        old_handler = getattr(self, attr_name, None)
        if old_handler is not None:
            return old_handler

        handler = GUIQueueHandler(gui_queue)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(LibFileHandlerFormatter(include_context=True))

        self.logger.addHandler(handler)
        setattr(self, attr_name, handler)
        return handler


    def remove_gui_handler(self, handler_name: str = "gui"):
        """
        Detach a GUI queue handler from this logger.
        """
        attr_name = f"_handler_{handler_name}"
        handler = getattr(self, attr_name, None)

        if handler is None:
            return

        try:
            self.logger.removeHandler(handler)
        finally:
            try:
                handler.close()
            except Exception:
                pass
            setattr(self, attr_name, None)


    # -----------------------------------------------------------------------------------
    # Logging Methods (Debug, Info, Warning, Error, Critical)
    # -----------------------------------------------------------------------------------
    def debug(self, msg: str, context: str | None = None):
        """
        Log a debug message.

        Args:
            msg (str): Message to log.
            context (str): Context override (uses default if None).

        Returns: None
        """
        self._check_date_rollover()
        ctx = context or self.default_context
        self.logger.debug(msg, extra={'context': ctx})

    def debug2(self, context: str, msg: str):
        """
        Log a debug message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.

        Returns: None
        """
        self.debug(msg, context=context)

    def info(self, msg: str, context: str | None = None):
        """
        Log an info message.

        Args:
            msg (str): Message to log.
            context (str): Context override (uses default if None).

        Returns: None
        """
        self._check_date_rollover()
        ctx = context or self.default_context
        self.logger.info(msg, extra={'context': ctx})

    def info2(self, context: str, msg: str):
        """
        Log an info message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.

        Returns: None
        """
        self.info(msg, context=context)

    def warning(self, msg: str, context: str | None = None):
        """
        Log a warning message.

        Args:
            msg (str): Message to log.
            context (str): Context override (uses default if None).

        Returns: None
        """
        self._check_date_rollover()
        ctx = context or self.default_context
        self.logger.warning(msg, extra={'context': ctx})

    def warning2(self, context: str, msg: str):
        """
        Log a warning message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.

        Returns: None
        """
        self.warning(msg, context=context)

    def warn(self, msg: str, context: str | None = None):
        """
        Log a warning message (backward-compatible alias).

        Args:
            msg (str): Message to log.
            context (str): Context override (uses default if None).

        Returns: None
        """
        self.warning(msg, context)

    def warn2(self, context: str, msg: str):
        """
        Log a warning message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.

        Returns: None
        """
        self.warning(msg, context=context) 

    def error(self, msg: str, context: str | None = None, exc_info: bool = False):
        """
        Log an error message.

        Args:
            msg (str): Error message to log.
            context (str): Context override (uses default if None).
            exc_info (bool): If True, include exception traceback in log.

        Returns: None
        """
        self._check_date_rollover()
        ctx = context or self.default_context
        self.logger.error(msg, extra={'context': ctx}, exc_info=exc_info)

    def error2(self, context: str, msg: str, exc_info: bool = False):
        """
        Log an error message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.
            exc_info (bool): If True, include exception traceback in log.

        Returns: None
        """
        self.error(msg, context=context, exc_info=exc_info)

    def critical(self, msg: str, context: str | None = None, exc_info: bool = False):
        """
        Log a critical message.

        Args:
            msg (str): Critical message to log.
            context (str): Context override (uses default if None).
            exc_info (bool): If True, include exception traceback in log.

        Returns: None
        """
        self._check_date_rollover()
        ctx = context or self.default_context
        self.logger.critical(msg, extra={'context': ctx}, exc_info=exc_info)

    def critical2(self, context: str, msg: str, exc_info: bool = False):
        """
        Log a critical message with context-first ordering.

        Args:
            context (str): Context label to attach.
            msg (str): Message to log.
            exc_info (bool): If True, include exception traceback in log.

        Returns: None
        """
        self.critical(msg, context=context, exc_info=exc_info)


# -----------------------------------------------------------------------------------
# Convenience Function to Get ContextLogger
# -----------------------------------------------------------------------------------
def get_logger(name: str, log_dir: str = "logs", 
               context: str | None = None,
               console_log_level: str = "DEBUG", 
               file_log_level: str = "DEBUG",
                rotation_mode: str = "size",
                max_bytes: int = 10 * 1024 * 1024,
                backup_count: int = 5, 
                retention_days: int = 30):
    """
    Create or retrieve a cached ContextLogger instance.

    Args:
        name (str): Logger name (usually program name).
        log_dir (str): Directory for log files.
        context (str): Default context label (uses name if None).
        console_log_level (str): Minimum console log level.
        file_log_level (str): Minimum file log level.
        rotation_mode (str): "size" or "daily".
        max_bytes (int): Optional size cap per file.
        backup_count (int): Number of rotated files to keep.
        retention_days (int): Days to retain log files.

    Returns:
        ContextLogger: Logger instance ready to use.
    """
    cache_key = f"{name}_{os.path.abspath(log_dir)}"
    if cache_key in _loggers:
        logger = _loggers[cache_key]

        # Optional sync if user passes new parameters later
        if retention_days is not None:
            logger.retention_days = retention_days
        if max_bytes is not None:
            logger.max_bytes = max_bytes
        if backup_count is not None:
            logger.backup_count = backup_count

        return logger

    logger = ContextLogger(
        name=name,
        log_dir=log_dir,
        context=context,
        console_log_level=console_log_level,
        file_log_level=file_log_level,
        rotation_mode=rotation_mode,
        max_bytes=max_bytes,
        backup_count=backup_count,
        retention_days=retention_days
    )
    _loggers[cache_key] = logger
    return logger


# check if logger has GUI handler
def has_gui_handler(self, handler_name: str = "gui") -> bool:
    return getattr(self, f"_handler_{handler_name}", None) is not None


# -----------------------------------------------------------------------------------
# GUI Queue Handler (Optional)
# -----------------------------------------------------------------------------------
class GUIQueueHandler(logging.Handler):
    def __init__(self, gui_queue: queue.Queue):
        super().__init__()
        self.gui_queue = gui_queue

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        if self.gui_queue.full():
            try:
                self.gui_queue.get_nowait()
            except Exception:
                pass

        self.gui_queue.put_nowait(msg)





# -----------------------------------------------------------------------------------
# Legacy Function - Backward Compatibility
# -----------------------------------------------------------------------------------

# # Logs data to a file separated by date.
# def log_data(log_text:str, folder_path:str=os.getcwd(), log_level:str="INFO", context:str="Main") -> None:
#     """
#     Append a timestamped log line to a date-based file.

#     Args:
#         log_text (str): Text to log.
#         folder_path (str): Directory to write log files.
#         log_level (str): Log level label to store.
#         context (str): Context label to attach.

#     Returns: None
#     """

#     # Ensure the folder exists
#     os.makedirs(folder_path, exist_ok=True)
    
#     # Generate log file name based on current date
#     date_str = datetime.now().strftime("%Y%m%d")
#     log_filename = f"log_{date_str}.log"
#     log_filepath = os.path.join(folder_path, log_filename)
    
#     # Prepare log entry with timestamp
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     entry = f"{timestamp} [{log_level}] [{context}]: {log_text}\n"
    
#     # Check if file exists, create if not
#     if not os.path.exists(log_filepath):
#         with open(log_filepath, "w") as f:
#             f.write(entry)
#     else:
#         with open(log_filepath, "a") as f:
#             f.write(entry)

         
