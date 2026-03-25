"""Lightweight watchdog primitives for TCP/IP listener and similar services.

Design notes:
- Watchdogs only track time; the supervisor executes recovery actions.
- Two built-in recovery options: rebind socket or restart the program.
- Compatible with both Python scripts and PyInstaller executables.
"""

import os
import sys
import logging
import threading
import time
from typing import Callable, Dict, Union, Optional
import socket
import errno
from myStandard_Library.lib_ContextLogger import ContextLogger

# ---------------------------------------------------------------------------
# Watchdog Types
# ---------------------------------------------------------------------------
# Base Watchdog as common for all types
class Watchdog_Base():
    """Common watchdog state container."""

    def __init__(self,
                display_name: str,
                timeout_s: int,
                logger: ContextLogger,) -> None:

        self.name = display_name
        self._timeout_s = timeout_s
        self._logger = logger
        self._last_kick_s = time.monotonic()

        # Guard conditions
        if self._timeout_s <= 0:
            self._logger.error2("Watchdog_Base", "Timeout must be positive integer.")
            raise ValueError("timeout_s must be positive")

    # kick in the latest update time
    def _kick(self) -> None:
        self._last_kick_s = time.monotonic()

    def _expired(self) -> bool:
        return (time.monotonic() - self._last_kick_s) >= self._timeout_s

    def _reset(self) -> None:
        self._last_kick_s = time.monotonic()


class Watchdog_Heartbeat(Watchdog_Base):
    """Tracks remote heartbeat events (time-based only)."""
    def __init__(self,
                display_name: str,
                timeout_s: int,
                logger: ContextLogger,
                socket_handler: socket.socket | None = None, 
                context_label: str = "Watchdog_Heartbeat",) -> None:
        
        super().__init__(display_name, timeout_s, logger)
        self._socket_handler = socket_handler
        self._context_label = context_label
        self._lock = threading.Lock()

    # Override method with thread locking. Prevent racing when concurrent process is applied. 
    def kick(self) -> None:  
        with self._lock:
            super()._kick()

    # Standard expired method with thread locking
    def expired_(self) -> bool: 
        with self._lock:
            return super()._expired()
        
    # Update socket handler
    def update_socket(self, new_socket: socket.socket) -> None: 
        with self._lock:
            self._socket_handler = new_socket
            self._logger.info2(self._context_label, "Socket handler updated.")


    # Customized expired method with custom check 
    def expired(self) -> bool:  
        if not self.expired_():
            return False
        else: # if stardard expired return True
            if self._socket_handler:
                self.kick()
                self._logger.debug2(self._context_label, "Heartbeat expired.")
                return True
            else:
                self.kick()
                self._logger.debug2(self._context_label, "Socket not connected.")
                return False


    def reset(self) -> None:  
        with self._lock:
            super()._reset()


# ---------------------------------------------------------------------------
# Recovery Actions
# ---------------------------------------------------------------------------
RecoveryAction = Callable[[], None]

def restart_program() -> None:
    """Restart the current program (supports script or PyInstaller exe)."""
    executable = sys.executable
    args = [executable] + sys.argv[1:] if getattr(sys, "frozen", False) else [executable] + sys.argv
    os.execv(executable, args)

# only placeholder, no function
def flag_restart_program() -> None:
    ...

RECOVERY_ACTIONS: Dict[str, RecoveryAction] = {
    "restart_program": restart_program,
    "flag_restart_program": flag_restart_program,
    "noop": lambda: None
}


# ---------------------------------------------------------------------------
# Watchdog Supervisor
# ---------------------------------------------------------------------------
class Watchdog_Supervisor:
    """Coordinates scanning and recovery for registered watchdogs."""

    def __init__(self, scan_interval_s: float, logger: ContextLogger):
        self._scan_interval_s = scan_interval_s
        self._logger = logger
        self._registry = []
        self._stop_event = threading.Event()
        self._thread = None
        self._lock = threading.Lock()

    # register watchdog into supervisor's checklist
    def register(self, watchdog: Watchdog_Base, recovery_action: Union[str, RecoveryAction]) -> None:
        # check recovery_action is default program or user-defined function
        if isinstance(recovery_action, str):
            if recovery_action not in RECOVERY_ACTIONS:
                self._logger.error2("Watchdog_Supervisor", f"Unknown recovery action: {recovery_action}")
                raise KeyError(f"Unknown recovery action: {recovery_action}")
            action = RECOVERY_ACTIONS[recovery_action]
        else:
            action = recovery_action  # user-supplied callable
            self._logger.info2("Watchdog_Supervisor", f"[{watchdog.name}] will [{action.__name__}] upon expiration.")
        # register to registry
        with self._lock:
            self._registry.append((watchdog.name, watchdog, action))

    # scan all the watchdogs in the list
    def scan(self) -> None:
        # take a snapshot so we don't hold the lock while running actions
        with self._lock:
            snapshot = list(self._registry)
        for _name, _watchdog, _recovery_action in snapshot:
            try:
                if _watchdog.expired():
                    self._logger.warning2(_name, "Watchdog expired. Executing recovery action...")
                    _watchdog.reset()
                    _recovery_action()                 
            except Exception:
                self._logger.error2("Watchdog_Supervisor", f"Recovery action failed for {_name}", exc_info=True)

    def reset_all(self) -> None:
        with self._lock:
            snapshot = list(self._registry)
        for _name, _watchdog, _recovery_action in snapshot:
            _watchdog.reset()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._scan_interval_s + 1)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.scan()
            self._stop_event.wait(self._scan_interval_s)
