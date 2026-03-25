"""
lib_TCPIP_Socket_V2.py

Purpose:
    Provide TCP/IP socket helpers for clients, servers, and connection tracking.

Changelog: 
- 2.0.0: 
    - Separate TCPIP_Listening_Server class from TCPIP_Socket_Connection class.
    - TCPIP_Socket_Connection class now uses persistent connections. 
- 1.0.0: 
    - Creates a TCPIP_Socket class to handle incoming and outgoing connections.
    - outgoing connections open and close connections every single time. 
"""

# import libraries
import os
import time
import socket
import threading
import subprocess
import json
from myStandard_Library.lib_ContextLogger import ContextLogger

__version__ = "2.0.0"


# Base class, common to TCPIP_Socket_Connection & TCPIP_Listening_Server
class TCPIP_Base():
    """
    Shared base for TCP socket helpers.
    """
    def __init__(self,
                ip: str,
                port: int,
                logger: ContextLogger,
                context_name: str = "TCPIP_Base") -> None:
        """
        Initialize shared state for TCP socket helpers.

        Args:
            ip (str): Remote or local IP address.
            port (int): TCP port number.
            logger (ContextLogger): Context-aware logger used for tracing.
            context_name (str): Label used in log output.

        Returns: None
        """
        
        self._ip = ip
        self._port = port
        self._logger = logger
        self._context = context_name
        self._lock = threading.Lock()


    @property
    def version(self) -> str:
        """
        Library version identifier.

        Returns:
            str: Semantic version string for this module.
        """
        return __version__


    # kill all process using a specific port, before the start of process
    def force_close_port(self) -> None:
        """
        Forcefully terminate any process using the configured port (excluding self).

        Args: None

        Returns: None
        """
        self._logger.info2(self._context, f"Preparing to close any process using port {self._port}.")
        try:
            # Get list of processes using the port
            result = subprocess.check_output(f'netstat -ano | findstr :{self._port}', shell=True).decode()

            current_pid = str(os.getpid())
            # Kill each process using the port
            for line in result.splitlines():
                pid = line.strip().split()[-1]
                if pid != current_pid:   # prevent self-kill
                    subprocess.call(f'taskkill /PID {pid} /F', shell=True)
                    self._logger.debug2(self._context, f"Killed process {pid} using port {self._port}.")
            self._logger.info2(self._context, f"Processes using port {self._port} killed.") 

        # No process using the port
        except subprocess.CalledProcessError as e:
            self._logger.info2(self._context, f"No process using port {self._port}. Error: {e}") 
            pass   


# Persistent Client
class TCPIP_Socket_Connection(TCPIP_Base):
    """
    Persistent TCP client with JSON/CSV helpers and retry handling.
    """
    def __init__(self,
                 ip: str,
                 port: int,
                 logger: ContextLogger,
                 label: str,
                 context_name: str = "TCPIP_Socket",
                 buffer_size: int = 8192,
                 timeout: float = 5.0,
                 reconnect_delay: float = 1.0) -> None:
        """
        Initialize a persistent TCP client with retrying JSON/CSV helpers.

        Args:
            ip (str): Remote IP to connect to.
            port (int): Remote port.
            logger (ContextLogger): Context-aware logger used for tracing.
            label (str): Friendly name for identifying this connection.
            context_name (str): Label used in log output.
            buffer_size (int): Maximum bytes to read per recv call.
            timeout (float): Socket timeout in seconds.
            reconnect_delay (float): Delay before reconnect attempts.

        Returns: None
        """
        
        super().__init__(ip, port, logger)
        self._label = label
        self._context = context_name
        self._buff_size = buffer_size
        self._timeout = timeout
        self._reconnect_delay = reconnect_delay
        self._lock = threading.Lock()
        self._consecutive_failures: int = 0
        self._conn: socket.socket | None = None
        self._aborted = False


    # create persistent connection for client
    def connect_socket(self) -> None:
        """
        Open a TCP connection with the configured timeout.

        Args: None

        Returns: None
        """
        if self._aborted:
            raise RuntimeError("Socket aborted.")
        self._logger.info2(self._context, f"Connecting to {self._ip}:{self._port}...")
        sock = socket.create_connection((self._ip, self._port), timeout=self._timeout)
        sock.settimeout(self._timeout)
        self._conn = sock

    
    # close socket without thread lock
    def _close(self) -> None:
        """
        Shutdown and close the underlying socket without acquiring locks.

        Args: None

        Returns: None
        """
        # cleanup socket
        if self._conn:
            # try shutdown connection
            try: 
                self._conn.shutdown(socket.SHUT_RDWR)
                self._logger.info2(self._context, f"Socket {self._ip}:{self._port} shutdown. Socket ID: {self._conn}")
            except Exception as e:  
                self._logger.warning2(self._context, f"Socket {self._ip}:{self._port} fail to shutdown. Error: {e}")
            # close connection
            self._conn.close()
            self._logger.info2(self._context, f"Socket {self._ip}:{self._port} closed. Socket ID: {self._conn}")
            self._conn = None


    # close socket with thread lock
    def close_socket(self) -> None:
        """
        Close the socket using the instance lock.

        Args: None

        Returns: None
        """
        with self._lock:
            self._close()

    def abort(self) -> None:
        """
        Attempt to unblock any pending recv by shutting down the socket.

        Args: None

        Returns: None
        """
        self._aborted = True
        sock = self._conn
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass


    # send and receive single json 
    def send_recv_json(self,
                       command: dict,
                       max_retries: int = 3,
                       terminator: str = "\n") -> dict:
        """
        Send a JSON command and return a decoded JSON response.

        Args:
            command (dict): JSON-serializable payload to send.
            max_retries (int): Number of send/receive attempts.
            terminator (str): Line terminator appended to the payload.

        Returns:
            dict: Parsed JSON reply.

        Raises:
            RuntimeError: If the retry limit is exceeded.
            ConnectionError: If the remote endpoint closes unexpectedly.
        """

        payload = json.dumps(command, separators=(",", ":")) + terminator
        last_error = None

        for attempt in range(max_retries):
            if self._aborted:
                raise RuntimeError("Socket aborted.")
            data = bytearray()
            try:
                with self._lock:
                    if self._conn is None:
                        self.connect_socket()
                    else:
                        self._conn.sendall(payload.encode("utf-8"))
                        self._logger.info2(self._context, f"Sent: {payload.strip()}")

                        while True:
                            chunk = self._conn.recv(self._buff_size)
                            if not chunk:
                                self._logger.warning2(self._context, f"Remote closed. No more incoming packets.")
                                raise ConnectionError("Remote closed.")
                            data.extend(chunk)
                            try:
                                json_reply = json.loads(data.decode("utf-8"))
                                self._logger.info2(self._context, f"Received: {json_reply}")
                                self._consecutive_failures = 0
                                return json_reply
                            except json.JSONDecodeError:
                                continue

            except Exception as e:
                if self._aborted:
                    raise RuntimeError("Socket aborted.") from e
                last_error = e
                self._consecutive_failures += 1
                self._logger.warning2(self._context, f"Received partial/empty: {data.decode('utf-8')}")
                self._logger.warning2(
                    self._context,
                    f"Attempt {attempt+1} failed ({self._consecutive_failures} consecutive): {e}",
                )
                self._close()
                time.sleep(self._reconnect_delay)

        raise RuntimeError("Max retries exceeded") from last_error
    

    # send and receive single csv 
    def send_recv_csv(self,
                      command: list,
                      max_retries: int = 3,
                      delimiter: str = ",",
                      header: str = "",
                      terminator: str = "\r\n") -> list[str]:
        """
        Send a CSV command and return the parsed row as a list of strings.

        Args:
            command (list): Sequence of values to CSV-encode.
            max_retries (int): Number of send/receive attempts.
            delimiter (str): CSV delimiter.
            header (str): Optional header prefix inserted before the payload.
            terminator (str): Line terminator appended to the payload.

        Returns:
            list[str]: First CSV line received, split into fields.

        Raises:
            RuntimeError: If the retry limit is exceeded.
            ConnectionError: If the remote endpoint closes unexpectedly.
        """

        payload = header + delimiter.join(map(str, command)) + terminator
        last_error = None

        for attempt in range(max_retries):
            if self._aborted:
                raise RuntimeError("Socket aborted.")
            data = bytearray()
            try:
                with self._lock:
                    if self._conn is None:
                        self.connect_socket()
                    else:
                        self._conn.sendall(payload.encode("utf-8"))
                        self._logger.info2(self._context, f"Sent: {payload.strip()}")

                        while True:
                            chunk = self._conn.recv(self._buff_size)
                            if not chunk:
                                self._logger.warning2(self._context, f"Remote closed. No more incoming packets.")
                                raise ConnectionError("Remote closed.")
                            data.extend(chunk)
                            decoded = data.decode("utf-8")
                            if terminator in decoded:
                                line = decoded.split(terminator)[0]
                                reply = line.split(delimiter) if line else []
                                self._logger.info2(self._context, f"Received: {reply}")
                                self._consecutive_failures = 0
                                return reply

            except Exception as e:
                if self._aborted:
                    raise RuntimeError("Socket aborted.") from e
                last_error = e
                self._consecutive_failures += 1
                self._logger.warning2(self._context, f"Received partial/empty: {data.decode('utf-8')}")
                self._logger.warning2(
                    self._context,
                    f"Attempt {attempt+1} failed ({self._consecutive_failures} consecutive): {e}"
                )
                self._close()
                time.sleep(self._reconnect_delay)

        raise RuntimeError("Max retries exceeded") from last_error



# 
class TCPIP_Listening_Server():
    """
    Single-connection TCP server for receiving raw data.
    """
    def __init__(self,
                 ip: str,
                 port: int,
                 logger: ContextLogger,
                 label: str,
                 context_name: str = "TCPIP_Server", 
                 timeout: float = 5.0) -> None:
        """
        Initialize a single-connection TCP server for receiving raw data.

        Args:
            ip (str): Local IP/interface to bind.
            port (int): Listening port.
            logger (ContextLogger): Context-aware logger used for tracing.
            label (str): Friendly name for identifying this server.
            context_name (str): Label used in log output.
            timeout (float): Socket timeout in seconds.

        Returns: None
        """
        
        self._ip = ip
        self._port = port
        self._label = label
        self._logger = logger
        self._context = context_name
        self._socket: socket.socket | None = None
        self._conn: socket.socket | None = None
        self._lock = threading.Lock()


    @property
    def version(self) -> str:
        """
        Library version identifier.

        Returns:
            str: Semantic version string for this module.
        """
        return __version__


    # start listening server 
    def start(self) -> None:
        """
        Bind and start listening for a single TCP client.

        Args: None

        Returns: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._ip, self._port))
        sock.listen(1)
        self._sock = sock
        self._logger.info2(self._context, f"Listening on {self._ip}:{self._port}")


    # accept connection once
    def accept_once(self) -> None:
        """
        Accept one incoming connection if none is active.

        Args: None

        Returns: None
        """
        if self._conn:
            return
        else:
            self._conn, addr = self._sock.accept()
            self._logger.info2(self._context, f"Accepted connection from {addr}")


    # receive from connection
    def recv(self, buffer: int = 8192) -> bytes:
        """
        Receive bytes from the active client or raise if disconnected.

        Args:
            buffer (int): Maximum bytes to read from the socket.

        Returns:
            bytes: Raw data received from the client.

        Raises:
            RuntimeError: If no active client connection exists.
            ConnectionError: If the client disconnects.
        """
        if not self._conn:
            raise RuntimeError("No active connection")
        data = self._conn.recv(buffer)
        if not data:
            self._conn.close()
            self._conn = None
            self._logger.error2(self._context, f"No data received.")
            raise ConnectionError("Client disconnected.")
        
        self._logger.info2(self._context, f"Received: {data}")
        return data


    # close connection and socket
    def close_socket(self) -> None:
        """
        Close the active connection and listening socket.

        Args: None

        Returns: None
        """
        if self._conn:
            self._conn.close()
            self._logger.info2(self._context, f"Connection {self._ip}:{self._port} closed. Socket ID: {self._socket}")
        if self._sock:
            self._sock.close()
            self._logger.info2(self._context, f"Socket {self._ip}:{self._port} closed. Socket ID: {self._socket}")
        


# this class is to track all TCP socket connection if necessary
class TCPIP_List:
    """
    Registry for tracking socket connections by label, IP, and port.
    """
    def __init__(self, logger: ContextLogger, context_name: str = "TCPIP List") -> None:
        """
        Initialize the socket registry.

        Args:
            logger (ContextLogger): Context-aware logger used for tracing.
            context_name (str): Label used in log output.

        Returns: None
        """
        self._map: dict[tuple[str, str, int], TCPIP_Socket_Connection] = {}
        self._logger = logger
        self._context = context_name
        self._lock = threading.Lock()

    @property
    def version(self) -> str:
        """
        Library version identifier.

        Returns:
            str: Semantic version string for this module.
        """
        return __version__
    

    def _key(self, sock: TCPIP_Socket_Connection) -> tuple[str, str, int]:
        """
        Compose the unique key for a socket entry.

        Args:
            sock (TCPIP_Socket_Connection): Socket handler to index.

        Returns:
            tuple[str, str, int]: Tuple containing label, IP address, and port.
        """
        return (sock._label, sock._ip, sock._port)

    # register socket to dict
    def register(self, sock: TCPIP_Socket_Connection) -> None:
        """
        Add a socket to the registry if not already present.

        Args:
            sock (TCPIP_Socket_Connection): Socket handler to register.

        Returns: None
        """
        key = self._key(sock)
        with self._lock:
            if key not in self._map:
                self._map[key] = sock
                self._logger.info2(self._context, f"Registered {sock._label} -> {sock._ip}:{sock._port}")
            else:
                self._logger.warning2(self._context, f"{sock._label} -> {sock._ip}:{sock._port} exists. No action done.")

    # remove socket from dict
    def remove(self, sock: TCPIP_Socket_Connection) -> None:
        """
        Remove a socket from the registry if it exists.

        Args:
            sock (TCPIP_Socket_Connection): Socket handler to remove.

        Returns: None
        """
        key = self._key(sock)
        with self._lock:
            if key in self._map:
                self._map.pop(key)
                self._logger.info2(self._context, f"Removed {sock._label} -> {sock._ip}:{sock._port}.")
            else:
                self._logger.warning2(self._context, f"{sock._label} -> {sock._ip}:{sock._port} not found. No action done.")

    # update socket handler in the dict
    def update(self, sock: TCPIP_Socket_Connection) -> None:
        """
        Replace the stored handler for an existing socket entry.

        Args:
            sock (TCPIP_Socket_Connection): Socket handler with the latest reference.

        Returns: None
        """
        key = self._key(sock)
        with self._lock:
            if key in self._map:
                self._map[key] = sock  # latest handler
                self._logger.info2(self._context, f"Updated {sock._label} -> {sock._ip}:{sock._port} socket handle.")
            else:
                self._logger.warning2(self._context, f"{sock._label} -> {sock._ip}:{sock._port} not found. No action done.")
