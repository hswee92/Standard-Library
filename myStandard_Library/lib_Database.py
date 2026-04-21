import pymysql
from myStandard_Library.lib_ContextLogger import ContextLogger


# conn = pymysql.connect(host_ip="...", user="...", password="...", database="your_db")
# exists = table_exists(conn, "your_db", "your_table")
class SQLDatabase:
    def __init__(
        self,
        host_ip: str,
        user: str,
        password: str,
        db_name: str,
        logger: ContextLogger,
        db_table: str | None = None,
        port: int = 3306,
        connect_timeout: int = 5,
        charset: str = "utf8mb4",
        max_reconnect: int = 5
    ):
        self._host_ip = host_ip
        self._user = user
        self._password = password
        self._db_name = db_name
        self._db_table = db_table
        self._port = port
        self._connect_timeout = connect_timeout
        self._charset = charset
        self._max_reconnect = max_reconnect
        self._logger = logger
        self._conn: pymysql.connections.Connection | None = None


    def connect(self) -> pymysql.connections.Connection:
        if self._conn is not None and self._conn.open:
            return self._conn
        self._conn = pymysql.connect(
            host=self._host_ip,
            user=self._user,
            password=self._password,
            database=self._db_name,
            port=self._port,
            connect_timeout=self._connect_timeout,
            charset=self._charset,
        )
        return self._conn


    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None

    
    def is_connected(self) -> bool: 
        try: 
            self._conn.ping(reconnect=True)
            return True
        except:
            self._logger.warning2("SQLDatabase", "Connection closed and reconnect failed.")
            raise ConnectionError("Connection closed and reconnect failed.")
    

    def validate_db(self) -> bool:
        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                conn = self.connect()
                self.is_connected()
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE schema_name = %s
                        LIMIT 1
                        """,
                        (self._db_name,),
                    )
                    exists = cur.fetchone() is not None

                if exists:
                    return True
                else: 
                    self._logger.error2("SQLDatabase", f"Database not found: {self._db_name}")
                    raise OSError(f"Database not found: {self._db_name}")
                
            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to validate database.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to validate database after {attempt} attempts.")


    def validate_table(self, table_name: str | None = None) -> bool:
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        
        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                conn = self.connect()
                self.is_connected()
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                        LIMIT 1
                        """,
                        (self._db_name, table),
                    )
                    return cur.fetchone() is not None
            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to validate table in database.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to validate table in database after {attempt} attempts.")


    def get_columns(self, table_name: str) -> list[str]:
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        
        attempt = 0
        while attempt < self._max_reconnect:
            try:
                conn = self.connect()
                self.is_connected()
                sql = f"SHOW COLUMNS FROM `{table_name}`"

                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()

                return {row[0] for row in rows}
            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to get column list from database.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to get column list from database after {attempt} attempts.")


    def get_last_value(
        self,
        column_name: str,
        table_name: str | None = None,
        order_by: str | None = None,
    ):
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        order_col = order_by or column_name
        table_safe = table.replace("`", "``")
        column_safe = column_name.replace("`", "``")
        order_safe = order_col.replace("`", "``")

        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                conn = self.connect()
                self.is_connected()
                sql = f"SELECT `{column_safe}` FROM `{table_safe}` ORDER BY `{order_safe}` DESC LIMIT 1"
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()

                if row is None:
                    return None
                self._logger.debug2("SQL", f"{row[0]}")
                return row[0]
    
            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to get last value.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to get last value after {attempt} attempts.")


    # row_count = db.insert_rows([{"pressure": 12.3}, {"pressure": 12.4}], table_name="readings")
    def insert_rows(
        self,
        rows: list[dict],
        table_name: str | None = None,
        on_duplicate: str | None = None,
        key_columns: str | list[str] | tuple[str, ...] | None = None,
    ) -> int:
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        if not rows:
            return 0
        table_safe = table.replace("`", "``")
        columns = [str(col) for col in rows[0].keys()]
        cols_sql = ", ".join(f"`{col.replace('`', '``')}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO `{table_safe}` ({cols_sql}) VALUES ({placeholders})"
        if on_duplicate is not None:
            mode = on_duplicate.strip().lower()
            if mode == "skip":
                sql = f"INSERT IGNORE INTO `{table_safe}` ({cols_sql}) VALUES ({placeholders})"
            elif mode == "update":
                if key_columns is None:
                    raise ValueError("key_columns is required when on_duplicate='update'")
                if isinstance(key_columns, str):
                    key_cols = {key_columns}
                else:
                    key_cols = {str(col) for col in key_columns}
                if not key_cols:
                    raise ValueError("key_columns must not be empty")
                update_cols = [col for col in columns if col not in key_cols]
                if update_cols:
                    updates_sql = ", ".join(
                        f"`{col.replace('`', '``')}` = VALUES(`{col.replace('`', '``')}`)"
                        for col in update_cols
                    )
                    sql += f" ON DUPLICATE KEY UPDATE {updates_sql}"
                else:
                    # No non-key columns to update; convert to no-op on duplicates.
                    first_key = next(iter(key_cols)).replace("`", "``")
                    sql += f" ON DUPLICATE KEY UPDATE `{first_key}` = `{first_key}`"
            else:
                raise ValueError("on_duplicate must be one of: 'skip', 'update', or None")
        values = [tuple(row[col] for col in columns) for row in rows]

        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                self._logger.debug2("SQL Insert", "Attempt to connect.")
                conn = self.connect()
                self.is_connected()
                self._logger.debug2("SQL Insert", "Attempt to insert rows.")
                with conn.cursor() as cur:
                    cur.executemany(sql, values)
                conn.commit()
                self._logger.debug2("SQL Insert", "Completed insert rows.")
                return len(rows)

            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to add rows into database.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to add rows into database after {attempt} attempts.")


    def create_table(
        self,
        columns: dict[str, str],
        table_name: str | None = None,
        if_not_exists: bool = True,
        primary_key: str | list[str] | tuple[str, ...] | None = None,
    ) -> None:
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")
        if not columns:
            raise ValueError("columns is required")
        table_safe = table.replace("`", "``")
        col_defs = ", ".join(
            f"`{col.replace('`', '``')}` {col_type}" for col, col_type in columns.items()
        )
        pk_cols: list[str] = []
        if primary_key is not None:
            if isinstance(primary_key, str):
                pk_cols = [primary_key]
            else:
                pk_cols = [str(col) for col in primary_key]
            if not pk_cols:
                raise ValueError("primary_key must not be empty")
            missing = [col for col in pk_cols if col not in columns]
            if missing:
                raise ValueError(f"primary_key column(s) not in columns: {missing}")
            pk_sql = ", ".join(f"`{col.replace('`', '``')}`" for col in pk_cols)
            col_defs += f", PRIMARY KEY ({pk_sql})"
        ine_clause = "IF NOT EXISTS " if if_not_exists else ""
        sql = f"CREATE TABLE {ine_clause}`{table_safe}` ({col_defs})"

        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                conn = self.connect()
                self.is_connected()
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
                return

            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to create table in database.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to create table in database after {attempt} attempts.")


    def add_column(
        self,
        column_name: str,
        datatype: str,
        table_name: str | None = None,
        after_which_column: str | None = None,
        first: bool = False,
        default_value: str | int | float | None = None
    ):
        table = table_name or self._db_table
        if not table:
            raise ValueError("table_name is required")

        table_safe = table.replace("`", "``")
        column_safe = column_name.replace("`", "``")
        default_safe = str(default_value).replace("'", "''")

        # default value 
        default_sql = ""
        if default_value is not None:
            if isinstance(default_value, (int, float)):
                default_sql = f" DEFAULT {default_safe}"
            else:
                default_sql = f" DEFAULT '{default_safe}'"

        # column position
        if first and after_which_column:
            raise ValueError("Use either first=True or after_which_column, not both")
        position_sql = ""
        if first:
            position_sql = " FIRST"
        elif after_which_column:
            after_safe = after_which_column.replace("`", "``")
            position_sql = f" AFTER `{after_safe}`"

        sql = f"""
        ALTER TABLE `{table_safe}`
        ADD COLUMN IF NOT EXISTS `{column_safe}` {datatype}{default_sql}
        {position_sql}
        """

        attempt = 0
        while attempt < self._max_reconnect:
            try: 
                conn = self.connect()
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
                return

            except Exception: 
                self._logger.warning2("SQLDatabase", f"Attempt {attempt+1}: Fail to add column '{column_name}' in '{table}'.")
                attempt += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self._conn.close()
                except Exception: 
                    pass
                self._conn = None

        if attempt == self._max_reconnect: 
            self._logger.error2("SQLDatabase", f"Fail to add column '{column_name}' in '{table}' after {attempt} attempts.")


# create table based on dict_sql_format if table is missing in database
# add column based on dict_sql_format if column is missing in table
def create_table_add_column(db_handler: SQLDatabase, 
                            db_table: str, 
                            dict_sql_format: dict[str:str], 
                            logger: ContextLogger, 
                            primary_key: list = ["Server_Datetime"]) -> None:
    
    if not db_handler.validate_table(db_table):
        db_handler.create_table(dict_sql_format, table_name=db_table, primary_key=primary_key)
        logger.info2("SQLDatabase", f"Table '{db_table}' created.")

    else: 
        sql_columns = db_handler.get_columns(db_table)
        previous_column = None
        # check column line by line
        for col in list(dict_sql_format.keys()):
            if col not in sql_columns:
                logger.info2("SQLDatabase", f"Missing '{col}'. Adding column to SQL table...")
                if previous_column is None: 
                    db_handler.add_column(col, dict_sql_format[col],db_table,first=True)
                else: 
                    db_handler.add_column(col, dict_sql_format[col],db_table,after_which_column=previous_column)
            previous_column = col
        logger.info2("SQLDatabase", f"Columns in '{db_table}' are up to date.")
        
