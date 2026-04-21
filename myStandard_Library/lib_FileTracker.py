from pathlib import Path
from enum import IntEnum
import pandas as pd

from myStandard_Library.lib_ContextLogger import ContextLogger

# enum class for modified status
class ModifiedStatus(IntEnum):
    UNCHANGED = 0
    MODIFIED = 1
    NEW = 2
    ERROR = -1


class FileTracker:
    def __init__(self, file_path: Path, logger: ContextLogger, machine_label: str | None = None):
        self._file_path = file_path
        self._folder_path = file_path.parent
        self._file_stem = file_path.stem
        self._file_extension = file_path.suffix
        self._logger = logger

        self.tracked_mtime = None
        self.tracked_date: str | None = None
        self.tracked_time: str | None = None
        self.tracked_feature1: str | None = None
        self.latest_mtime = None

        if machine_label:
            self._context = machine_label.lower() + "_FileTracker"
        else:
            self._context = "FileTracker"

        self._validate_file()


    # verify path to confirm it is a valid path
    def _validate_file(self, verbose: bool = True) -> bool:
        if verbose:
            self._logger.debug2(self._context, f"This is the file: {self._file_path}")

        if self._file_path.is_file():
            return True
        else: 
            self._logger.warning2(self._context, f"File not found: {self._file_path}")
            return False
            # raise OSError(f"File not found: {self._file_path}")

    # return mtime 
    def _get_mtime(self) -> float: # type: ignore
        try:
            return self._file_path.stat().st_mtime
        except PermissionError as e:
            self._logger.error2(self._context, f"Permission denied: unable to get last modification time for file {self._file_path}")
            raise
        except OSError as e:
            self._logger.error2(self._context, f"Failed to retrieve last modification time for file {self._file_path}: {e}")
            raise

    # check if file has been modified since last check
    def has_file_changed(self) -> ModifiedStatus:
        if not self._validate_file(verbose = False):
            self._logger.error2(self._context, f"File not found: {self._file_path}")
            raise OSError(f"File not found: {self._file_path}")
        try:
            self._latest_mtime = self._get_mtime()
        except Exception:
            return ModifiedStatus.ERROR

        if self.tracked_mtime is None:
            return ModifiedStatus.NEW     
        else: 
            if self.tracked_mtime != self._latest_mtime:
                return ModifiedStatus.MODIFIED
            else:  # self.tracked_mtime == self._latest_mtime
                self._latest_mtime = None
                return ModifiedStatus.UNCHANGED
         
    def update_tracked_mtime(self) -> None:
        self.tracked_mtime = self._latest_mtime
        self._latest_mtime = None

    # def update_tracked_date(self) -> None:
    #     self.tracked_date = self._latest_date
    #     self._latest_date = None

    # def update_tracked_time(self) -> None:
    #     self.tracked_time = self._latest_time
    #     self._latest_time = None


    # check and remove first line duplicated tracked feature in df, if any
    # use to remove duplicated shot counter
    # this will remove top rows that have the same tracked_feature, until a different feature value is found
    def remove_duplicated_tracked_features(self, df: pd.DataFrame, feature_column: str) -> pd.DataFrame:
        if self.tracked_feature1 is None:
            return df
        else:
            i_count = 0
            # df_deduplicated = df[df[feature_column] != self.tracked_feature1]
            for i in range(len(df)):
                if df[feature_column].iloc[i] == self.tracked_feature1: 
                    if i == len(df)-1: # last line
                        df_deduplicated = pd.DataFrame()    
                        i_count = i
                    continue
                else: # found different tracked_feature 
                    df_deduplicated = df.iloc[i:]
                    i_count = i
                    break

            self._logger.debug2(self._context, f"Removed {i_count} duplicated tracked feature(s): {self.tracked_feature1}")
            return df_deduplicated
            

    # return new lines in df
    def get_new_lines(self, df: pd.DataFrame, date_column: str, time_column: str, feature_column: str | None = None) -> pd.DataFrame:
        # compare values in date/time columns and match tracked_date/tracked_time
        # get index of line
        if self.tracked_date is None or self.tracked_time is None:
            self._logger.debug2(self._context, f"Tracked date: {self.tracked_date} & Tracked time: {self.tracked_time}")
            return df
        
        self._logger.debug2(self._context, f"Tracked date: '{self.tracked_date}' & Tracked time: '{self.tracked_time}'")
        self._logger.debug2(self._context, f"Tracked date: {type(self.tracked_date)} & Tracked time: {type(self.tracked_time)}")
        self._logger.debug2(self._context, f"Last date: '{df[date_column].iloc[-1]}' & Last time: '{df[time_column].iloc[-1]}'")
        self._logger.debug2(self._context, f"Last date: {type(df[date_column].iloc[-1])} & Last time: {type(df[time_column].iloc[-1])}")
        
        tracked_line = df[
            (df[date_column] == self.tracked_date) & (df[time_column] == self.tracked_time)
        ]
        
        self._logger.debug2(self._context, f"Tracked line: {tracked_line}")
        if tracked_line.empty:
            if feature_column is not None:
                if len(df) > 0:
                    df_filtered = self.remove_duplicated_tracked_features(df, feature_column)
                    return df_filtered
            return df
        row_id = tracked_line.index[-1]
        pos = df.index.get_loc(row_id)
        if pos < 0:
            if feature_column is not None:
                df_filtered = self.remove_duplicated_tracked_features(df, feature_column)
                return df_filtered            
            return df
        elif pos >= len(df) - 1:
            return df.iloc[0:0]
        # elif pos == len(df): # last line
        #     return pd.DataFrame()
        else:
            df_new = df.iloc[pos + 1:]
            if feature_column is not None:
                df_filtered = self.remove_duplicated_tracked_features(df_new, feature_column)
                return df_filtered            
            return df_new


    # backup all older dates
    def backup_by_date(self) -> None:
        # read starting mtime
        start_mtime =  self._file_path.stat().st_mtime
        # load from csv file
        df = pd.read_csv(self._file_path)

        # get all unique dates 
        unique_dates = df['DATE'].unique().tolist()
        # remove latest date from unique_dates, since we want to keep the latest date in the original file
        latest_date = df['DATE'].iloc[-1]
        unique_dates.remove(latest_date)

        # filter and save to new file
        for date in unique_dates:
            full_file_path = f"{self._folder_path}/{self._file_stem}_{date}{self._file_extension}"
            # check if file already exists, if yes, skip
            if Path(full_file_path).is_file():
                self._logger.debug2("Backup By Date", f"File already exists, skipping backup for date {date}: {full_file_path}")
                continue
            df_filtered = df[df['DATE'] == date]
            df_filtered.to_csv(full_file_path, index=False)
            self._logger.info2("Backup By Date", f"Backup successful for date {date}: {full_file_path}")

        # finally get the latest date 
        latest_mtime = self._file_path.stat().st_mtime
        # only replace if the file is not changed during the backup process, otherwise, skip and log warning
        if start_mtime == latest_mtime:
            df_latest = df[df['DATE'] == latest_date]
            df_latest.to_csv(self._file_path, index=False)
        else:
            self._logger.warning2("Backup By Date", f"File has been modified during backup process, skipping replace {latest_date}: {self._file_path}")




    
