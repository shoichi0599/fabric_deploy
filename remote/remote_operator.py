import contextlib
import time
from fabric import Connection
from pathlib import Path


class RemoteOperator:
    """ This class provides useful functions for remote operation.

    Initialize with `fabric.connection.Connection` to run commands
    on a remote host.

    """

    def __init__(self, connection):
        """ Constructor.

        Args:
            connection (Connection): A connection to a remote host
        """

        self.connection = connection

    def mkdir(self, remote_path: str, **kwargs):
        """Creates a directory on a remote host.

        Args:
            remote_path (str) : A path of directory to be created on host
            **kwargs          : See `invoke.Runner.run` for details on the available keyword arguments

        Raises:
            ValueError: If an argument is invalid
            OSError   : If failed to create a directory

        """

        if not (isinstance(remote_path, str) or remote_path):
            raise ValueError('remote_path must be string and not be None or empty.')

        result = self.connection.run(f'ls -l {remote_path}', warn=True, **kwargs)
        if result.failed:
            result = self.connection.run(f'mkdir -pv {remote_path}', warn=True, **kwargs)
            if result.failed:
                raise OSError(f'Failed to create directory by command: {result.command}')
            print(f'Directory is created by command: {result.command}')

    def backup(self, path_from: str, path_to: str, **kwargs):
        """Backs up an application.

        First create a backup directory with the specified 'path_to'.
        Then copies files from the specified 'path_from' to the backup directory.

        Args:
            path_from (str) : A path of flies to be backed up
            path_to (str)   : A path of directory to back up the files to
            **kwargs        : See `invoke.Runner.run` for details on the available keyword arguments

        Raises:
            ValueError: If an argument is invalid
            OSError   : If failed to create a directory

        """

        if not (isinstance(path_from, str) or path_from):
            raise ValueError('path_from must be string and not be None or empty.')
        if not (isinstance(path_to, str) or path_to):
            raise ValueError('path_to must be string and not be None or empty.')
        result = self.connection.run(f'ls -l {path_from}', warn=True, **kwargs)
        if result.failed:
            # Not to miss backing up files, raise an error if path does not exist
            raise ValueError(f'path_from does exist on host by command: {result.command}')

        # Create a backup directory
        self.mkdir(path_to, **kwargs)

        # Copy files to the backup directory
        print('Backing up ...')
        result = self.connection.run(f'cp -prv {path_from} {path_to}', warn=True)
        if result.failed:
            raise OSError(f'Failed to back up file(s) by command: {result.command}')
        print(f'Backed up files by command: {result.command}')

    def upload(self, local_path: Path, remote_path: str, **kwargs):
        """Uploads a file or directory.

        If local_path is a directory, create the directory on host,
        else put a file on host.

        Args:
            local_path (Path) : A Path object of directory or file to be created on remote host
            remote_path (str) : An absolute path of directory on remote host
            **kwargs          : See `invoke.Runner.run` for details on the available keyword arguments

        Raises:
            TypeError : If type of argument is invalid
            ValueError: If invalid arguments are specified
            OSError   : If any remote operations failed

        """

        # Validate arguments
        if not isinstance(local_path, Path):
            raise TypeError('Type of local_path must be pathlib.Path')
        if not (isinstance(remote_path, str) or remote_path):
            raise ValueError('remote_path must be string and not be None or empty.')
        result = self.connection.run(f'test -d {remote_path}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Remote path does not exists or is not a directory by command: {result.command}.')

        # Create a directory on remote host if the given local path is a directory
        if local_path.is_dir():
            self.mkdir(f'{remote_path}/{local_path.name}', **kwargs)
            for p in local_path.iterdir():
                # The directory of the given path_local is created under the remote_path on host,
                # so pass the remote path including the path of the created directory to the method
                self.upload(p, f'{remote_path}/{local_path.name}', **kwargs)

        # Upload the file
        else:
            result = self.connection.put(local_path.absolute(), f'{remote_path}/{local_path.name}')
            print(f'Uploaded {result.local} to {result.remote}')
            return

    @contextlib.contextmanager
    def disable_crontab(self, workspace: str, save_filename: str, **kwargs):
        """Disables crontab.

        This method uses `contextlib.contextmanager` and is expected to use with the `with` statement
        and automatically enables crontab after all of the tasks in `with `statement finish.

        See below 'Usage' for detail.

        Process:
        1) Save the current crontab
        2) Create an empty file
        3) Disable crontab
        4) Execute task in the `with` statement
        5) Enable crontab

        Args:
            workspace (str)    : A path for workspace for crontab operation on host
            save_filename (str): A file name to save the code in the current crontab
            **kwargs           : See `invoke.Runner.run` for details on the available keyword arguments

        Raises:
            ValueError        : If any arguments are invalid
            NotADirectoryError: If the specified workspace does not exist or is not a directory on host
            OSError           : If any remote operations failed

        Usage:
            with RemoteOperator(Connection).disable_crontab('workspace', 'save_filename'):
                task1
                taks2

        """

        # Validate arguments
        if not (isinstance(workspace, str) or workspace):
            raise ValueError('workspace must be string and not be None or empty.')
        if not (isinstance(save_filename, str) or save_filename):
            raise ValueError('save_filename must be string and not be None or empty.')
        result = self.connection.run(f'test -d {workspace}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'workspace does not exist or is not a directory by command: {result.command}.')

        # Save original crontab file
        result = self.connection.run(f'crontab -l > {workspace}/{save_filename}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to save the crontab file by command: {result.command}')

        print(f'Saved original crontab file by command: {result.command}')
        self.connection.run(f'cat {workspace}/{save_filename}', warn=True)

        # Create an empty file
        empty_filename = 'crontab.empty'
        self.connection.run(f'rm {workspace}/{empty_filename}', warn=True)  # Remove a old file
        result = self.connection.run(f'touch {workspace}/{empty_filename}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to create a file by command: {result.command}')

        # Set crontab to empty file
        result = self.connection.run(f'crontab {workspace}/{empty_filename}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to disable crontab by command: {result.command}')
        print(f'Disabled crontab by command: {result.command}')

        try:
            yield
        finally:
            self.enable_crontab(f'{workspace}/{save_filename}')

    def enable_crontab(self, file_path: str, **kwargs):
        """Enables crontab.

        Sets crontab to the specified file.

        Args:
            file_path (str): A path of a file to set crontab to
            **kwargs       : See `invoke.Runner.run` for details on the available keyword arguments

        Raises:
            ValueError       : If any arguments are invalid
            FileNotFoundError: If a file is not found in the specified file path
            OSError          : If any remote operations failed

        """

        # Validate arguments
        if not (isinstance(file_path, str) or file_path):
            raise ValueError('file_path must be string and not be None or empty.')

        result = self.connection.run(f'ls -l {file_path}', warn=True, **kwargs)
        if result.failed:
            raise FileNotFoundError(f'Specified file is not found by command: {result.command}')

        print('Before enabling crontab')
        self.connection.run(f'crontab -l', warn=True)

        # Set crontab to the specified file
        result = self.connection.run(f'crontab {file_path}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to enable crontab by command: {result.command}')
        print(f'Enabled crontab by command: {result.command}')

        print('After enabling crontab')
        self.connection.run(f'crontab -l', warn=True)

    def stop_process_with_kill_file(
            self,
            to_be_created_kill_file_path: str,
            process_name_pattern: str,
            **kwargs
    ):
        """Stops an application.

        This method is to stop the application which manages its process with a file.

        Process:
        1) If the application already stopped, does nothing and exits this method.
        2) Creates a kill file under the specified path.
        3) Confirms that the application has stopped.
           It will be timed-out after 60 seconds.

        Args:
            to_be_created_kill_file_path (str): A path of a kill file which will be created
            process_name_pattern (str)        : A pattern of an app process name to grep system processes with
            **kwargs                          : See `invoke.Runner.run` for details on the available keyword arguments

        """

        # Check if application has been running
        result = self.connection.run(f'ps -ef | grep {process_name_pattern} | grep -v grep', warn=True, **kwargs)
        if result.failed:
            print(f'Application has not been running by command: {result.command}')
            print('Skip stopping process')
            return

        # Stop the process by creating a kill file
        result = self.connection.run(f'touch {to_be_created_kill_file_path}', warn=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to create a kill file by command: {result.command}')

        # Confirm that application stopped
        max_limit_time = time.time() + 60
        while True:
            current_time = time.time()
            if current_time >= max_limit_time:
                raise OSError('Process could not be stopped within 60 seconds, Please check the server spec.')

            result = self.connection.run(f'ps -ef | grep {process_name_pattern} | grep -v grep', warn=True, **kwargs)
            if result.failed:  # => No process found
                print(f'Confirmed that the application has stopped by command: {result.command}')
                break
            else:
                time.sleep(1)

    def start_process_with_kill_file(
            self,
            to_be_removed_kill_file_path: str,
            process_name_pattern: str,
            exec_file_path: str,
            **kwargs
    ):
        """Starts an application.

        This method is to start the application which manages its process with a file.

        Process:
        1) If the application has already been running, does nothing and exits this method.
        2) Removes a kill file
        3) Runs an execution file of an application.
        4) Confirms that the application has started.
           It will be timed-out after 60 seconds.

        Args:
            to_be_removed_kill_file_path (str): A path of a kill file which will be removed
            process_name_pattern (str)        : A pattern of an app process name to grep system processes with
            exec_file_path (str)              : A path of an execution file of application
            **kwargs                          : See `invoke.Runner.run` for details on the available keyword arguments

        """

        # Check if application has been running
        result = self.connection.run(f'ps -ef | grep {process_name_pattern} | grep -v grep', warn=True, **kwargs)
        if result.ok:
            print(f'Application has already been running by command: {result.command}')
            print('Skip starting process')
            return

        # Remove the kill file
        result = self.connection.run(f'ls -l {to_be_removed_kill_file_path}', warn=True)
        if result.ok:
            result = self.connection.run(f'rm -v {to_be_removed_kill_file_path}', warn=True)
            if result.failed:
                raise OSError(f'Failed to remove the kill file by command: {to_be_removed_kill_file_path}')
        else:
            print(f'Kill file did not exist, path: {to_be_removed_kill_file_path}')
            print('Skip removing the kill file')

        # Start the application
        print('Staring the application ...')
        result = self.connection.run(f'nohup sh {exec_file_path} &', warn=True, pty=True, **kwargs)
        if result.failed:
            raise OSError(f'Failed to start the application by command: {result.command}')

        # Confirm that the application is running
        max_limit_time = time.time() + 60
        while True:
            current_time = time.time()
            if current_time >= max_limit_time:
                raise OSError('Process could not be started within 60 seconds, Please check the server spec.')

            result = self.connection.run(f'ps -ef | grep {process_name_pattern} | grep -v grep', warn=True, **kwargs)
            if result.ok:  # => Process found
                print(f'Confirmed that the application has started by command: {result.command}')
                break
            else:
                print('Waiting for the application to start ...')
                time.sleep(1)
