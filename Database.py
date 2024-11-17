"""
author: Alexey Aleshchenok
date: 2023-14-11
"""

import win32con
import win32file
import win32event
import pickle
import os


class DataBase:
    """
    A simple in-memory key-value database.
    """
    def __init__(self):
        self.data = {}

    def set_value(self, key, value):
        """
        Sets the value for a given key. Returns False if the key already exists.
        """
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def get_value(self, key):
        """
        Gets the value associated with the given key. Returns None if the key does not exist.
        """
        if key in self.data:
            return self.data.get(key, None)

    def delete(self, key):
        """
        Deletes the value associated with the given key. Returns the deleted value, or None if the key does not exist.
        """
        if key in self.data:
            return self.data.pop(key, None)


class SerializedDataBase(DataBase):
    """
    A database that supports serialization to a file.
    """
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.file_mutex = win32event.CreateMutex(None, False, None)
        self.load()

    def save(self):
        """
        Saves the current state of the database to a file using pickle.
        """
        handle = None
        win32event.WaitForSingleObject(self.file_mutex, win32event.INFINITE)
        try:
            handle = win32file.CreateFile(self.filename, win32file.GENERIC_WRITE, 0, None,
                                          win32con.CREATE_ALWAYS, 0, None)
            data = pickle.dumps(self.data)
            win32file.WriteFile(handle, data)
        finally:
            win32file.CloseHandle(handle)
            win32event.ReleaseMutex(self.file_mutex)

    def load(self):
        """
        Loads the state of the database from a file using pickle.
        """
        win32event.WaitForSingleObject(self.file_mutex, win32event.INFINITE)
        try:
            if os.path.exists(self.filename):
                handle = win32file.CreateFile(self.filename, win32file.GENERIC_READ, 0, None,
                                              win32con.OPEN_EXISTING, 0, None)
                _, data = win32file.ReadFile(handle, os.path.getsize(self.filename))
                self.data = pickle.loads(data)
                win32file.CloseHandle(handle)
            else:
                self.data = {}
        finally:
            win32event.ReleaseMutex(self.file_mutex)


class SynchronizedDataBase(SerializedDataBase):
    """
    A thread-safe or process-safe database with read/write locks and limited concurrent reads.
    """
    def __init__(self, filename, mode):
        super().__init__(filename)
        self.read_limit = 10

        # Synchronization primitives
        if mode == 'threads':
            self.mutex = win32event.CreateMutex(None, False, None)
            self.read_semaphore = win32event.CreateSemaphore(None, self.read_limit, self.read_limit, None)
            self.write_lock = win32event.CreateMutex(None, False, None)
        elif mode == 'processes':
            self.mutex = win32event.CreateMutex(None, False, "Global\\DataMutex")
            self.read_semaphore = win32event.CreateSemaphore(None, self.read_limit, self.read_limit,
                                                             "Global\\ReadSemaphore")
            self.write_lock = win32event.CreateMutex(None, False, "Global\\WriteLock")

    def value_set(self, key, value):
        """
        Sets the value for a given key with write-lock and read-lock protection.
        """
        # Acquire write lock
        win32event.WaitForSingleObject(self.write_lock, win32event.INFINITE)
        # Acquire read lock
        win32event.WaitForSingleObject(self.mutex, win32event.INFINITE)
        for _ in range(self.read_limit):
            win32event.WaitForSingleObject(self.read_semaphore, win32event.INFINITE)

        # Perform the write operation
        result = super().set_value(key, value)
        self.save()

        # Release the read semaphore and mutex
        for _ in range(self.read_limit):
            win32event.ReleaseSemaphore(self.read_semaphore, 1)
        win32event.ReleaseMutex(self.mutex)
        win32event.ReleaseMutex(self.write_lock)
        return result

    def value_get(self, key):
        """
        Gets the value for a given key with read-lock protection.
        """
        # Acquire read semaphore
        win32event.WaitForSingleObject(self.read_semaphore, win32event.INFINITE)

        # Perform the read operation
        self.load()
        data = super().get_value(key)

        # Release read semaphore
        win32event.ReleaseSemaphore(self.read_semaphore, 1)
        return data


def value_delete(self, key):
    """
    Deletes the value for a given key with write-lock and read-lock protection.
    """
    # Acquire write lock
    win32event.WaitForSingleObject(self.write_lock, win32event.INFINITE)
    # Acquire read lock
    win32event.WaitForSingleObject(self.mutex, win32event.INFINITE)
    for _ in range(self.read_limit):
        win32event.WaitForSingleObject(self.read_semaphore, win32event.INFINITE)

    # Perform the delete operation
    result = super().delete(key)
    self.save()

    # Release the read semaphore and mutex
    for _ in range(self.read_limit):
        win32event.ReleaseSemaphore(self.read_semaphore, 1)
    win32event.ReleaseMutex(self.mutex)
    win32event.ReleaseMutex(self.write_lock)
    return result


if __name__ == "__main__":
    key_ = 0
    value_ = 1
    file_name = 'test.pkl'

    db1 = DataBase()
    assert db1.set_value(key_, value_) is True
    assert db1.get_value(key_) == value_
    db1.delete(key_)
    assert db1.get_value(key_) != value_

    db2 = SynchronizedDataBase(file_name, 'threads')
    assert db2.set_value(key_, value_) is True
    assert db2.get_value(key_) == value_
    db2.delete(key_)
    assert db2.get_value(key_) != value_

    db3 = SynchronizedDataBase(file_name, 'processes')
    assert db3.set_value(key_, value_) is True
    assert db3.get_value(key_) == value_
    db3.delete(key_)
    assert db3.get_value(key_) != value_
