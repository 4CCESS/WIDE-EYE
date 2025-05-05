"""
UserManager: handles persistent user registration and authentication
using SQLite and PBKDF2-HMAC-SHA256 password hashing.
"""

import sqlite3, os, hashlib, binascii
from typing import Tuple, Optional
from dispatcher.config import DISPATCHER_CONFIG


class UserManager:
    """
    Manages user credentials: registration, secure storage, and authentication.
    """

    def __init__(self):
        """
        Initialize or create the users database.
        """
        db_path = DISPATCHER_CONFIG["user_db_path"]
        need_init = not os.path.exists(db_path)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        if need_init:
            self.conn.execute("""
              CREATE TABLE users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL
              )
            """)
            self.conn.commit()

    def _hash_password(self, password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """
        Generate or verify a PBKDF2-HMAC-SHA256 hash.

        Returns (salt, password_hash).
        """
        if salt is None:
            salt = binascii.hexlify(os.urandom(16)).decode()
        pwd_hash = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt.encode(), 100_000
        )
        return salt, binascii.hexlify(pwd_hash).decode()

    def register_user(self, username: str, password: str) -> Tuple[bool, str]:
        """
        Insert a new user with hashed password.
        Returns (success, message).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM users WHERE username = ?", (username,))
        if cursor.fetchone():
            return False, "Username already exists."
        salt, pwd_hash = self._hash_password(password)
        self.conn.execute(
            "INSERT INTO users(username, password_hash, salt) VALUES (?, ?, ?)",
            (username, pwd_hash, salt),
        )
        self.conn.commit()
        return True, "User registered."

    def authenticate_user(self, username: str, password: str) -> bool:
        """
        Verify a username/password against stored hash.
        Returns True if valid.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT password_hash, salt FROM users WHERE username = ?", (username,)
        )
        row = cursor.fetchone()
        if not row:
            return False
        stored_hash, salt = row
        _, test_hash = self._hash_password(password, salt)
        return test_hash == stored_hash
