import requests
import os
from dotenv import load_dotenv
import base64
load_dotenv()
import sqlite3
import json
import pandas as pd
from datetime import datetime
import time
from typing import Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from dataclasses import dataclass
from contextlib import contextmanager
import threading
from functools import wraps

# Data models
@dataclass
class User:
	uri: str
	name: str
	image_url: Optional[str] = None

@dataclass
class Track:
	uri: str
	name: str
	image_url: Optional[str] = None
	album_uri: Optional[str] = None
	album_name: Optional[str] = None
	artist_uri: Optional[str] = None
	artist_name: Optional[str] = None
	context_uri: Optional[str] = None
	context_name: Optional[str] = None
	context_index: Optional[int] = None

class SpotifyAnalyzer:
	def __init__(self, database_path: str, bearer_token: str):
		self.database_path = database_path
		self.bearer_token = bearer_token
		self.setup_logging()
		# Add thread-local storage for database connections
		self.thread_local = threading.local()
		self.init_database()
		self.verify_database_structure()

	def setup_logging(self):
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - %(message)s'
		)
		self.logger = logging.getLogger(__name__)

	def get_db_connection(self):
		"""Get a thread-specific database connection"""
		if not hasattr(self.thread_local, "connection"):
			conn = sqlite3.connect(self.database_path, timeout=30.0)  # Increase timeout
			conn.row_factory = sqlite3.Row
			self.thread_local.connection = conn
		return self.thread_local.connection

	def close_db_connection(self):
		"""Close the thread-specific database connection"""
		if hasattr(self.thread_local, "connection"):
			self.thread_local.connection.close()
			del self.thread_local.connection

	def with_db_retry(max_attempts=3, delay=1):
		"""Decorator to retry database operations with exponential backoff"""
		def decorator(func):
			@wraps(func)
			def wrapper(self, *args, **kwargs):
				last_error = None
				for attempt in range(max_attempts):
					try:
						return func(self, *args, **kwargs)
					except sqlite3.OperationalError as e:
						if "database is locked" in str(e):
							last_error = e
							wait_time = delay * (2 ** attempt)
							self.logger.warning(f"Database locked, retrying in {wait_time}s...")
							time.sleep(wait_time)
							# Close and remove the connection to force a new one
							self.close_db_connection()
							continue
						raise
				raise last_error
			return wrapper
		return decorator

	def init_database(self):
		"""Initialize SQLite database with required tables"""
		with self.get_db_connection() as conn:
			conn.executescript('''
				CREATE TABLE IF NOT EXISTS users (
					uri TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					image_url TEXT
				);

				CREATE TABLE IF NOT EXISTS tracks (
					uri TEXT PRIMARY KEY,
					name TEXT NOT NULL,
					image_url TEXT,
					album_uri TEXT,
					album_name TEXT,
					artist_uri TEXT,
					artist_name TEXT
				);

				CREATE TABLE IF NOT EXISTS user_tables (
					user_uri TEXT PRIMARY KEY,
					table_name TEXT NOT NULL,
					created_at INTEGER NOT NULL,
					FOREIGN KEY (user_uri) REFERENCES users (uri)
				);
			''')
			conn.commit()

	def get_user_table_name(self, user_uri: str) -> str:
		"""Generate a valid SQLite table name from user URI"""
		# Extract username from URI and sanitize it
		try:
			username = user_uri.split(':')[-1]
			# Replace invalid characters with underscores
			sanitized_name = ''.join(c if c.isalnum() else '_' for c in username)
			# Ensure the table name starts with a letter
			table_name = f"user_{sanitized_name}_listening_activity"
			# Validate the final table name
			if not table_name.replace('_', '').isalnum():
				raise ValueError(f"Invalid table name generated from URI: {user_uri}")
			return table_name
		except Exception as e:
			self.logger.error(f"Error generating table name for URI {user_uri}: {str(e)}")
			raise

	@with_db_retry()
	def create_user_table(self, user_uri: str) -> str:
		"""Create a user-specific listening activity table if it doesn't exist"""
		try:
			table_name = self.get_user_table_name(user_uri)
			
			conn = self.get_db_connection()
			cursor = conn.cursor()
			
			# Check if table exists
			cursor.execute(f'''
				SELECT name FROM sqlite_master 
				WHERE type='table' AND name=?
			''', (table_name,))
			
			if not cursor.fetchone():
				self.logger.info(f"Creating new table for user {user_uri}: {table_name}")
				# Create the user-specific table
				cursor.execute(f'''
					CREATE TABLE IF NOT EXISTS {table_name} (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						timestamp INTEGER NOT NULL,
						track_uri TEXT NOT NULL,
						context_uri TEXT,
						context_name TEXT,
						context_index INTEGER,
						FOREIGN KEY (track_uri) REFERENCES tracks (uri)
					)
				''')
				
				# Create index on timestamp
				cursor.execute(f'''
					CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp 
					ON {table_name}(timestamp)
				''')
				
				# Record the table creation in user_tables
				cursor.execute('''
					INSERT OR REPLACE INTO user_tables (user_uri, table_name, created_at)
					VALUES (?, ?, ?)
				''', (user_uri, table_name, int(time.time())))
				
				conn.commit()
			
			return table_name
				
		except Exception as e:
			self.logger.error(f"Error creating table for user {user_uri}: {str(e)}")
			raise

	@with_db_retry()
	def store_activity(self, activity_data: Dict):
		"""Store activity data in the database"""
		try:
			# Validate activity_data structure
			if not isinstance(activity_data, dict):
				self.logger.error(f"Invalid activity data: {activity_data}")
				return

			# Log the incoming data
			# self.logger.info(f"Processing activity data: {json.dumps(activity_data, indent=2)}")

			user_data = activity_data.get("user", {})
			track_data = activity_data.get("track", {})
			album_data = track_data.get("album", {})
			artist_data = track_data.get("artist", {})
			context_data = track_data.get("context", {})

			# Ensure required fields are present
			if not user_data or not track_data:
				self.logger.error(f"Missing required fields in activity data: {activity_data}")
				return

			user = User(
				uri=user_data.get("uri", ""),
				name=user_data.get("name", "Unknown"),
				image_url=user_data.get("imageUrl") or user_data.get("image_url")
			)

			# Validate user URI
			if not user.uri or ':' not in user.uri:
				self.logger.error(f"Invalid user URI: {user.uri}")
				return

			track = Track(
				uri=track_data.get("uri", ""),
				name=track_data.get("name", "Unknown"),
				image_url=track_data.get("imageUrl") or track_data.get("image_url"),
				album_uri=album_data.get("uri"),
				album_name=album_data.get("name"),
				artist_uri=artist_data.get("uri"),
				artist_name=artist_data.get("name"),
				context_uri=context_data.get("uri"),
				context_name=context_data.get("name"),
				context_index=context_data.get("index")
			)

			self.logger.debug(f"Processing activity for user: {user.uri}")
			
			conn = self.get_db_connection()
			table_name = None  # Initialize outside try block
			
			try:
				# Get or create user-specific table
				table_name = self.create_user_table(user.uri)
				self.logger.debug(f"Using table {table_name} for user {user.uri}")

				# Store user and track data
				cursor = conn.cursor()
				cursor.execute('''
					INSERT OR REPLACE INTO users (uri, name, image_url)
					VALUES (?, ?, ?)
				''', (user.uri, user.name, user.image_url))

				cursor.execute('''
					INSERT OR REPLACE INTO tracks 
					(uri, name, image_url, album_uri, album_name, artist_uri, artist_name)
					VALUES (?, ?, ?, ?, ?, ?, ?)
				''', (track.uri, track.name, track.image_url, track.album_uri, 
					track.album_name, track.artist_uri, track.artist_name))

				# Store listening activity
				cursor.execute(f'''
					INSERT INTO {table_name}
					(timestamp, track_uri, context_uri, context_name, context_index)
					VALUES (?, ?, ?, ?, ?)
				''', (activity_data.get("timestamp"), track.uri,
					track.context_uri, track.context_name, track.context_index))

				conn.commit()

			except sqlite3.Error as e:
				self.logger.error(f"SQLite error while storing activity: {str(e)}")
				if table_name:
					self.logger.error(f"Table name: {table_name}")
				raise

		except Exception as e:
			self.logger.error(f"Error storing activity: {str(e)}")
			self.logger.error(f"Activity data: {json.dumps(activity_data, indent=2)}")
			raise

	def get_user_activity(self, user_uri: str, start_time: Optional[int] = None, 
					 end_time: Optional[int] = None) -> pd.DataFrame:
		"""Get listening activity for a specific user within a time range"""
		table_name = self.get_user_table_name(user_uri)
		
		query = f'''
			SELECT 
				la.timestamp,
				t.name as track_name,
				t.artist_name,
				la.context_name
			FROM {table_name} la
			JOIN tracks t ON la.track_uri = t.uri
			WHERE 1=1
		'''
		
		params = []
		if start_time:
			query += " AND la.timestamp >= ?"
			params.append(start_time)
		if end_time:
			query += " AND la.timestamp <= ?"
			params.append(end_time)
			
		query += " ORDER BY la.timestamp DESC"
		
		with self.get_db_connection() as conn:
			return pd.read_sql_query(query, conn, params=params)

	def get_all_user_activities(self, start_time: Optional[int] = None) -> pd.DataFrame:
		"""Get activities from all user-specific tables"""
		with self.get_db_connection() as conn:
			# Get all user tables
			cursor = conn.cursor()
			cursor.execute('SELECT user_uri, table_name FROM user_tables')
			user_tables = cursor.fetchall()
			
			if not user_tables:
				return pd.DataFrame()
			
			# Build UNION query for all user tables
			queries = []
			params = []
			
			for user_uri, table_name in user_tables:
				query = f'''
					SELECT 
						la.timestamp,
						u.name as user_name,
						t.name as track_name,
						t.artist_name,
						la.context_name
					FROM {table_name} la
					JOIN users u ON u.uri = ?
					JOIN tracks t ON la.track_uri = t.uri
					WHERE 1=1
				'''
				if start_time:
					query += " AND la.timestamp >= ?"
					params.extend([user_uri, start_time])
				else:
					params.append(user_uri)
				queries.append(query)
			
			# Combine all queries with UNION
			full_query = " UNION ALL ".join(queries) + " ORDER BY timestamp DESC"
			
			return pd.read_sql_query(full_query, conn, params=params)

	def analyze_recent_activity(self, minutes: int = 10) -> pd.DataFrame:
		"""Analyze recent listening activity within the specified time window"""
		cutoff_time = int(time.time() * 1000) - (minutes * 60 * 1000)
		return self.get_all_user_activities(start_time=cutoff_time)

	def get_hourly_activity_heatmap(self) -> pd.DataFrame:
		"""Generate hourly activity heatmap data"""
		with self.get_db_connection() as conn:
			# Get all user tables
			cursor = conn.cursor()
			cursor.execute('SELECT user_uri, table_name FROM user_tables')
			user_tables = cursor.fetchall()
			
			if not user_tables:
				return pd.DataFrame()
			
			# Build UNION query for all user tables
			queries = []
			params = []
			
			for user_uri, table_name in user_tables:
				query = f'''
					SELECT 
						strftime('%H', datetime(timestamp/1000, 'unixepoch')) as hour,
						COUNT(*) as activity_count,
						u.name as user_name
					FROM {table_name} la
					JOIN users u ON u.uri = ?
					GROUP BY hour, u.name
				'''
				params.append(user_uri)
				queries.append(query)
			
			# Combine all queries with UNION
			full_query = " UNION ALL ".join(queries) + " ORDER BY hour, user_name"
			
			return pd.read_sql_query(full_query, conn, params=params)

	def run_collection_loop(self, interval_seconds: int = 180):
		"""Run continuous data collection loop"""
		self.logger.info("Starting data collection loop...")
		while True:
			try:
				activities = self.fetch_friend_activity()
				if not activities:
					self.logger.info("No activities found in the API response.")
				else:
					for activity in activities:
						if isinstance(activity, dict):  # Ensure activity is a dictionary
							self.store_activity(activity)
						else:
							self.logger.error(f"Invalid activity format: {activity}")
					self.logger.info(f"Stored {len(activities)} activities")
				time.sleep(interval_seconds)
			except Exception as e:
				self.logger.error(f"Error in collection loop: {e}")
				time.sleep(interval_seconds)

	def filter_active_friends(self, friend_activity, active_threshold_seconds=300):
		"""Filter only active friends based on the timestamp."""
		current_time = time.time()
		active_friends = []

		for friend in friend_activity.get("friends", []):
			timestamp = friend.get("timestamp", 0) / 1000  # Convert to seconds
			if current_time - timestamp <= active_threshold_seconds:
				active_friends.append(friend)

		return active_friends
	@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
	def fetch_friend_activity(self) -> List[Dict]:
		encoded_url = "aHR0cHM6Ly9zcGNsaWVudC53Zy5zcG90aWZ5LmNvbS9wcmVzZW5jZS12aWV3L3YxL2J1ZGR5bGlzdA=="
		url = base64.b64decode(encoded_url).decode("utf-8")  # Base64 çözümleme		
		headers = {"Authorization": f"Bearer {self.bearer_token}"}
		try:
			
			response = requests.get(url, headers=headers)
			response.raise_for_status()
			data = response.json()
			# self.logger.info(f"API Response: {json.dumps(data, indent=2)}")  # Log the response
			return self.filter_active_friends(data)
		except requests.exceptions.RequestException as e:
			self.logger.error(f"Error fetching friend activity: {e}")
			return []

	def verify_database_structure(self):
		"""Verify and repair database structure if needed"""
		with self.get_db_connection() as conn:
			try:
				# Check for required tables
				cursor = conn.cursor()
				cursor.execute('''
					SELECT name FROM sqlite_master 
					WHERE type='table' AND 
					name IN ('users', 'tracks', 'user_tables')
				''')
				existing_tables = {row[0] for row in cursor.fetchall()}
				
				if len(existing_tables) < 3:
					self.logger.warning("Missing required tables, reinitializing database")
					self.init_database()
				
				# Verify user_tables entries
				cursor.execute('SELECT user_uri, table_name FROM user_tables')
				for user_uri, table_name in cursor.fetchall():
					# Check if the table exists
					cursor.execute(f'''
						SELECT name FROM sqlite_master 
						WHERE type='table' AND name=?
					''', (table_name,))
					if not cursor.fetchone():
						self.logger.warning(f"Missing table {table_name} for user {user_uri}")
						self.create_user_table(user_uri)
						
			except sqlite3.Error as e:
				self.logger.error(f"Error verifying database structure: {str(e)}")
				raise

	def get_all_time_activity(self) -> pd.DataFrame:
		"""Fetch all-time listening activity from the database"""
		with self.get_db_connection() as conn:
			# Get all user tables
			cursor = conn.cursor()
			cursor.execute('SELECT user_uri, table_name FROM user_tables')
			user_tables = cursor.fetchall()
			
			if not user_tables:
				return pd.DataFrame()
			
			# Build UNION query for all user tables
			queries = []
			params = []
			
			for user_uri, table_name in user_tables:
				query = f'''
					SELECT 
						la.timestamp,
						u.name as user_name,
						t.name as track_name,
						t.artist_name,
						t.album_name,
						la.context_name,
						strftime('%Y-%m-%d', datetime(timestamp/1000, 'unixepoch')) as date,
						strftime('%H', datetime(timestamp/1000, 'unixepoch')) as hour,
						strftime('%w', datetime(timestamp/1000, 'unixepoch')) as day_of_week
					FROM {table_name} la
					JOIN users u ON u.uri = ?
					JOIN tracks t ON la.track_uri = t.uri
				'''
				params.append(user_uri)
				queries.append(query)
			
			# Combine all queries with UNION
			full_query = " UNION ALL ".join(queries) + " ORDER BY timestamp DESC"
			
			return pd.read_sql_query(full_query, conn, params=params)

if __name__ == "__main__":
	# Initialize analyzer with your bearer token
	TOKEN = os.getenv("SPOTIFY_BEARER_TOKEN")
	analyzer = SpotifyAnalyzer(
		database_path="spotify_activity.db",
		bearer_token=TOKEN
	)
	
	# Start the collection loop
	analyzer.run_collection_loop()