import sqlite3

path = 'spotify_activity.db'

conn = sqlite3.connect(path)
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# Get all columns from the users table
cursor.execute("PRAGMA table_info(users);")
columns = cursor.fetchall()
print("Columns in users table:", columns)

# Get all data from the users table
cursor.execute("SELECT * FROM users;")
users = cursor.fetchall()
print("Users:", users)

# Get all data from the listening_activity table
cursor.execute("SELECT * FROM listening_activity;")
activities = cursor.fetchall()
conn.close()