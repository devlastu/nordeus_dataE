"""
This script processes event data, validates and inserts it into a MySQL database.
The main functionalities include handling registration, session, and match events,
validating their data, and performing database operations to store the events and
relevant user information.

The script is designed to:
1. Establish a connection to a MySQL database using credentials from environment variables.
2. Define event handler classes that validate and insert data into various event-related tables.
3. Handle errors and log error messages.
4. Define schema and table creation logic for a fresh start, including foreign key constraints and table drops.

Modules used:
- pymysql: For interacting with the MySQL database.
- json: For parsing event data in JSON format.
- os: For handling environment variables and file paths.
- dotenv: For loading environment variables from a .env file.
- pprint: For pretty printing data during debugging.

Note: All database credentials and configuration should be set via environment variables.
"""

import json
import os
from pprint import pprint
import pymysql
from pymysql.err import MySQLError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants for validation and error handling
VALID_DEVICE_OS = {'iOS', 'Android', 'Web'}
VALID_SESSION_EVENT_TYPES = {'session_start', 'session_end'}
VALID_COUNTRIES = {'US', 'UK', 'DE', 'FR', 'IT'}
ERROR_MESSAGES = []
REQUIRED_FIELDS = ['event_id', 'event_timestamp', 'event_type', 'event_data']
TEST_FILE = os.getenv('TEST_FILE')


def get_db_connection():
    """
    Establishes a connection to the MySQL database using credentials from environment variables.

    Returns:
        pymysql.connections.Connection: A connection object for interacting with the database.
        None: If the connection fails.
    """
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        return connection
    except MySQLError as e:
        print(f"Error connecting to the database: {e}")
        return None


def load_timezones_to_db(jsonl_file):
    """
    Loads timezone data from a JSONL file and inserts it into the 'timezones' table in the database.

    Args:
        jsonl_file (str): The path to the JSONL file containing timezone data.
    """
    db_connection = get_db_connection()
    cursor = db_connection.cursor()

    # Open the JSONL file and read the data
    with open(jsonl_file, 'r') as file:
        for line in file:
            data = json.loads(line.strip())

            country = data['country']
            timezone = data['timezone']

            # SQL query to insert timezone data
            insert_query = """
            INSERT INTO timezones (country, timezone)
            VALUES (%s, %s)
            """
            cursor.execute(insert_query, (country, timezone))

    # Commit changes to the database
    db_connection.commit()
    print(f"Data from {jsonl_file} has been successfully inserted into the database.")


def drop_and_create_tables():
    """
    Drops existing tables and creates new ones for the application schema.
    Ensures that the schema is reset before loading new data.
    """
    global ERROR_MESSAGES
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()

        try:
            # Disable foreign key checks to safely drop tables
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

            # Drop all necessary tables
            cursor.execute(
                "DROP TABLE IF EXISTS match_events, session_events, registration_events, events, users, timezones;")

            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

            # Create the necessary tables for the schema
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS users (
                    num INT AUTO_INCREMENT PRIMARY KEY,        
                    id VARCHAR(255) UNIQUE NOT NULL,           
                    num_of_sessions INT, 
                    time_spent_in_game INT, -- time spent in game in seconds
                    total_points_home INT ,
                    total_points_away INT,
                    match_time_percentage FLOAT 
                );
            """)
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS events (
                    event_id INT PRIMARY KEY,
                    event_type ENUM('registration', 'session_ping', 'match') NOT NULL,
                    event_timestamp INT NOT NULL
                )
            """)
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS timezones (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    country VARCHAR(50) UNIQUE NOT NULL,
                    timezone VARCHAR(255) NOT NULL
                );
            """)
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS registration_events (
                    registration_event_id INT AUTO_INCREMENT PRIMARY KEY,
                    event_id INT,
                    country VARCHAR(50),
                    user_id VARCHAR(255),
                    device_os ENUM('iOS', 'Android', 'Web'),
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
                    FOREIGN KEY (country) REFERENCES timezones(country)
                )
            """)
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS session_events (
                    session_event_id INT AUTO_INCREMENT PRIMARY KEY,
                    event_id INT,
                    user_num INT,
                    user_id VARCHAR(255),
                    session_id INT,
                    session_duration INT,
                    FOREIGN KEY (user_num) REFERENCES users(num),
                    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
                );
            """)
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS match_events (
                    match_event_id INT AUTO_INCREMENT PRIMARY KEY,
                    event_id INT,
                    match_id VARCHAR(255),
                    home_user_id VARCHAR(255),
                    away_user_id VARCHAR(255),
                    home_goals_scored INT,
                    away_goals_scored INT,
                    FOREIGN KEY (home_user_id) REFERENCES users(id),
                    FOREIGN KEY (away_user_id) REFERENCES users(id),
                    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
                )
            """)
            connection.commit()
            print("Tables successfully dropped and recreated!")
        except MySQLError as e:
            print("Error dropping and creating tables:" + str(e))
            ERROR_MESSAGES.append("Error dropping and creating tables:" + str(e))
        finally:
            cursor.close()
            connection.close()
    else:
        print("Error connecting to the database.")


class EventHandler:
    """
    Abstract base class for handling different types of events (Registration, Session, Match).
    Provides a common interface for all event handlers.
    """
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(EventHandler, cls).__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def handle(self, cursor, event_id, event_data, user_id=None):
        """
        Abstract method to handle the event. Should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement handle method")

    def validate(self, event_data):
        """
        Abstract method to validate the event data. Should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement validate method")


class RegistrationEventHandler(EventHandler):
    """
    Handles registration events by validating and inserting them into the database.
    """

    def validate(self, event_data):
        """
        Validates the registration event data.

        Args:
            event_data (dict): The event data to validate.

        Returns:
            bool: True if validation passes, False otherwise.
            str: A message indicating validation failure.
        """
        if not all(key in event_data for key in ['country', 'user_id', 'device_os']):
            return False, 'Missing fields in event_data for registration'
        if event_data['device_os'] not in VALID_DEVICE_OS:
            return False, f"Invalid device_os -->{event_data}->{event_data['device_os']}"
        return True, ""

    def handle(self, cursor, event_id, event_data, user_id=None):
        """
        Handles the insertion of registration event data into the database.

        Args:
            cursor (pymysql.cursors.Cursor): The database cursor for executing SQL queries.
            event_id (int): The event ID.
            event_data (dict): The event data to insert.
            user_id (str): The user ID associated with the event.
        """
        sql_registration = """INSERT INTO registration_events (event_id, country, user_id, device_os)
                              VALUES (%s, %s, %s, %s)"""
        cursor.execute(sql_registration, (event_id, event_data['country'], user_id, event_data['device_os']))


class SessionEventHandler(EventHandler):
    """
        Handles session_ping events by validating and inserting session-related data into the database.
    """

    def validate(self, event_data):
        """
            Validates the session event data.

            Args:
                event_data (dict): The session event data to validate.

            Returns:
                bool: True if validation passes, False otherwise.
                str: An error message if validation fails.
        """

        if 'user_id' not in event_data:
            return False, 'Missing user_id in event_data for session_ping'

    def handle(self, cursor, event_id, event_data, user_id=None):
        """
            Handles the insertion of a session event into the session_events table.

            Args:
                cursor (pymysql.cursors.Cursor): The database cursor for executing SQL queries.
                event_id (int): The event ID.
                event_data (dict): The session event data to insert.
                user_id (str): The user ID associated with the session event.
        """
        # Fetch user_num based on user_id from the users table
        sql_fetch_user_num = """SELECT num FROM users WHERE id = %s"""
        cursor.execute(sql_fetch_user_num, (user_id,))
        result = cursor.fetchone()

        if result is None:
            raise ValueError(f"User with ID {user_id} does not exist.")

        user_num = result[0]

        # Inserting session event into session_events table with user_id
        sql_session = """INSERT INTO session_events (event_id, user_num, user_id)
                         VALUES (%s, %s, %s)"""
        cursor.execute(sql_session, (event_id, user_num, user_id))

        # CTE for session_events value
        sql_cte_session = """
            WITH session_events_cte AS (
                SELECT
                    se.user_num,
                    e.event_timestamp,
                    CASE
                        WHEN e.event_timestamp - LAG(e.event_timestamp) OVER (PARTITION BY se.user_num ORDER BY e.event_timestamp) > 60
                        THEN 1
                        ELSE 0
                    END AS new_session
                FROM events e
                INNER JOIN session_events se ON e.event_id = se.event_id
                WHERE se.user_num = %s
            )
            SELECT 
                user_num,
                SUM(new_session) OVER (PARTITION BY user_num ORDER BY event_timestamp 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id,
                event_timestamp
            FROM session_events_cte
            ORDER BY user_num, event_timestamp;
        """
        # Execute the CTE query to process session_id values
        cursor.execute(sql_cte_session, (user_num,))



class MatchEventHandler(EventHandler):
    """
        Handles match events by validating and inserting match-related data into the database.
    """
    def validate(self, event_data):
        """
            Validates the match event data.

            Args:
                event_data (dict): The match event data to validate.

            Returns:
                bool: True if validation passes, False otherwise.
                str: An error message if validation fails.
        """
        if not all(key in event_data for key in ['match_id', 'home_user_id', 'away_user_id']):
            return False, 'Missing fields in event_data for match'
        # Checking for numeric values of goals
        if 'home_goals_scored' in event_data and (
                not isinstance(event_data['home_goals_scored'], int) or event_data['home_goals_scored'] < 0):
            return False, 'home_goals_scored must be a non-negative integer'

        if 'away_goals_scored' in event_data and (
                not isinstance(event_data['away_goals_scored'], int) or event_data['away_goals_scored'] < 0):
            return False, 'away_goals_scored must be a non-negative integer'
        return True, ""

    def handle(self, cursor, event_id, event_data, user_id=None):
        """
            Handles the insertion of a match event into the match_events table.

            Args:
                cursor (pymysql.cursors.Cursor): The database cursor for executing SQL queries.
                event_id (int): The event ID.
                event_data (dict): The match event data to insert.
                user_id (str): Optional user ID associated with the match event.
        """

        home_user_id = event_data['home_user_id']
        away_user_id = event_data['away_user_id']
        global ERROR_MESSAGES
        # Check if both home and away users are present in the database
        cursor.execute("SELECT COUNT(*) FROM users WHERE id IN (%s, %s)", (home_user_id, away_user_id))
        if cursor.fetchone()[0] < 2:
            ERROR_MESSAGES.append("One or both users not found in the database!")
            return

        sql_match = """INSERT IGNORE INTO match_events (event_id, match_id, home_user_id, away_user_id, home_goals_scored, away_goals_scored)
                       VALUES (%s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql_match,
                       (event_id, event_data['match_id'], home_user_id,
                        away_user_id, event_data.get('home_goals_scored', 0),
                        event_data.get('away_goals_scored', 0)))


# Function for inserting event into the database
def insert_event_to_db(connection, event):
    """
        Inserts an event into the database by first validating and using the appropriate handler for insertion.

        Args:
            connection (pymysql.connections.Connection): The database connection object.
            event (dict): The event data to insert.

        Returns:
            tuple: (bool, str) A status indicating whether the insertion was successful and any error message.
    """
    global ERROR_MESSAGES

    try:
        with connection.cursor() as cursor:
            # Check and add user only if event type is 'registration'
            user_id = None
            if event['event_type'] != 'match':
                user_id = event['event_data'].get('user_id')
                if user_id:
                    sql_user = """INSERT IGNORE INTO users (id) VALUES (%s)"""
                    cursor.execute(sql_user, (user_id,))

            # Record the event
            sql_event = """INSERT INTO events (event_id, event_type, event_timestamp)
                           VALUES (%s, %s, %s)"""
            cursor.execute(sql_event, (event['event_id'], event['event_type'], event['event_timestamp']))
            event_id = event['event_id']


            # Use the appropriate handler class for insertion
            handler = None

            if event['event_type'] == 'registration':
                handler = RegistrationEventHandler()
            elif event['event_type'] == 'session_ping':
                # print(f"Inserting session event: event_id={event_id}, user_id={user_id}, type={event.get('type', '')}")
                handler = SessionEventHandler()
            elif event['event_type'] == 'match':
                handler = MatchEventHandler()

            #Base validation
            for field in REQUIRED_FIELDS:
                if field not in event:
                    return False, f'Missing required field: {field}'
            if not isinstance(event['event_id'], int) or not isinstance(event['event_timestamp'], int):
                return False, 'event_id and event_timestamp must be integers'
            if event['event_type'] not in ['registration', 'session_ping', 'match']:
                return False, 'Invalid event_type'

            event_data = event.get('event_data', {})
            #Validate specific type of event and handle insertion
            if handler:
                handler.validate(event_data)
                handler.handle(cursor, event_id, event['event_data'], user_id)

            connection.commit()
            # print(f'Event {event["event_type"]} with ID {event_id} successfully inserted.')

    except pymysql.MySQLError as e:
        if e.args[0] == 1452 or e.args[0] == 1265:
            ERROR_MESSAGES.append("Error inserting event to database... Invalid value for os")
    except Exception as e:
        ERROR_MESSAGES.append("Error inserting event:" + str(e))





def process_file_and_insert(file_path):
    """
        Processes the events from a file and inserts them into the database, handling errors along the way.

        Args:
            file_path (str): The path to the file containing event data.
    """
    global ERROR_MESSAGES
    event_ids = set()
    connection = get_db_connection()
    if connection:
        try:
            connection.begin()

            #Counter for printing dots
            count = 0


            print("Database insertion started. Please wait...\nLoading", end='', flush=True)

            with open(file_path, 'r') as file:
                for line in file:
                    try:
                        event = json.loads(line.strip())  # Load JSON object
                        if event["event_id"] in event_ids:
                            ERROR_MESSAGES.append(f"Skiped insertion of event with id {event['event_id']}: duplicate")
                            continue
                        event_ids.add(event["event_id"])
                        insert_event_to_db(connection, event)

                        #This part is only for printing dots
                        count += 1
                        if count % 1000 == 0:  #
                            print(".", end='', flush=True)

                    except json.JSONDecodeError as e:

                        ERROR_MESSAGES.append(f"Error decoding JSON: {e}")
                    except Exception as e:

                        ERROR_MESSAGES.append(f"Error processing event: {e}")

            connection.commit()

        except Exception as e:

            ERROR_MESSAGES.append(f"Error processing the file: {e}")
            connection.rollback()  # Rollback any changes if an error occurs
        finally:
            # First print all errors before the loading dots
            print('\n')
            for error in ERROR_MESSAGES:
                print(error)


            connection.close()
    else:
        print("Unable to establish a connection with the database.")


#Helper function for counting unique events
def count_unique_event_ids(file_path):
    """
        Counts the unique event IDs in a given file.

        Args:
            file_path (str): The path to the file containing event data.

        Returns:
            int: The count of unique event IDs.
    """
    event_ids = set()


    with open(file_path, "r") as file:
        for line in file:
            try:

                data = json.loads(line.strip())


                event_ids.add(data["event_id"])
            except json.JSONDecodeError:
                print(f"Greška pri čitanju linije: {line}")
            except KeyError:
                print("Ključ 'event_id' nije pronađen u liniji:", line)

    event_ids = sorted(list(event_ids))
    print("Prvi: " + str(event_ids[0]) + "   Poslednji: " + str(event_ids[-1]))

    return len(event_ids)


def update_session_duration():
    """
        Helper function to update the session_duration column in the session_events table.
        Calculates the duration of each session based on the event_timestamp values from the events table.
        Commits the transaction after updating.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    # SQL query to calculate session duration and update the session_duration column
    sql_update_duration = """
            WITH session_duration_calculated AS (
                SELECT 
                    se.session_event_id,
                    TIMESTAMPDIFF(
                        SECOND, 
                        LAG(FROM_UNIXTIME(e.event_timestamp)) OVER (PARTITION BY se.user_num, se.session_id ORDER BY e.event_timestamp), 
                        FROM_UNIXTIME(e.event_timestamp)
                    ) AS session_duration
                FROM session_events se
                JOIN events e ON se.event_id = e.event_id
            )

            UPDATE session_events se
            JOIN session_duration_calculated sdc ON se.session_event_id = sdc.session_event_id
            SET se.session_duration = sdc.session_duration;
        """

    try:
        # Execute the query to update the session_duration column
        cursor.execute(sql_update_duration)
        # Commit the transaction to save changes
        connection.commit()
        print("session_duration column successfully updated.")
    except Exception as e:
        # Roll back in case of error to avoid partial updates
        connection.rollback()
        print(f"Error updating session_duration: {e}")



def fetch_session_events():
    """
    Connect to the database, fetch all session events, and return them as a list of dictionaries.

    Each dictionary in the list contains the keys: 'user_num', 'session_id', and 'session_duration'.

    Returns:
        list of dict: A list of dictionaries with session event data.
    """
    # Database connection
    connection = get_db_connection()
    connection.cursorclass = pymysql.cursors.DictCursor
    # Query to fetch the session_events data
    query = """
    SELECT user_num, session_id, session_duration, session_event_id 
    FROM session_events
    """

    try:
        # Execute the query and fetch the results
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        # Prepare session events list
        session_events_list = [
            {
                'user_num': row['user_num'],
                'session_id': row['session_id'],
                'session_event_id': row['session_event_id'],
                'session_duration': row['session_duration'] if row['session_duration'] is not None else 0
            }
            for row in rows
        ]

        # Store the latest session_id for each user
        previous_session_pings = {}
        maximum_session_id = 1  # Start with the first session ID

        # Set the session_id for the first event
        session_events_list[0]['session_id'] = maximum_session_id
        previous_session_pings[session_events_list[0]['user_num']] = 0

        # Iterate through the rest of the events
        for i in range(1, len(session_events_list)):
            event = session_events_list[i]
            user_num = event['user_num']

            # Check for new session based on duration
            if event['session_duration'] > 60:
                # If event belongs to a new session
                maximum_session_id += 1
                event['session_id'] = maximum_session_id
            elif user_num in previous_session_pings:
                # Assign previous session_id for small duration events
                previous_event = session_events_list[previous_session_pings[user_num]]
                event['session_id'] = previous_event['session_id']
            else:
                # Assign a new session_id if no previous session is found for the user
                maximum_session_id += 1
                event['session_id'] = maximum_session_id

            # Update the previous session index for this user
            previous_session_pings[user_num] = i

    finally:
        # Close the connection
        connection.close()

    return session_events_list


def update_session_ids_in_db(session_events_list):
    """
    Update the session_id values in the database based on the provided session_events_list.

    Args:
        session_events_list (list of dict): List containing session events with updated session_ids.
    """
    # Database connection
    connection = get_db_connection()
    connection.cursorclass = pymysql.cursors.DictCursor

    try:
        # Start a transaction for batch update
        with connection.cursor() as cursor:
            # Prepare the update queries for each session_event
            update_query = """
            UPDATE session_events 
            SET session_id = %s
            WHERE session_event_id = %s
            """
            # Iterate over the session events and update each record
            for event in session_events_list:
                cursor.execute(update_query, (event['session_id'], event['session_event_id']))

            # Commit the changes
            connection.commit()
    finally:
        # Close the connection
        connection.close()


# unique_event_count = count_unique_event_ids("events.jsonl")
# print(f"Broj jedinstvenih event_id: {unique_event_count}")

# Rename the function to initialize_database
def initialize_database():
    # Your original logic goes here
    drop_and_create_tables()
    load_timezones_to_db('data/timezones.jsonl')
    file_path = TEST_FILE  # Set the appropriate path to the file
    # Insert data into the database
    process_file_and_insert(file_path)
    update_session_duration()
    session_events = fetch_session_events()
    print("Updating session_events table...filling session_ids")
    update_session_ids_in_db(session_events)
