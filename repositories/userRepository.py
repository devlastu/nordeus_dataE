import pymysql
from datetime import datetime
from databaseProcess import get_db_connection

SESSION_NUM = 0


class UserRepository:
    """Repository class to manage user-related database operations."""

    def __init__(self, connection=None):
        """
        Initializes the UserRepository with a database connection.

        Args:
            connection (optional): A custom database connection. If not provided, a default connection is used.
        """
        self.connection = connection or get_db_connection()

    def get_user_by_id(self, user_id):
        """
        Retrieves user information (registration event, country, timezone) based on the user ID.

        Args:
            user_id (int): The ID of the user.

        Returns:
            tuple: Contains event timestamp, country, and timezone if the user is found; None otherwise.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(""" 
                SELECT e.event_timestamp, r.country, t.timezone
                FROM registration_events r
                JOIN events e ON r.event_id = e.event_id
                JOIN timezones t ON r.country = t.country
                WHERE r.user_id = %s
            """, (user_id,))
            result = cursor.fetchone()
            return result if result else None

    def get_last_login_date(self, user_id):
        """
        Retrieves the last login date of the user based on their ID.

        Args:
            user_id (int): The ID of the user.

        Returns:
            tuple: Contains event timestamp, country, and timezone for the last login; None if no login is found.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(""" 
                SELECT e.event_timestamp, r.country, t.timezone
                FROM registration_events r
                JOIN events e ON r.event_id = e.event_id
                JOIN timezones t ON r.country = t.country
                WHERE r.user_id = %s
                ORDER BY e.event_timestamp DESC
                LIMIT 1
            """, (user_id,))
            result = cursor.fetchone()
            return result if result else None

    def get_sessions_for_user(self, user_id, date=None):
        """
        Retrieves the number of sessions for a user either on a specific date or overall.

        Args:
            user_id (int): The ID of the user.
            date (optional): The date for which to calculate the number of sessions (format 'YYYY-MM-DD').

        Returns:
            int: The number of sessions for the user.
        """
        with self.connection.cursor() as cursor:
            # Check if session count exists in the users table
            cursor.execute("""
                SELECT num_of_sessions
                FROM users
                WHERE id = %s
            """, (user_id,))
            result = cursor.fetchone()

            if result is not None and result[0] is not None:  # Check if result is not None
                return result[0]

            # If session count is not in the users table, calculate it
            if date:
                cursor.execute("""
                    SELECT COUNT(DISTINCT session_id) 
                    FROM session_events s
                    JOIN events e ON s.event_id = e.event_id
                    WHERE s.user_id = %s AND DATE(e.event_timestamp) = %s
                """, (user_id, date))
            else:
                cursor.execute("""
                    SELECT COUNT(DISTINCT session_id) 
                    FROM session_events 
                    WHERE user_id = %s
                """, (user_id,))

            result = cursor.fetchone()
            session_count = result[0] if result and result[0] is not None else 0

            # Update the number of sessions in the users table
            cursor.execute("""
                UPDATE users
                SET num_of_sessions = %s
                WHERE id = %s
            """, (session_count, user_id))

            return session_count

    def get_time_spent_in_game(self, user_id, date=None):
        """
        Retrieves the total time spent in the game for a user, either for a specific date or overall.

        Args:
            user_id (int): The ID of the user.
            date (optional): The date for which to calculate the time spent in the game (format 'YYYY-MM-DD').

        Returns:
            float: The total time in seconds spent in the game.
        """
        with self.connection.cursor() as cursor:
            # Check if time spent in game exists in the users table
            cursor.execute("""
                SELECT time_spent_in_game
                FROM users
                WHERE id = %s
            """, (user_id,))
            result = cursor.fetchone()

            if result is not None and result[0] is not None:  # Check if result is not None
                return result[0]

            cursor.execute("""
                            SELECT num_of_sessions
                            FROM users
                            WHERE id = %s
                        """, (user_id,))
            result = cursor.fetchone()
            sessions = result[0] if result and result[0] is not None else 0

            # Calculate the time spent in game if not found
            if date:
                cursor.execute("""
                    SELECT COUNT(se.session_id) AS session_count
                    FROM session_events se
                    INNER JOIN events e ON se.event_id = e.event_id
                    WHERE se.user_id = %s
                    AND DATE(FROM_UNIXTIME(e.event_timestamp)) = %s;
                """, (user_id, date))
            else:
                cursor.execute("""
                    SELECT COUNT(se.session_id) AS session_count
                    FROM session_events se
                    WHERE se.user_id = %s;
                """, (user_id,))

            result = cursor.fetchone()
            session_count = result[0] if result and result[0] is not None else 0

            # Calculate the total time spent in the game (assuming each session is 60 seconds)
            total_time_in_seconds = (session_count - sessions) * 60

            # Update the time spent in game in the users table
            cursor.execute("""
                UPDATE users
                SET time_spent_in_game = %s
                WHERE id = %s
            """, (total_time_in_seconds, user_id))

            return total_time_in_seconds

    def get_total_points(self, user_id, date=None):
        """
        Retrieves the total points earned by a user, either for a specific date or overall.

        Args:
            user_id (int): The ID of the user.
            date (optional): The date for which to calculate the points (format 'YYYY-MM-DD').

        Returns:
            tuple: A tuple containing total home and away points.
        """
        with self.connection.cursor() as cursor:
            # Check if total_points_home and total_points_away exist in the users table
            cursor.execute("""
                SELECT total_points_home, total_points_away
                FROM users
                WHERE id = %s
            """, (user_id,))
            result = cursor.fetchone()

            if result and result[0] is not None and result[1] is not None:
                # If points are already in the database, return them
                return result[0], result[1]

            # If points are not in the database, calculate them
            if date:
                # Calculate points for home games
                cursor.execute("""
                    SELECT SUM(
                        CASE
                            WHEN home_user_id = %s AND home_goals_scored > away_goals_scored THEN 3
                            WHEN home_goals_scored = away_goals_scored THEN 1
                            ELSE 0
                        END
                    ) AS total_points_home
                    FROM match_events
                    WHERE home_user_id = %s
                    AND DATE(event_timestamp) = %s
                """, (user_id, user_id, date))
                result_home = cursor.fetchone()
                total_points_home = result_home[0] if result_home[0] else 0

                # Calculate points for away games
                cursor.execute("""
                    SELECT SUM(
                        CASE
                            WHEN away_user_id = %s AND away_goals_scored > home_goals_scored THEN 3
                            WHEN home_goals_scored = away_goals_scored THEN 1
                            ELSE 0
                        END
                    ) AS total_points_away
                    FROM match_events
                    WHERE away_user_id = %s
                    AND DATE(event_timestamp) = %s
                """, (user_id, user_id, date))
                result_away = cursor.fetchone()
                total_points_away = result_away[0] if result_away[0] else 0
            else:
                # Calculate points without a date filter
                cursor.execute("""
                    SELECT SUM(
                        CASE
                            WHEN home_user_id = %s AND home_goals_scored > away_goals_scored THEN 3
                            WHEN home_goals_scored = away_goals_scored THEN 1
                            ELSE 0
                        END
                    ) AS total_points_home
                    FROM match_events
                    WHERE home_user_id = %s
                """, (user_id, user_id))
                result_home = cursor.fetchone()
                total_points_home = result_home[0] if result_home[0] else 0

                cursor.execute("""
                    SELECT SUM(
                        CASE
                            WHEN away_user_id = %s AND away_goals_scored > home_goals_scored THEN 3
                            WHEN home_goals_scored = away_goals_scored THEN 1
                            ELSE 0
                        END
                    ) AS total_points_away
                    FROM match_events
                    WHERE away_user_id = %s
                """, (user_id, user_id))
                result_away = cursor.fetchone()
                total_points_away = result_away[0] if result_away[0] else 0

            # Update the total points in the users table
            cursor.execute("""
                UPDATE users
                SET total_points_home = %s, total_points_away = %s
                WHERE id = %s
            """, (total_points_home, total_points_away, user_id))
            self.connection.commit()
            return total_points_home, total_points_away

    def get_match_time(self, user_id, date=None):
        """
        Calculates the total match time for a user based on match events.

        Args:
            user_id (int): The ID of the user.
            date (optional): The date for which to calculate the match time (format 'YYYY-MM-DD').

        Returns:
            float: The total match time in seconds.
        """
        total_match_time = 0
        with self.connection.cursor() as cursor:
            if date:
                cursor.execute("""
                    SELECT me.match_id, e.event_timestamp 
                    FROM match_events me
                    JOIN events e ON me.event_id = e.event_id
                    WHERE (me.home_user_id = %s OR me.away_user_id = %s)
                    AND DATE(e.event_timestamp) = %s
                    ORDER BY me.match_id, e.event_timestamp
                """, (user_id, user_id, date))
            else:
                cursor.execute("""
                    SELECT me.match_id, e.event_timestamp 
                    FROM match_events me
                    JOIN events e ON me.event_id = e.event_id
                    WHERE me.home_user_id = %s OR me.away_user_id = %s
                    ORDER BY me.match_id, e.event_timestamp
                """, (user_id, user_id))

            match_events = cursor.fetchall()

            current_match_id = None
            start_time = None
            end_time = None

            for match_id, event_timestamp in match_events:
                # If event_timestamp is UNIX timestamp, convert to datetime
                if isinstance(event_timestamp, int):
                    event_timestamp = datetime.fromtimestamp(event_timestamp)

                if current_match_id != match_id:
                    if start_time and end_time:
                        match_duration = (end_time - start_time).total_seconds()
                        total_match_time += match_duration

                    current_match_id = match_id
                    start_time = event_timestamp
                    end_time = event_timestamp
                else:
                    end_time = event_timestamp

            # For the last match, calculate duration
            if start_time and end_time:
                match_duration = (end_time - start_time).total_seconds()
                total_match_time += match_duration

        return float(total_match_time)
