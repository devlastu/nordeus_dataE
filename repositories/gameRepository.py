import pymysql
from databaseProcess import get_db_connection
from repositories.userRepository import UserRepository


class GameRepository:
    """Repository class to manage game-related database operations."""

    def __init__(self, connection=None, user_repo=None):
        """
        Initializes the GameRepository with a database connection and a user repository.

        Args:
            connection (optional): A custom database connection. If not provided, a default connection is used.
            user_repo (optional): A custom UserRepository. If not provided, a default one is used.
        """
        self.connection = connection or get_db_connection()
        self.user_repo = user_repo or UserRepository()  # Initialize UserRepository if not passed

    def get_daily_active_users(self, date=None):
        """
        Returns the number of daily active users for a specific date or overall if no date is provided.

        Args:
            date (optional): The date for which to calculate daily active users (format 'YYYY-MM-DD').

        Returns:
            int: The count of daily active users.
        """
        with self.connection.cursor() as cursor:
            if date:
                # Query to count active users for the specified date
                cursor.execute("""
                    SELECT COUNT(DISTINCT user_id) AS daily_active_users
                    FROM session_events
                    WHERE DATE(event_timestamp) = %s
                """, (date,))
            else:
                # Query to count total unique users who ever had a session
                cursor.execute("""
                    SELECT COUNT(DISTINCT user_id) AS daily_active_users
                    FROM session_events
                """)

            result = cursor.fetchone()
            return result[0] if result else 0

    def get_total_sessions(self, date=None):
        """
        Returns the total number of sessions for a specific date or overall if no date is provided.

        Args:
            date (optional): The date for which to calculate the total number of sessions (format 'YYYY-MM-DD').

        Returns:
            int: The total number of sessions.
        """
        with self.connection.cursor() as cursor:
            if date:
                # Query to find the maximum session_id for the specified date
                cursor.execute("""
                            SELECT MAX(session_id) AS session_count
                            FROM session_events
                            JOIN events ON session_events.session_event_id = events.event_id
                            WHERE DATE(events.event_timestamp) = %s
                        """, (date,))
            else:
                # Query to find the maximum session_id overall
                cursor.execute("""
                            SELECT MAX(session_id) AS session_count
                            FROM session_events
                            JOIN events ON session_events.session_event_id = events.event_id
                        """)

            result = cursor.fetchone()
            return result[0] if result else 0

    def get_avg_sessions_per_user(self, date=None):
        """
        Returns the average number of sessions per user for a specific date or overall if no date is provided.

        Args:
            date (optional): The date for which to calculate the average sessions per user (format 'YYYY-MM-DD').

        Returns:
            float: The average number of sessions per user.
        """
        with self.connection.cursor() as cursor:
            # Get the count of active users (those who have had at least one session)
            active_users_count = self.get_daily_active_users(date)

            if active_users_count == 0:
                return 0  # Return 0 if no active users

            # Get the total number of sessions for active users
            total_sessions = 0
            if date:
                cursor.execute("""
                    SELECT DISTINCT user_id
                    FROM session_events
                    JOIN events ON session_events.event_id = events.event_id
                    WHERE DATE(events.event_timestamp) = %s
                """, (date,))
            else:
                cursor.execute("""
                    SELECT DISTINCT user_id
                    FROM session_events
                    JOIN events ON session_events.event_id = events.event_id
                """)

            users = cursor.fetchall()

            # Sum the number of sessions for each user
            for user in users:
                user_id = user[0]
                total_sessions += self.user_repo.get_sessions_for_user(user_id, date)

            # Calculate the average sessions per user
            avg_sessions_per_user = total_sessions / active_users_count if active_users_count > 0 else 0
            avg_sessions_per_user = round(avg_sessions_per_user, 2)
            return avg_sessions_per_user

    def get_users_with_most_points(self, date=None):
        """
        Returns the list of users with the highest total points for a specific date or overall if no date is provided.

        Args:
            date (optional): The date for which to calculate the users with most points (format 'YYYY-MM-DD').

        Returns:
            tuple: A tuple containing a list of user IDs and the highest points.
        """
        with self.connection.cursor() as cursor:
            if date:
                # Query to get the users who have points on a specific date
                cursor.execute("""
                    SELECT DISTINCT id
                    FROM users
                    WHERE DATE(event_timestamp) = %s
                """, (date,))
            else:
                # Query to get all users who have ever had a session
                cursor.execute("""
                    SELECT DISTINCT id
                    FROM users
                """)

            users = cursor.fetchall()

            max_points = 0
            top_users = []

            # Iterate through each user to calculate their total points
            for user in users:
                user_id = user[0]
                user_points = self.user_repo.get_total_points(user_id, date)
                user_points = user_points[0] + user_points[1]  # Total points (home + away)

                # Check if the user has the highest points and update the list
                if user_points > max_points:
                    max_points = user_points
                    top_users = [user_id]
                elif user_points == max_points:
                    top_users.append(user_id)

            return top_users, max_points
