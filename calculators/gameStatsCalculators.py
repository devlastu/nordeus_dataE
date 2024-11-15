from abc import ABC, abstractmethod

# Abstract base class for Game Stats Calculators
class GameStatsCalculator(ABC):
    def __init__(self, game_repo):
        """
        Initializes the GameStatsCalculator with the game repository.

        Args:
            game_repo (GameRepository): The repository used for accessing game-related data.
        """
        self.game_repo = game_repo

    @abstractmethod
    def calculate(self, date=None):
        """
        Abstract method for calculating game statistics.

        Args:
            date (str, optional): The date for which to calculate the stats. Defaults to None.

        Returns:
            dict: A dictionary of calculated statistics.
        """
        pass


# Calculator for calculating Daily Active Users
class DailyActiveUsers(GameStatsCalculator):
    def calculate(self, date=None):
        """
        Calculates the number of active users on a specific day.

        Args:
            date (str, optional): The date for which to calculate the daily active users. Defaults to None.

        Returns:
            dict: A dictionary containing the number of active users.
        """
        active_users = self.game_repo.get_daily_active_users(date)
        return {'active_users': active_users}


# Calculator for calculating Total Sessions
class TotalSessions(GameStatsCalculator):
    def calculate(self, date=None):
        """
        Calculates the total number of game sessions on a specific day.

        Args:
            date (str, optional): The date for which to calculate the total sessions. Defaults to None.

        Returns:
            dict: A dictionary containing the total number of sessions.
        """
        number_of_sessions = self.game_repo.get_total_sessions(date)
        return {'number_of_sessions': number_of_sessions}


# Calculator for calculating Average Sessions per User
class AvgSessionsPerUser(GameStatsCalculator):
    def calculate(self, date=None):
        """
        Calculates the average number of sessions per user on a specific day.

        Args:
            date (str, optional): The date for which to calculate the average sessions per user. Defaults to None.

        Returns:
            dict: A dictionary containing the average number of sessions per user.
        """
        avg_sessions_for_users = self.game_repo.get_avg_sessions_per_user(date)
        return {'avg_sessions_for_users': avg_sessions_for_users}


# Calculator for calculating Users with Most Points
class UsersWithMostPoints(GameStatsCalculator):
    def calculate(self, date=None):
        """
        Calculates the users with the most points on a specific day.

        Args:
            date (str, optional): The date for which to calculate the users with the most points. Defaults to None.

        Returns:
            list: A list containing the users with the most points.
        """
        users_with_most_points, points = self.game_repo.get_users_with_most_points(date)
        # Returning a dictionary of users with their corresponding points
        return {'user_with_most_points' : user for user in users_with_most_points}
