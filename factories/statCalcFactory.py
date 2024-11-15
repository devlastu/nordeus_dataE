from abc import abstractmethod, ABC
from calculators.gameStatsCalculators import *
from calculators.userStatsCalculators import *


class StatsFactory(ABC):
    @abstractmethod
    def create_calculators(self, calculators=None):
        """
        Create and return instances of requested calculators.

        Args:
            calculators (list, optional): List of calculator class names to instantiate.
                                          If None, all calculators will be instantiated.

        Returns:
            list: List of instantiated calculators.
        """
        pass


class GameStatsFactory(StatsFactory):
    def __init__(self, game_repo):
        """
        Initializes the GameStatsFactory with the game repository.

        Args:
            game_repo (GameRepository): The repository used for accessing game-related data.
        """
        self.game_repo = game_repo

    def create_calculators(self, calculators=None):
        """
        Creates and returns a list of game statistics calculators based on the requested ones.

        Args:
            calculators (list, optional): List of calculator class names to instantiate.
                                          If None, all calculators will be instantiated.

        Returns:
            list: List of instantiated calculators based on the requested ones or all calculators if none is specified.
        """
        all_calculators = {
            "DailyActiveUsers": DailyActiveUsers(self.game_repo),
            "TotalSessions": TotalSessions(self.game_repo),
            "AvgSessionsPerUser": AvgSessionsPerUser(self.game_repo),
            "UsersWithMostPoints": UsersWithMostPoints(self.game_repo),
        }

        # If specific calculators are requested, instantiate only those
        if calculators:
            return [all_calculators[calc] for calc in calculators if calc in all_calculators]

        # If no specific calculators are requested, return all
        return list(all_calculators.values())


class UserStatsFactory(StatsFactory):
    def __init__(self, user_repo):
        """
        Initializes the UserStatsFactory with the user repository.

        Args:
            user_repo (UserRepository): The repository used for accessing user-related data.
        """
        self.user_repo = user_repo

    def create_calculators(self, calculators=None):
        """
        Creates and returns a list of user statistics calculators based on the requested ones.

        Args:
            calculators (list, optional): List of calculator class names to instantiate.
                                          If None, all calculators will be instantiated.

        Returns:
            list: List of instantiated calculators based on the requested ones or all calculators if none is specified.
        """
        all_calculators = {
            "UserCountryInfo": UserCountryInfo(self.user_repo),
            "LastLoginInfo": LastLoginInfo(self.user_repo),
            "UserSessionCount": UserSessionCount(self.user_repo),
            "GameTimeSpent": GameTimeSpent(self.user_repo),
            "PointsCalculator": PointsCalculator(self.user_repo),
            "MatchParticipationCalculator": MatchParticipationCalculator(self.user_repo),
        }

        # If specific calculators are requested, instantiate only those
        if calculators:
            return [all_calculators[calc] for calc in calculators if calc in all_calculators]

        # If no specific calculators are requested, return all
        return list(all_calculators.values())
