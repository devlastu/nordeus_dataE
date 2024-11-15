from factories.statCalcFactory import UserStatsFactory
from repositories.userRepository import UserRepository

class UserService:
    def __init__(self, user_repo=None):
        """
        Initializes the UserService instance.

        Args:
            user_repo (UserRepository, optional): The user repository to fetch user data.
                                                   Defaults to UserRepository() if not provided.
        """
        self.user_repo = user_repo or UserRepository()  # Use provided user repository or create a new one
        self.calculator_factory = UserStatsFactory(self.user_repo)  # Factory for creating user stat calculators

    def get_user_stats(self, user_id, date=None):
        """
        Retrieves user statistics by calculating stats for the given user and optional date.

        Args:
            user_id (str): The unique identifier of the user whose stats are to be fetched.
            date (str, optional): The date filter for the statistics. Defaults to None, meaning no filter.

        Returns:
            dict: A dictionary with user statistics, and errors if any calculators failed.
        """
        stats = {}  # Dictionary to hold the user statistics
        calculators = self.calculator_factory.create_calculators()  # Create calculators using the factory

        for calculator in calculators:
            try:
                result = calculator.calculate(user_id, date)  # Calculate user stats for the given user_id and date
                stats.update(result)  # Update the stats dictionary with the result from the calculator
            except Exception as e:
                calc_name = type(calculator).__name__  # Get the name of the calculator class
                stats[f"{calc_name}_error"] = str(e)  # If an error occurs, store it with the calculator's name

        return stats  # Return the compiled user statistics or errors

