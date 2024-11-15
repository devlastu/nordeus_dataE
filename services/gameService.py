from factories.statCalcFactory import GameStatsFactory
from repositories.gameRepository import GameRepository

class GameService:
    def __init__(self, game_repo=None, user_repo=None):
        """
        Initializes the GameService instance.

        Args:
            game_repo (GameRepository, optional): The game repository to fetch game data.
                                                   Defaults to GameRepository() if not provided.
            user_repo (UserRepository, optional): The user repository (not used in this case).
        """
        self.game_repo = game_repo or GameRepository()  # Use provided game repository or create a new one
        self.calculator_factory = GameStatsFactory(self.game_repo)  # Factory for creating stat calculators

    def get_game_stats(self, date=None):
        """
        Retrieves game statistics, calculating the stats based on available calculators.

        Args:
            date (str, optional): The date filter for the statistics. Defaults to None, meaning no filter.

        Returns:
            dict: A dictionary with game statistics, with potential errors in case of failures during calculation.
        """
        stats = {}  # Dictionary to hold the stats
        calculators = self.calculator_factory.create_calculators()  # Create the calculators using the factory

        for calculator in calculators:
            try:
                result = calculator.calculate(date=date)  # Calculate stats for the given date
                stats.update(result)  # Update the stats dictionary with the result from the calculator
            except Exception as e:
                calc_name = type(calculator).__name__  # Get the name of the calculator class
                stats[f"{calc_name}_error"] = str(e)  # If there's an error, store it with the calculator's name

        return stats  # Return the compiled statistics or errors

