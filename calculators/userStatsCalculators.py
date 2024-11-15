from abc import abstractmethod, ABC
from datetime import datetime
import pytz

class UserStatsCalculator(ABC):
    """Abstract base class for user stats calculators."""
    @abstractmethod
    def calculate(self, user_id, date=None):
        """Method to be implemented by subclasses to calculate user stats."""
        pass

class UserCountryInfo(UserStatsCalculator):
    """Calculates the user's country and registration date."""
    def __init__(self, user_repo):
        """Initializes the calculator with a user repository."""
        self.user_repo = user_repo

    def calculate(self, user_id, date=None):
        """Fetches the user's country and registration time, and returns them."""
        # Fetch user data (timestamp, country, timezone)
        user_data = self.user_repo.get_user_by_id(user_id=user_id)
        registration_date_timestamp = user_data[0]
        country = user_data[1]
        timezone_str = user_data[2]

        # Convert registration_date from UNIX timestamp
        original_timezone = pytz.timezone(timezone_str)
        registration_date = datetime.fromtimestamp(registration_date_timestamp, tz=original_timezone)
        registration_time = registration_date.astimezone(original_timezone)

        # Format the registration date as a string
        formatted_registration_time = registration_time.strftime('%Y-%m-%d %H:%M:%S')

        # Return the country, formatted registration date, and timezone
        return {
            "country": country,
            "registration_datetime": formatted_registration_time,
            "timezone": timezone_str
        }

class LastLoginInfo(UserStatsCalculator):
    """Calculates the number of days since the user's last login."""
    def __init__(self, user_repo):
        """Initializes the calculator with a user repository."""
        self.user_repo = user_repo

    def calculate(self, user_id, date=None):
        """Fetches the last login timestamp and calculates days since last login."""
        data = self.user_repo.get_last_login_date(user_id=user_id)
        last_login_timestamp = data[0]
        timezone_str = data[2]

        # Check if the timezone string is valid
        try:
            timezone = pytz.timezone(timezone_str)
        except pytz.UnknownTimeZoneError:
            print(f"Invalid timezone: {timezone_str}, using UTC as fallback.")
            timezone = pytz.UTC

        # If the last login timestamp exists, convert it to datetime in the correct timezone
        if last_login_timestamp:
            last_login_date = datetime.fromtimestamp(last_login_timestamp, tz=timezone)
            last_login_date = datetime.fromisoformat(str(last_login_date))
            date_only = last_login_date.date()
            date_as_datetime = datetime(date_only.year, date_only.month, date_only.day)

            # Calculate the number of days since last login
            days_since_last_login = (datetime.today() - date_as_datetime).days
        else:
            # If no last login timestamp is available, return None
            days_since_last_login = None

        return {"days_since_last_login": days_since_last_login}


class UserSessionCount(UserStatsCalculator):
    """Calculates the number of sessions a user has had."""
    def __init__(self, game_repo):
        """Initializes the calculator with a game repository."""
        self.game_repo = game_repo

    def calculate(self, user_id, date=None):
        """Fetches the number of sessions for the user."""
        sessions = self.game_repo.get_sessions_for_user(user_id=user_id, date=date)
        return {"sessions": sessions}


class GameTimeSpent(UserStatsCalculator):
    """Calculates the total time a user has spent in the game."""
    def __init__(self, game_repo):
        """Initializes the calculator with a game repository."""
        self.game_repo = game_repo

    def calculate(self, user_id, date=None):
        """Fetches the total time the user has spent in the game."""
        time_spent = self.game_repo.get_time_spent_in_game(user_id=user_id, date=date)
        return {"time_spent_in_game": time_spent}


class PointsCalculator(UserStatsCalculator):
    """Calculates the total points a user has scored in home and away games."""
    def __init__(self, game_repo):
        """Initializes the calculator with a game repository."""
        self.game_repo = game_repo

    def calculate(self, user_id, date=None):
        """Fetches the total points for home and away games for the user."""
        total_points_home, total_points_away = self.game_repo.get_total_points(user_id=user_id, date=date)
        return {"total_points_home": total_points_home,
                "total_points_away": total_points_away}


class MatchParticipationCalculator(UserStatsCalculator):
    """Calculates the percentage of time a user spent in matches versus total game time."""
    def __init__(self, game_repo):
        """Initializes the calculator with a game repository."""
        self.game_repo = game_repo

    def calculate(self, user_id, date=None):
        """Fetches match time and total game time, then calculates the percentage."""
        match_time = self.game_repo.get_match_time(user_id=user_id, date=date)
        time_in_game =  self.game_repo.get_time_spent_in_game(user_id=user_id, date=date)

        # Calculate the percentage of time spent in matches
        match_time_percentage = (float(match_time) / time_in_game) * 100 if time_in_game > 0 else 0
        match_time_percentage = round(float(match_time_percentage), 2)

        return {"match_time_percentage": match_time_percentage}
