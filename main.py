from fastapi import FastAPI, Depends
from databaseProcess import initialize_database
from services.userService import UserService
from services.gameService import GameService
from fastapi.testclient import TestClient

# Define a lifespan context manager for initialization and shutdown
app = FastAPI()


# Dependency injection for UserService
def get_user_service():
    return UserService()


# Dependency injection for GameService
def get_game_service():
    return GameService()


# Endpoint to initialize the database
@app.get("/initialize")
async def initialize_db():
    """
       Endpoint to initialize the database. This function calls the 'initialize_database' function
       that creates the necessary tables and sets up initial data.

       Returns:
           dict: A message indicating the result of the database initialization.
    """
    print("Initializing database. Please wait...")
    initialize_database()  # Calls the function for database initialization
    return {"message": "Database initialized!"}


# Endpoint to get user stats
@app.get("/user_stats")
async def get_user_stats(user_id: str, date: str = None, user_service: UserService = Depends(get_user_service)):
    """
    Get the statistics of a user.

    Args:
        user_id (str): The ID of the user.
        date (str, optional): The date for which to fetch stats. Defaults to None.

    Returns:
        dict: User statistics data.
    """
    stats = user_service.get_user_stats(user_id, date)
    return stats


# Endpoint to get game stats
@app.get("/game_stats")
async def get_game_stats(date: str = None, game_service: GameService = Depends(get_game_service)):
    """
    Get the statistics for a game.

    Args:
        date (str, optional): The date for which to fetch stats. Defaults to None.

    Returns:
        dict: Game statistics data.
    """
    stats = game_service.get_game_stats(date)
    return stats

# Uncomment below for testing the application with the TestClient

# client = TestClient(app)
# # Calling the function like a real request
#
# response = client.get("/user_stats?user_id=52d65a1b-8012-934e-001b-19e6ba3cdc0e")
# for i, j in response.json().items():
#     print(f"{i}: {j}")
#     print("-------------------------")
#
# print("\n==============================================\n")
# print("GameStats")
# response = client.get("/game_stats")
# for i, j in response.json().items():
#     print(f"{i}: {j}")
#     print("-------------------------")
