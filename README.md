# Nordeus Data Challenge
This repository contains the solution for the Nordeus Data Challenge, focusing on analyzing game data to uncover patterns and optimize user experience. It includes data processing, machine learning models, and optimization techniques aimed at improving player retention and match performance.

## System Design and Technology Choices

1.  **FastAPI and MySql**  
    This project is built around the FastAPI framework for its speed,
    simplicity, and excellent support for building RESTful APIs. It uses MySQL as the database
    for storing user and game data, as it is a reliable and widely-used relational database that integrates easily with Python applications.


2. **Factory Design Pattern**
    The project uses the Factory pattern to create different types of calculators. This pattern allows for flexible creation of objects without specifying the exact class of object that will be created. 
    When the services need a specific calculator (e.g., a user stats calculator or a game stats calculator), they request it from the appropriate factory.
    Each calculator communicates with the corresponding repository to retrieve or store data. 
    The repositories handle the interactions with the database and encapsulate the data access logic, ensuring that the services and calculators remain decoupled from the database implementation.
    By using the Factory pattern, we can easily add new types of calculators or change their implementation without modifying the core service logic. 
    This promotes flexibility and scalability in the system design.


3.  **Window Functions for Session Ping Events**
    For the processing of session ping events, **window functions** in SQL were used to efficiently track session behavior and calculate various time-based metrics for user sessions. 
    This approach was chosen for several reasons:
    - **Efficient Ranking and Partitioning**: Window functions allow for ranking, partitioning, and performing operations across rows related to a single user session 
        without needing to use complex joins or subqueries. By partitioning session data by user ID and ordering by timestamp, we could easily calculate metrics such as the time between successive pings 
        or the duration of a session.
    - **Real-Time Session Tracking**: With window functions, I efficiently calculated metrics like "time spent per session" or "session length" in a single query. 
        This avoids the overhead of running multiple queries and joins, making the process faster and more efficient, especially with large datasets.
    - **Row-Level Operations**: Window functions enabled me to perform operations on a row while still having access to other rows in the partition.
    - **Scalability**: Given the volume of session ping events expected in this application, window functions allowed me to process large datasets with minimal performance degradation.
        This was one of crucial steps in maintaining high performance while analyzing player session data in real-time.
        In summary, window functions were used to handle session ping events because they simplify the query structure, reduce the need for subqueries or joins, and optimize performance when working with large datasets.   

## Dependencies
- fastapi
- pymysql
- pytz
- uvicorn
- requests
- pytest
- httpx
- datetime
- json
- abc
- python-dotenv

### Struktura Projekta

- **`databaseProcess.py`** - Skripta koja obrađuje i popunjava inicijalnu bazu podataka.
- **`main.py`** - Glavna tačka ulaza u aplikaciju, obično postavlja FastAPI aplikaciju i definiše endpoint-e.
  
- **`/calculators/`** - Direktorijum sa logikom za izračunavanje statistika.
  - **`gameStatsCalculators.py`** - Sadrži funkcionalnosti za izračunavanje statistika vezanih za igre.
  - **`userStatsCalculators.py`** - Sadrži funkcionalnosti za izračunavanje statistika vezanih za korisnike.

- **`/factories/`** - Direktorijum koji sadrži repozitorijume za interakciju sa bazama podataka.
  - **`gameRepository.py`** - Odgovoran za interakciju sa podacima vezanim za igre.
  - **`userRepository.py`** - Odgovoran za interakciju sa podacima vezanim za korisnike.

- **`/services/`** - Direktorijum koji sadrži poslovnu logiku za izvršavanje upita vezanih za igre i korisnike.
  - **`gameService.py`** - Sadrži poslovnu logiku za izvršavanje upita vezanih za igre.
  - **`userService.py`** - Sadrži poslovnu logiku za izvršavanje upita vezanih za korisnike.

- **`requirements.txt`** - Lista svih zavisnosti projekta koja je potrebna za instalaciju putem `pip` komande.
## Installation

1. Clone the repository

2. Navigate to the project directory

3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. **Set Up MySQL Database**  
   Before running the application, ensure that your MySQL server is up and running. You need to create the necessary schema in MySQL by running the following SQL command:
   ```sql
   CREATE DATABASE data_project;

5. **Set Up MySQL Database**
    Make sure to setup enviroment variables in .env file

### 2. **Running the Application**  

## Running the Application

1. Start the FastAPI server:
    ```bash
    uvicorn main:app --reload
    ```

2. Once the server is running, you can access the API via:
    ```
    http://127.0.0.1:8000
    ```

3. The Swagger documentation for your API can be accessed at:
    ```
    http://127.0.0.1:8000/docs
    ```
## How to use

1. You can just run already configured fastApi configuration

2. You can run commad 
    ``` 
    uvicorn main:app --reload
    ```
3. Test aplication trough test client in main.py by just running main.py script

### 3. **API Usage**  

## API Usage
### 1. Initialize database

**Endpoint**: `/initialize`
* in case of larger data sets, watch python run console for info.

### 2. Get User-Level Stats

**Endpoint**: `/user_stats`

**Method**: `GET`

**Parameters**:
- `user_id` (required): Unique identifier for a user.
- `date` (optional): Date in the format `YYYY-MM-DD` to filter stats for a specific day.

### 3. Get Game-Level Stats
**Endpoint**: `/game-stats`

**Method**: `GET`

**Parameters**:
- `date` (optional): Date in the format `YYYY-MM-DD` to filter stats for a specific day.

