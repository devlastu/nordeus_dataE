# Nordeus Data Challenge
This repository contains the solution for the Nordeus Data Challenge, focusing on analyzing game data to uncover patterns and optimize user experience. It includes data processing, machine learning models, and optimization techniques aimed at improving player retention and match performance.

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

