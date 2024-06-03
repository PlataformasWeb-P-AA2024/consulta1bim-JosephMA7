import csv
from pymongo import MongoClient

# Conexión al servidor MongoDB (asegúrate de que MongoDB esté en ejecución)
client = MongoClient('localhost', 27017)

# Creación de una base de datos llamada 'tennis'
db = client['tennis']

# Creación de una colección llamada 'Partidos' en la base de datos 'tennis'
collection = db['Partidos']

# Lectura del archivo CSV y almacenamiento de los datos en la colección
with open('atp_tennis.csv', 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        # Insertar cada fila como un documento en la colección
        collection.insert_one(row)

#=========================================================================================================
# Consulta para obtener todos los torneos únicos
distinct_tournaments = collection.distinct("Tournament")

# Enumeración de los torneos únicos
print("================")
print("\nTorneos únicos:\n")
print("================")

for idx, tournament in enumerate(distinct_tournaments, start=1):
    print(f"{idx}. {tournament}")

#=========================================================================================================
# Consulta para obtener todos los ganadores de la fecha '2013-01-17'
query_winners_2013_01_17 = {
    "Date": "2013-01-17"
}

# Proyección para incluir solo el nombre del ganador en los resultados
projection = {
    "_id": 0,
    "Winner": 1
}

# Se ejecuta la consulta y se recuperan los resultados
results_winners_2013_01_17 = collection.find(query_winners_2013_01_17, projection)

# Se presenta por consola la información de los ganadores en la fecha '2013-01-17'
print("====================================")
print("\nGanadores en la fecha '2013-01-17':\n")
print("====================================")

for result in results_winners_2013_01_17:
    print(result['Winner'])


# Cierre de la conexión al servidor MongoDB
client.close()