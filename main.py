import os
import pandas as pd

# Definir la ruta de la carpeta
folder_path = 'Datos/Consolidado 2/Archivos'

# Obtener una lista de todos los archivos CSV en la carpeta
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Inicializar un DataFrame vacío para almacenar todos los datos transpuestos
all_transposed_data = pd.DataFrame()

# Leer y procesar cada archivo CSV
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    try:
        # Leer el archivo CSV con el delimitador adecuado
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Encontrar el índice de la fila "Country Name"
        start_index = None
        for i, line in enumerate(lines):
            if line.startswith('"Country Name"'):
                start_index = i
                break
        
        if start_index is None:
            print(f'No se encontró la fila "Country Name" en el archivo {csv_file}')
            continue
                
        # Leer el archivo CSV desde la fila "Country Name"
        data = pd.read_csv(file_path, encoding='utf-8', delimiter=',', quotechar='"', on_bad_lines='skip', skiprows=start_index)
        
        # Eliminar las filas vacías del DataFrame original
        data_cleaned = data.dropna(subset=[data.columns[0]])
        
        # Se define desde que columna lee los años
        years = data_cleaned.columns[4:]

        # Crear una lista para almacenar las filas transpuestas
        transposed_data = []

        # Iterar sobre cada fila del DataFrame original
        for index, row in data_cleaned.iterrows():
            country_name = row['Country Name']
            country_code = row['Country Code']
            indicator_name = row['Indicator Name']
            indicator_code = row['Indicator Code']
    
            # Iterar sobre las columnas de años
            for year in years[:-2]:  # Excluir 'Unnamed: 68' y 'Most_Recent_Value'
                value = row[year]
                if not pd.isna(value):
                    transposed_data.append([country_name, country_code, indicator_name, indicator_code, year, value])

        # Crear un nuevo DataFrame con los datos transpuestos
        transposed_df = pd.DataFrame(transposed_data, columns=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code', 'Year', 'Value'])

        # Concatenar los datos transpuestos al DataFrame principal
        all_transposed_data = pd.concat([all_transposed_data, transposed_df], ignore_index=True)
        
    except pd.errors.ParserError as e:
        print(f'Error al procesar el archivo {csv_file}: {e}')

# Ordenar el DataFrame combinado por el nombre de la columna "Country Name"
all_transposed_data.sort_values(by=['Country Name', 'Indicator Name'], inplace=True)

# Mostrar el DataFrame combinado ordenado
print(all_transposed_data.head())

# Guardar el DataFrame combinado en un archivo CSV
output_file_path = os.path.join(folder_path, 'transposed_data.csv')
all_transposed_data.to_csv(output_file_path, index=False)
print(f'El archivo CSV combinado se ha guardado en: {output_file_path}')