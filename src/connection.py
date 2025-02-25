## importamos librerias
import mysql.connector
import pandas as pd

##-----------------------------------------------------------------
##configuracion de mysql

config = {"host":"localhost","port":3306,"user": "root", "password":"","database":"proyectoetl"}

##------------------------------------------------------------------
## Nombre de la tabla y csv

table = 'lung_cancer'
csv_file= './DATASET/lung_cancer_prediction_dataset.csv'

##------------------------------------------------------------------
##conexion con mysql

conn = mysql.connector.connect(host=config["host"],port=config["port"], user=config["user"], password=config["password"])
cursor= conn.cursor()

##------------------------------------------------------------------
## creacion de base de datos si no existe

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
conn.database = config["database"]

##------------------------------------------------------------------
## Creacion de tabla

cursor.execute(""" 
CREATE TABLE IF NOT EXISTS lung_cancer (
               ID INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
               Country VARCHAR(50),
               Population_Size FLOAT,
               Age INT,
               Gender VARCHAR(10),
               Smoker VARCHAR(10),
               Years_of_Smoking INT,
               Cigarettes_per_Day INT,
               Passive_Smoker VARCHAR(10),
               Family_History VARCHAR(10),
               Lung_Cancer_Diagnosis VARCHAR(10),
               Cancer_Stage VARCHAR(10),
               Survival_Years INT,
               Adenocarcinoma_Type VARCHAR(10),
               Air_Pollution_Exposure ENUM('Low','Medium','High'),
               Occupational_Exposure VARCHAR(10),
               Indoor_Pollution VARCHAR(10),
               Healthcare_Access ENUM('Poor','Good'),
               Early_Detection VARCHAR(10),
               Treatment_Type ENUM('None','Surgery','Radiotherapy','Chemotherapy'),
               Developed_or_Developing ENUM('Developing','Developed'),
               Annual_Lung_Cancer_Deaths INT,
               Lung_Cancer_Prevalence_Rate FLOAT,
               Mortality_Rate FLOAT);
""")

conn.commit()
print ("La base de datos y tabla se ha creado con exito")

##---------------------------------------------------------------------------------------
##carga de csv 

df= pd.read_csv(csv_file)

cursor.execute("SELECT COUNT(*) FROM lung_cancer")
resultado = cursor.fetchone()
if resultado[0] == 0:
    for _, row in df.iterrows():
        cursor.execute("""
                   INSERT INTO lung_cancer (Country, Population_Size, Age,Gender, Smoker, Years_of_Smoking, Cigarettes_per_Day, Passive_Smoker, Family_History, Lung_Cancer_Diagnosis, Cancer_Stage, Survival_Years, Adenocarcinoma_Type, Air_Pollution_Exposure, Occupational_Exposure, Indoor_Pollution, Healthcare_Access, Early_Detection, Treatment_Type, Developed_or_Developing, Annual_Lung_Cancer_Deaths, Lung_Cancer_Prevalence_Rate, Mortality_Rate)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (row['Country'],row['Population_Size'],row['Age'],row['Gender'],row['Smoker'],row['Years_of_Smoking'],row['Cigarettes_per_Day'],row['Passive_Smoker'],row['Family_History'],row['Lung_Cancer_Diagnosis'],row['Cancer_Stage'],row['Survival_Years'],row['Adenocarcinoma_Type'],row['Air_Pollution_Exposure'],row['Occupational_Exposure'],row['Indoor_Pollution'],row['Healthcare_Access'],row['Early_Detection'],row['Treatment_Type'],row['Developed_or_Developing'],row['Annual_Lung_Cancer_Deaths'],row['Lung_Cancer_Prevalence_Rate'],row['Mortality_Rate']))
    conn.commit()
    print("Dato cargados correcctamente")
else:
    conn.commit()
    print("No es necesario volver a cargar los datos")

cursor.close()
conn.close()