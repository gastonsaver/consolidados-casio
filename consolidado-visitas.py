# Consolidados Ferrero
# Version: 1.0
# Fecha de creación: 16/8/2022
# Fecha de Actualización: 16/8/2022
# Creó: @gs
# Actualizó:

from secrets import choice
import pandas as pd
import numpy as np
from datetime import datetime, date
from openpyxl import load_workbook

from minio import Minio
from minio.error import S3Error
import importlib.machinery
import platform
import json
from sqlalchemy import *

# Importar librerias para el envio de email automático
import smtplib
from email.header import Header
from email.mime.text import MIMEText
import email.utils
from email.mime.multipart import MIMEMultipart

with open('_common-path.ini', 'r') as f:
    path = json.load(f)
f.close()

# Cargar la libreria mylib.py
if platform.system() == 'Windows':
    lib_path = path['windows_mysql']
else:
    lib_path = path['linux_mysql']

loader = importlib.machinery.SourceFileLoader('mylib', lib_path)
my = loader.load_module('mylib')

def send_mail():
    

    print('Enviando mail automático al destinatario')

    # El link de descarga del archivo
    download_link = config['link']
    pbi_link = config['pbi_link']
    view_url = f'https://view.officeapps.live.com/op/embed.aspx?src={download_link}&wdOrigin=BROWSELINK'
    # Modificar este link también en el HTML

    # Crear el contenedor del mensaje - el tipo MIME correcto es multipart/alternative.
    msg = MIMEMultipart('alternative')
    subject = config['email_subject'].format(config['client_name'] + ' ' + fecha)
    msg['Subject'] = Header(subject, 'utf-8')  # subject
    msg['From'] = email.utils.formataddr((config['email_sender'], mail_user))

    # Si fueran varios destinatarios se debe hacer una join con la lista
    msg['To'] = ', '.join(config['emails_list'])
    msg['CCO'] = ', '.join(config['emails_list_cco'])

    # Crear el cuerpo del mensaje (una versión texto y una versión HTML).
    # Este texto podria encontrarse en un archivo paralelo
    with open(config['cuerpo_mail'], 'r') as f:
        text = f.read().format(pbi_link, view_url, download_link)
    f.close()

    # Abrir el archivo que contiene el cógido HTML
    with open(config['html_text'], "r", encoding='utf-8') as f:
        content = f.read()
    f.close()

    html = content.replace('link_descarga', download_link)
    html = html.replace('view_url', view_url)
    html = html.replace('pbi_url', pbi_link)

    # Grabar el tipo MIME de ambas partes - text/plain y text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Adjuntar partes al contenedor del mensaje.
    # De acuerdo a RFC 2046, la última parte del mensaje multiparte, en este caso
    # el mensaje HTML, es mejor.
    msg.attach(part1)
    msg.attach(part2)

    with smtplib.SMTP_SSL(mail_server_url, mail_server_port) as server:
        server.login(mail_user, mail_pass)
        print('Inicio de sesión')
        server.sendmail(mail_user, config['emails_list'], msg.as_string())


# -----------------------------------------------------------------------------
# CODIGO PRINCIPAL

#Abre archivo de configuraciones de la presentación y direcciones de mail de envio automático
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)['consolidado-ferrero']
f.close()

db_name = config['db_name']

# Nombre del bucket y la carpeta donde se cargará el archivo en el minio-server
bucket_name = config['bucket_name']
folder = config['minio_folder']

inicio = datetime.now()
fecha = date.today().strftime("%d/%m/%Y")

# Cargar parametros de autorizacion
auth = my.Authorization()
s3_server = auth.s3_server
s3_user = auth.s3_user
s3_pass = auth.s3_pass
involves_server = auth.involves_server
involves_port = auth.involves_port
involves_user = auth.involves_user
involves_pass = auth.involves_pass
mail_server_url = auth.reports_mail_server_url
mail_server_port = auth.reports_mail_server_port
mail_user = auth.reports_mail_user
mail_pass = auth.reports_mail_pass

# Crear el cursor de conexion usando la funcion previamente definida
sql = my.SQL(involves_server, involves_port, involves_user, involves_pass)
cnx = sql.cnx
sql.cursor_execute('USE {}'.format(db_name))

db_name = config['db_name']
sql_alch = my.SQLALCHEMY(involves_server, involves_port, involves_user, involves_pass)
cnx_alch = sql_alch.engine.connect().execution_options(autocommit=True)
cnx_alch.execute(text('USE {}'.format(db_name)))

# Descargar puntos de venta
query = "SELECT id, code, name as pos, companyName, address, number, city, state FROM PointOfSaleView WHERE deleted=0"

# Cargamos la tabla en un dataframe
pos = pd.read_sql(sql=query, con=cnx_alch)
pos["code"] = pd.to_numeric(pos["code"], downcast='integer')
print("Puntos de venta descargados")

# Descargar Form Mercado Base
query = config['survey_query']
# Cargamos la tabla en un dataframe
surveys = pd.read_sql(sql=query, con=cnx_alch)
surveys['valueEng'] = np.where(surveys['valueEng'] == 'false', 'NO', surveys['valueEng'])
surveys['valueEng'] = np.where(surveys['valueEng'] == 'true', 'YES', surveys['valueEng'])

# Realizamos el pivoting
surveys = surveys.pivot(values='valueEng',
                        index=['id','repliedAt', 'pointOfSale_id'],
                        columns=['FormFieldNameEng'])

surveys.reset_index(inplace=True)
surveys['repliedAt'] = pd.to_datetime(surveys['repliedAt']).dt.date

create_pos_survey = surveys[surveys['pointOfSale_id'].isnull()]
surveys = surveys[~surveys['pointOfSale_id'].isnull()]

#surveys.drop(columns=['id'], inplace=True)
surveys.rename(columns={'id':'surveyId'}, inplace=True)
create_pos_survey.drop(columns=['pointOfSale_id'], inplace=True)
create_pos_survey.rename(columns={'id':'surveyId'}, inplace=True)

# Hacemos los joins con tabla de puntos de venta // Hay que hacer Join de las encuestas de puntos de venta y por separado con aquellas que tienen datos de alta de PDV
surveys = pd.merge(left=surveys,
                   right=pos,
                   how='inner',
                   left_on='pointOfSale_id',
                   right_on='id')
surveys.drop(columns=['id', 'number', 'code', 'pointOfSale_id'], inplace=True)

create_pos_survey = pd.merge(left=create_pos_survey,
                             right=pos,
                             how='left',
                             left_on='surveyId',
                             right_on='code')
create_pos_survey['address'] = create_pos_survey['address'] + ' ' + create_pos_survey['number']
create_pos_survey['Is the POS Open?'] = 'YES'
create_pos_survey.drop(columns=['id', 'number', 'code'], inplace=True)

surveys = pd.concat([surveys, create_pos_survey], ignore_index=True)
surveys = surveys[surveys['Is the POS Open?'] == 'YES']
surveys.drop(columns=['Is the POS Open?'], inplace=True)

# Unificar columnas similares
surveys['I agree to receive informative emails from CASIO'] = np.where(
    surveys['I agree to receive informative emails from CASIO'] == '',
    surveys['I agree to receive informational emails from CASIO - Does not market'],
    surveys['I agree to receive informative emails from CASIO'])
surveys['POS location'] = np.where(
    surveys['POS location'] == '',
    surveys['Location'],
    surveys['POS location'])
surveys['phone'] = np.where(
    surveys['Phone number'] == '',
    surveys['Telephone Number - Does not sell'],
    surveys['Phone number'])
surveys['email'] = np.where(
    surveys['e-mail'] == '',
    surveys['email - Does not sell'],
    surveys['e-mail'])
surveys['Contact Name'] = np.where(
    surveys['Name last Name'] == '',
    surveys['Name/Surname - Does not sell'],
    surveys['Name last Name'])

surveys.drop(columns=['repliedAt', 'pos', 'I agree to receive informational emails from CASIO - Does not market', 'Location', 'Telephone Number - Does not sell', 'email - Does not sell', 'e-mail', 'Name/Surname - Does not sell', 'Name last Name', 'Phone number', 'Reasons why the training was not carried out'], inplace=True)

surveys.rename(columns=
               {
                'companyName': 'POS Name',
                'POS type Others': 'POS Type Others'
                },
               inplace=True)

# Crear la columna period a partir de la cantidad de visitas mensuales que se definen por presupuesto
condlist = [surveys['surveyId'] <= 37244082]
choicelist = [datetime.strptime('1/8/2022', '%d/%m/%Y').date()]

surveys['period'] = np.select(condlist=condlist, choicelist=choicelist, default=datetime.strptime('1/9/2022', '%d/%m/%Y').date())


# Ordenar columnas
order = ['surveyId',
         'period',
         'POS Name',
         'POS Type',
         'POS Type Others',
         'POS location',
         'address',
         'city',
         'state',
         'phone',
         'email',
         'Contact Name',
         'Does it have a window?',
         'Window size (meters)',
         'Do you sell calculators?',
         'If you do not have calculators, are you interested in selling Casio?',
         'Can the survey be carried out?',
         'Reason why survey cannot be performed',
         'Comment - Does not carry out a survey Others',
         'Total Size (m2)',
         'Number of salesman',
         'Do you have e-commerce or social networks? (If you do not have these means, leave blank)',
         'E-commerce (Indicate link)',
         'Instagram (Indicate link)',
         'Facebook (Indicate link)',
         'Standard Calculators',
         'Scientific Calculators',
         'Printers Calculators',
         'Do you sell Casio calculators?',
         'If you do not have Casio products, are you interested in marketing them?',
         'Provide Sales Information?',
         'How many units do you sell monthly?',
         'Standard Calculators - Casio Units',
         'Standard Calculators - Units Other Brands',
         'Scientific Calculators - Casio Units',
         'Scientific Calculators - Units Other Brands',
         'Printers Calculators - Casio Units',
         'Printers Calculators - Units Other Brands',
         'During the Start of the School Period (Units)',
         'During the rest of the year (Units)',
         'Who are your main Clients?',
         'Other clients',
         'What brand and model is the best seller?',
         'What is the main reason for choosing the brand and model in general?',
         'Do you know the main differences between the models, apart from the number of functions?',
         'Are Counterfeits/Copies a problem near your business?',
         'Where can we buy this type of product?',
         'Product Display',
         'Casio Display Units',
         'Exhibited Units Other Brands',
         'Do you have a problem with the distribution?',
         'Other problems',
         'Does the distributor provide you with POP material? (Catalogue, material for exhibition, etc.)',
         'Do you receive or did you receive any training on the products?',
         'When was the last training?',
         'Training Frequency',
         'How do you decide which products to buy?',
         'Purchase Decision Others',
         'How do you find out about new releases?',
         'New Releases Others',
         'Do you have a shopping platform?',
         'Other Platforms',
         'Do you have delivery service?',
         'Do you offer free shipping?',
         'Do you know the LAX line?',
         'Do you market the LAX line?',
         'Other reasons for choice',
         'Was the activation possible?',
         'Was the training possible?',
         'Branding tips that can help you improve sales',
         'General Comments of the Visit',
         'I agree to receive informative emails from CASIO'
]
surveys = surveys[order]

print("Encuestas descargadas")

# Grabar el dataframe en un archivo Excel en ubicación carpeta SharePoint
file_name = 'consolidado_visitas.xlsx'

book = load_workbook(file_name)
with pd.ExcelWriter(file_name, engine='openpyxl') as file:
    file.book = book
    file.sheets = dict((ws.title, ws) for ws in book.worksheets)
    surveys.to_excel(file, sheet_name='surveys', index=False)

print("Generando archivo", file_name)
fin = datetime.now()
print("Generado con éxito")

# Definir el cliente de conexión
print("Subiendo archivo a MinIO")
minio_client = Minio(s3_server, s3_user, s3_pass, secure=True)

# Nombre del Bucket y la carpeta donde se almacenará el archivo
object_name = folder + "/" + file_name

# Subir el archivo
try:
    minio_client.fput_object(bucket_name, object_name, file_name,)
    print("El archivo fue cargado con éxito")
    send_mail()
except S3Error as exc:
    print("Ha ocurrido un error.", exc)

fin = datetime.now()
tiempo = fin - inicio
print("Tiempo total", tiempo)
print()
