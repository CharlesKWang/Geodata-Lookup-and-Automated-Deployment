#------------------------------------------------------------------------------
# Title:           Geodata Lookup & Automated Deployment
# Program Name:    GLAD.py
# Purpose:         Given a zipfile url, downloads and extracts the zip
#                  Proceeds to find a shp_file within it. Reads the shp_file,
#                  creates a sql connection and table based off its fields, 
#                  and inserts rows into the sql table.
#
# Author:          Charles Wang
#   
# Created:         08/07/2017
#------------------------------------------------------------------------------
import arcpy
from arcpy import env
import xml.etree.ElementTree as ET
import requests
import zipfile
import io
import os
import sys
import pyodbc
import datetime
import shutil
import configparser
from bs4 import BeautifulSoup

def directory_exists(dpath):
    """Function to check if a directory exists, and if not create it"""
    d = os.path.dirname(dpath)
    if not os.path.exists(d):
        os.makedirs(d)
        
def zip_downloader(link, download_folder, proxy_dict):
    """Downloads a zipfile to the local_path directory, and extracts it there"""
    file_name = link.split('/')[-1]
    print (file_name)
    directory_exists(download_folder)
    r = requests.get(link, proxies=proxy_dict, stream=True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    file_path = download_folder + file_name.strip('?r=1').strip('.zip') + '/' 
    z.extractall(file_path)
    z.close()
    print 'Unzipped at ' + file_path
    return file_path

def shp_file_locator(file_direc, file_name):
    """Searches for and returns a shapefile within the file_direc"""
    file_list = []
    #Searches through the directory
    for dirpath, dirnames, filenames in os.walk(file_direc):
        for file in filenames:
            file_list.append(os.path.join(file_direc,dirpath, file))
    #Finds shp files
    for file in file_list:
        if file[-4:] == '.shp' and file_name in file:
            shp_file = file
    print 'shp_file used is ' + shp_file
    return shp_file

def gdbfile_locator(file_direc):
    """Searches for and returns a geodatabase location within the file_direc"""
    file_list = []
    #Searches through the directory
    for dirpath, dirnames, filenames in os.walk(file_direc):
        for file in filenames:
            if '.gdb' in dirpath and os.path.join(file_direc, dirpath) not in file_list:
                file_list.append(os.path.join(file_direc,dirpath))
    return file_list

def variable_transform(fields):
    """converts the shapefile Esri variable types into SQL variable types"""
    # {field_name: variable_type}
    vartype = {}
    #Covert from esri variables to sql variables
    for point in fields:
        if point[1] == 'OID':
            vartype[point[0]] = 'PRIMARY KEY'
        elif point[1] == 'GlobalID':
            vartype[point[0]] = 'INTEGER'
        elif point[1] == 'Guid':
            vartype[point[0]] = 'INTEGER'
        elif point[1] == 'Integer':
            vartype[point[0]] = 'INTEGER'
        elif point[1] == 'SmallInteger':
            vartype[point[0]] = 'SMALLINT'
        elif point[1] == 'Single':
            vartype[point[0]] = 'FLOAT'
        elif point[1] == 'Double':
            vartype[point[0]] = 'FLOAT'
        elif point[1] == 'Geometry':
            vartype[point[0]] = 'GEOMETRY'
        elif point[1] == 'String':
            vartype[point[0]] = 'VARCHAR(255)'
        elif point[1] == 'Date':
            vartype[point[0]] == 'DATE'
    return vartype

def zip_urls(dataUrl, proxyDict):
    """Gets all the zipfile download urls from the open data page"""
    r = requests.get(dataUrl, proxies=proxyDict)
    soup = BeautifulSoup(r.content)
    
    links = soup.find_all('a', href=True)
    zip_urls = []
    base_url = 'http://www1.nyc.gov'
    
    #Include the pluto page directly, to avoid the authentication redirect
    #Also includes the districts page, due to a different naming convention
    refer_pages = ["/site/planning/data-maps/open-data/dwn-pluto-mappluto.page",
                  "/site/planning/data-maps/open-data/districts-download-metadata.page"]
    for link in links:
        if 'open-data/dwn' in link['href'] and link['href'] not in refer_pages:
            if '#' not in link['href']:
                refer_pages.append(link['href'])
    
    for link in links:
        if '/download/zip' in link['href']:
            file_name = link['href']
            zip_urls.append(base_url + file_name)
                      
    for page in refer_pages:
        r = requests.get(base_url + page, proxies=proxyDict)
        soup = BeautifulSoup(r.content)
        
        referLinks = soup.find_all('a', href=True)
        for link in referLinks:
            if '/download/zip' in link['href']:
                file_name = link['href']
                zip_urls.append(base_url + file_name)
    return zip_urls

def shp_file_upload(shp_file, dataset_name):
    """Reads and uploadas a shapefile to the table specified(which it drops and creates)"""
    fields = arcpy.ListFields(shp_file)
    #Fields and their variable type. List of tuples
    fields_list = []
    #Field names alone. List of strings/unicode
    field_names = []
    db = DBase()
    
    for field in fields:
        fields_list.append((field.baseName,field.type))
    
    for field in fields:
        if field.baseName == 'Shape':
            #Gets the WKT format for the shape
            field_names.append('SHAPE@WKT')
        else:
            field_names.append(field.baseName)
            
    vartype = variable_transform(fields_list)
        
    #Drops table first
    drop_table = "IF OBJECT_ID('dbo.{}', 'U') IS NOT NULL DROP TABLE {}".format(dataset_name, dataset_name)
    db.cur.execute(drop_table)
    db.cur.commit()
    
    #Formats the create_table Command    
    Fields = []   
    vartype = sorted(vartype.items())                                        
    for var in vartype:
        if var[1] =='PRIMARY KEY':
            Fields.append('{} INTEGER NOT NULL PRIMARY KEY'.format(var[0]))
        else:
            Fields.append('{} {} NOT NULL'.format(var[0], var[1]))
    Fields = ', '.join(Fields)
    #sql command to create a table
    create_table = 'CREATE TABLE dbo.{} ({})'.format(dataset_name, Fields)
    
    #Executes the command
    db.cur.execute(create_table)
    db.cur.commit()
    
    #Iterates through the shapefile
    cursor = arcpy.da.SearchCursor(shp_file, field_names)
    desc = arcpy.Describe(shp_file)
    #Geometry type
    shape_type = desc.shapeType
    #A list of dictionaries. Each inner dict represents a row within the shapefile
    alldata = []
    for row in cursor:
        data = {}
        for x in range(len(row)):
            #fields_list has same number of elements as row. The first part of each element is the field name
            data[fields_list[x][0]] = row[x]
        alldata.append(data)
    
    #Inserts all rows into database
    for field_row in alldata:
        #A list in the form of [(field1, value1), (field2, value2)...]
        sorted_data=sorted(field_row.items())
        #A list of the field values
        values = []
        #That list, with quotes placed around the strings
        values2 = []
        for val in sorted_data:
            values.append(val[1])
            
        for val in values:
            if type(val) is unicode or type(val) is str:
                val = "'{}'".format(val.encode('ascii', 'ignore').replace("'", "''"))
            values2.append(val)
        field_values = ', '.join(str(x) for x in values2)
        
        #Sql command to insert a row
        insert_row = "INSERT INTO dbo.{} VALUES ({});".format(dataset_name, field_values) 
        db.cur.execute(insert_row)
    db.cur.commit()

class DBase:
    
    dsn = r'DRIVER={SQL Server Native Client 11.0};SERVER=DEVSQL202;DATABASE=IIT_GIS_REFERENCE;UID=adev_iit_gis_reference;PWD=Geor3f@health;'

    def __init__(self):
        try:
            self.conn = pyodbc.connect(self.dsn)
            self.cur = self.conn.cursor()
        except Exception as e:
                print(repr(e))
                

if __name__=='__main__':

    #Makes initial database connection
    db = DBase()
    #Grabs metadata variables
    sql_select = 'SELECT active, source_url, external_name_prefix, internal_name, data_format, update_frequency, update_date FROM dbo.ReferenceTest'
    lookup_table = db.cur.execute(sql_select)
        
    #Looks for settings file
    local_path = sys.path[0]
    settings_file = os.path.join(local_path, "settings.ini")                
    if os.path.isfile(settings_file):
        config = configparser.ConfigParser()
        config.read(settings_file)
    else:
        print("INI file not found. \nMake sure a valid 'settings.ini' file exists in the same directory as this script.")
        sys.exit()
        
    username = config.get('ACCOUNT', 'USER')
    password = config.get('ACCOUNT', 'PASS')
    download_folder = config.get('FILE', 'DOWNLOADFOLDER')
#    username = raw_input('username: ')
#    password = raw_input('password: ')
#    print '\n'*40
    
    proxyDict = {'http' : 'http://health%5C{}:{}@healthproxy.health.dohmh.nycnet:8080'.format(username, password),
                 'https' : 'http://health%5C{}:{}@healthproxy.health.dohmh.nycnet:8080'.format(username, password),
                 'ftp' : 'http://health%5C{}:{}@healthproxy.health.dohmh.nycnet:8080'.format(username, password),
                 }
    
    zips_list = zip_urls("http://www1.nyc.gov/site/planning/data-maps/open-data.page", proxyDict)
    
    today = datetime.datetime.today()
    
    for metadata in lookup_table:
           
        #Date Check. Pass if it's been updated recently enough
        #format = '%Y-%m-%d %H:%M:%S.%f0'
        #last_update = datetime.datetime.strptime(metadata.update_date, format)
        last_update = metadata.update_date
        timePassed = today - last_update
        
        if timePassed < metadata.update_frequency:
            continue
        
        #Link of file to downlaod
        download_link = ''
        for zip_url in zips_list:
            if metadata.external_name_prefix in zip_url:
                download_link = zip_url
                
        dataset_name = metadata.internal_name
        
        #Downloads the zip into a filepath specified in the function. Returns the filepath
        file_direc = zip_downloader(download_link, download_folder, proxyDict)
        
        #Finds the shapefile within this directory (and all lower directories)
        #If it is just a shapefile download
        #Make sure that the external name is the same as the file name          
        if metadata.data_format == '.shp':
            shp_file = shp_file_locator(file_direc, metadata.external_name_prefix)
            xml_file = shp_file + '.xml'
        elif metadata.data_format == '.gdb':
            env.workspace = gdbfile_locator(file_direc)[0]
            feature_class_list = arcpy.Listfeature_classes()
            for feature_class in feature_class_list:
                if metadata.external_name_prefix in feature_class:
                    shp_file = feature_class  
            
        shp_file_upload(shp_file, dataset_name)
        
        #Changes the metadata table to reflect this database was updated
        update_date = 'UPDATE dbo.geodata_data SET update_date={} WHERE id={}'.format(datetime.datetime.strftime(today,format), metadata.id)
        db.cur.execute(update_date)
        
        #Finds and downloads the xml file (As long as it was a shp and not gdb)
        if metadata.data_format == '.shp':
            with open(xml_file) as file:
                tree = ET.parse(file)
                root = tree.getroot()
                xml_str = ET.tostring(root, encoding='utf8', method='xml')
            
            update_XML = 'UPDATE dbo.geodata_data SET meta={} WHERE id={}'.format(xml_str, metadata.id)
            db.cur.execute(update_XML)
        db.cur.commit()
        shutil.rmtree(file_direc)