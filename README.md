# Geodata-Lookup-and-Automated-Deployment
A project of mine from the Department of Health and Mental Hygiene. The idea was to retrieve geodata from online sources, which were generally stored as shapefiles, and insert them automatically into a geodatabase through a python application.
It was by no means complete by the time I left at the end of the summer, and this is just the portion which I wrote myself.
I also wasn't able to test it very thoroughly due to network security issues, so judge it leniently please!

# Functions:

directory_exists(dpath):
Checks if the directory dpath exists, and if not creates it

zip_downloader(link, download_folder, proxy_dict):
Downloads (and extracts) a zip file from the specified link to a folder named after the zip file within the download_folder directory.
Returns the location of the downloaded zip folder

shp_file_locator(file_direc, file_name):
Searches for a shapefile with the file_name within the file direc, as well as all of the lower directories.
Returns the location of the shapefile

gdbfile_locator(file_direc):
Searches and returns a list of gdbfiles within the file_direc

variable_transform(fields):
Takes as input a dictionary organized {field_name1: variable_type1, field_name2: variable_type2 ...}
Changes these variable types from the arcmap names, to their corresponding sql variables (e.g. SmallInteger becomes SMALLINT)
Returns a dictionary as such {field_name1: transformed_variable1, ...}

zip_urls(dataUrl, proxyDict):
A modified version of the zipfiles script. Returns a list of zipfiles within the opendata page.

shp_file_upload(shp_file, dataset_name)
Reads the shape file indicated, and uploads it to the table indicated. Note that the table is dropped and created.
Uses arcpy to read the fields. The shape field is transformed into WKT so that it may be read by the sql database.
First it examines the shapefile to pull the field names and their respective variable types, and creates a table based off those specifications.
Then it reads the shapefile row by row, and puts it into the alldata list. This list is then read, with all the strings having quotes placed around them.
It creates an insert command for each row.
Executes after all the rows are inserted
The GLAD-SHAPEFILE version uses shapely instead of arcpy, at the cost of some data such as variable types, and the unique ID field.


# Main:
Within the main body, the script goes through the following steps:
1. It finds and reads the metadata table, taking such fields as how long ago it was updated, how frequently, the name of the table, etc.
2. It finds and reads the settings file, taking the username, password, and download folder in which files will be temporarily stored.
	The download folder will be deleted at the end of the script
3. It reads the username and password settings into the proxy dict variable, which allows us to pass the department's proxy settings.
4. It finds a list of viable zip urls, using the zip_urls function, which point directly to the files online locations.
5. It iterates through the metadata table now. First it checks the last update date and frequency against the current date, using the datetime module, and decides whether to update.
	Note that I have assumed update frequency as an integer here, instead of a datetime.
6. It then finds the file name specified (Here called metadata.external_name_prefix) within the ziplinks list. If there are multiple matches, it will use the last link found.
	It is important to note that this does not need to match the table's name entirely, but it must be identifying enough to pull the correct file.
	For example, setting the external_name_prefix to 'mappluto' will pull all five mappluto files for the different boroughs.
	On the other hand, using 'mn_mappluto_16v2.zip' will only work so long as the file version isn't updated.
	In this case, searching for 'mn_mappluto' would work to find solely the manhattan mappluto file.
7. It then downloads this zip, and stores it in the user specified download folder.
8. Within this extracted folder, it searches for either a shp file or gdb file, based off of the metadata table.
	The metadata.data_format tells it which one to search for. the metadata.external_name_prefix is the file name it searches for.
9. It uploads this file using the shp_file_upload function.
10. It changes the metadata table to reflect the database was update, as well as to isnert the xml file.
11. It deletes the folder in which the zipfile was stored in (not the download folder as a whole, but the specific folder for the zip)
12. It loops back around to the next row (back to step 5)
	
