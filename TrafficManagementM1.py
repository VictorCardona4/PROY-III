# -*- coding: utf-8 -*-

# --- DATAFRAME TRAFFIC --- #
# Date
# Weekday - Is_holiday
# Street_id
# Traffic
# (Weather)
# Line - Point

# Time control
import time
from datetime import datetime
# Web access and download
import requests
from bs4 import BeautifulSoup
from urllib import request
# Zip mamagement
from zipfile import ZipFile
# Path management
import os
# Dataframes and csvs management
import pandas as pd
import json

# Step 0: Folder creation
def CreateFolder(name):
    try:
        os.makedirs(f"../{name}")
        print(f"\t{name} folder created!")
    except FileExistsError:
        pass

# Step 1: Download zips from the web
def DownloadZips():
    # Initial point: Nothing.
    #   Having this code inside a folder named Code is hardly recommended
    #   New folders will be created at the same level as Code folder
    # Final point: All zips downloaded in Zips folder
        
    # Starting time
    t0 = time.time()
    print("Step 1:")
    
    # Creates TrafficData and Zips folders
    name = "TrafficData"
    path = f"../{name}/"
    CreateFolder(name)
    CreateFolder("Zips")
    
    # Reads the file if exists. If not, it will be created
    try:
        with open(path + "ZipsRegister.txt", "r") as readerFile:
            text = readerFile.read() + "\n"
    except FileNotFoundError:
        with open(path + "ZipsRegister.txt", "w") as writerFile:
            writerFile.write("")
        text = ""
    
    # Zips already analysed
    actual_zips = text.split("\n")[:-1]
    
    # Web scraping
    url = "https://github.com/ceferra/estat_trafic_VLC"
    result = requests.get(url)
    src = result.content
    soup = BeautifulSoup(src, 'lxml')
    
    # All zips available
    links = soup.find("div", class_="Box mb-3").find_all("a", class_="js-navigation-open Link--primary")
    link_names = [a.get_text() for a in links]
    link_names = [a for a in link_names if a.split(".")[-1] == "zip"]
    
    # New zips
    new_zips = list(set(link_names) - set(actual_zips))
    new_zips.sort(key=lambda x: (x[6:10], x[3:5], x[0:2]))
    
    for i, link_name in enumerate(new_zips):
        #if i == 10:
        #    break
        
        print(f"\tDownloading {link_name}")
        
        # Web scraping to download the new zip
        zip_path = "../Zips/" + link_name
        url = f"https://github.com/ceferra/estat_trafic_VLC/raw/main/{link_name}"
        
        # Download zip into local
        request.urlretrieve(url, zip_path)
        
        # Save the zip is already downloaded
        text += link_name + "\n"
    
    # Create the zips register
    with open(path + "ZipsRegister.txt", "w") as writerFile:
        writerFile.write(text[:-1])
    
    # Final time --> 1.1s/zip
    t1 = time.time()
    print(f"\tDownload zips time: {round(t1-t0,4)}s")

# Step 2: Download holidays from the web
def DownloadHolidays():
    # Initial point: All zips downloaded in Zips folder
    # Final point: Holidays json in TrafficData folder
        
    # Starting time
    t0 = time.time()
    print("Step 2:")
    
    # All zips downloaded
    zips_path = "../Zips/"
    zip_files = [f for f in os.listdir(zips_path) if f.split(".")[-1] == "zip"]
    years = set(map(int, [f[6:10] for f in zip_files]))
    
    try:
        with open("../TrafficData/Holidays.json", "r") as readerFile:
            holidays = json.load(readerFile)
            if years - set(map(int,holidays.keys())) != set():
                raise Exception
    except:
        holidays = {}
        for year in years:
            holidays[year] = []
            
            # Web scraping
            url = f"https://www.calendarioslaborales.com/calendario-laboral-valencia-{year}.htm"
            result = requests.get(url)
            src = result.content
            soup = BeautifulSoup(src, 'lxml')
            
            months = soup.find("div", id="wrapIntoMeses").find_all("div", class_="mes")
            for m, month in enumerate(months):
                for t in ["N", "R", "P"]:
                    for holiday in [h.get_text() for h in month.find_all("td", class_=f"cajaFestivo{t}")]:
                        holidays[year].append(f"{str(holiday).zfill(2)}-{str(m+1).zfill(2)}-{year}")
            
            holidays[year].sort(key=lambda x: (x[6:10], x[3:5], x[0:2]))
        
        with open('../TrafficData/Holidays.json', 'w') as writerFile:
            json.dump(holidays, writerFile)
    
    # Final time
    t1 = time.time()
    print(f"\tDownload holidays time: {round(t1-t0,4)}s")

# Step 3: Extract data from the zips
def ExtractDataFromZips():
    # Initial point: All zips downloaded in Zips folder
    # Final point: All csvs in FirstCsvs folder
    
    # Starting time
    t0 = time.time()
    print("Step 3:")
    
    # Creates the Csvs folder
    name = "Csvs"
    CreateFolder(name)
    
    # Constants
    N = 24*4
    
    # All zips downloaded
    zips_path = "../Zips/"
    zip_files = [f for f in os.listdir(zips_path) if f.split(".")[-1] == "zip"]
    zip_files.sort(key=lambda x: (x[6:10], x[3:5], x[0:2]))
    
    # Csvs extraction from the zips
    csvs_path = f"../{name}/"
    for zip_file in zip_files:
        # Extract all csvs from a zip
        ZipFile(zips_path + zip_file).extractall(csvs_path)
        
        # data folder path with all csvs
        data_path = csvs_path + "data/"
        csv_files = os.listdir(data_path)                        
        
        # Set all csvs in the rigth path
        for csv_file in csv_files:
            os.rename(data_path + csv_file, csvs_path + csv_file)
        
        # Add missing csvs
        if len(csv_files) < N:
            df = pd.DataFrame(columns = ["gid", "Estat / Estado"])
            d = {}
            for csv_file in csv_files:
                h, m = csv_file[21:26].split("-")
                if h in d:
                    d[h].add(m)
                else:
                    d[h] = {m}
            s = {'00', '15', '30', '45'}
            for k,v in d.items():
                if len(v) < 4:
                    for m in s - v:
                        new_name = f"{csvs_path}estat_traf{zip_file.split('.')[0]}_{k}-{m}-00.csv"
                        df.to_csv(new_name, sep = ";", index = False)
        
        # Delete data folder and the zip
        os.rmdir(data_path)
        os.remove(zips_path + zip_file)
    
    # Delete Zips folder
    os.rmdir(zips_path)
    
    # Final time
    t1 = time.time()
    print(f"\tExtraction data from zips time: {round(t1-t0,4)}s")

# Step 4: Create Streets Data
def CreateGeoData():
    # Initial point: All csvs in Csvs folder
    # Final point: Streets csv in TrafficData folder
    
    # Starting time
    t0 = time.time()
    print("Step 4:")
    
    try:
        with open("../TrafficData/Streets.csv", "r"):
            pass
    except:
        # Constants
        N = 376
        NEW_COLUMNS = {"gid": "Street_id",
                       "geo_shape" : "Line",
                       "geo_point_2d": "Point"}        
        
        # All csvs available
        path = "../Csvs/"
        files = [f for f in os.listdir(path)]
        files.sort(key=lambda x: (x[16:20], x[13:15], x[10:12], x[21:23], x[24:26]))
        
        for csv in files:
            # Dataframe from csv
            df = pd.read_csv(path + csv, delimiter=";")
            if len(df) == N:
                # Columns selection and rename
                df = df[NEW_COLUMNS.keys()]
                df.rename(columns = NEW_COLUMNS, inplace=True)
                df.sort_values(by="Street_id", inplace=True)
                
                # Line column modify
                l = []
                for x in df["Line"]:
                    l.append(list([list(map(float, reversed(y))) for y in json.loads(x)["coordinates"]]))
                df["Line"] = l
                
                # Point column modify
                df["Point"] = [list(map(float,x.split(","))) for x in df["Point"]]
                
                # Dataframe to csv and Traffic2.csv delete
                df.to_csv("../TrafficData/Streets.csv", sep = ";", index = False)
                break
    
    # Final time
    t1 = time.time()
    print(f"\tCreation geo data time: {round(t1-t0,4)}s")

# Step 5: Modify csvs
def ModifyCsvs():
    # Initial point: All csvs in Csvs folder and Streets csv in TrafficData folder
    # Final point: Traffic csv in TrafficData folder
    
    # Starting time
    t0 = time.time()
    print("Step 5:")
    
    # Dictionary holidays from json
    with open("../TrafficData/Holidays.json", "r") as readerFile:
        holidays = json.load(readerFile)
    
    # Dataframe Streets from csv
    df_s = pd.read_csv("../TrafficData/Streets.csv", delimiter=";")
    N = len(df_s)
    
    # All csvs available
    csvs_path = "../Csvs/"
    files = [f for f in os.listdir(csvs_path)]
    files.sort(key=lambda x: (x[16:20], x[13:15], x[10:12], x[21:23], x[24:26]))
    
    # Dataframe creation and constants
    df = pd.DataFrame()
    NEW_COLUMNS = {"gid": "Street_id",
                   "Estat / Estado": "Traffic"}
    
    print(f"\tExpected time: {round(len(files)*0.035, 2)}s")
    l = []
    for csv in files:
        # Time register
        t = csv[10:20].split("-") + csv[21:26].split("-")
        
        # Dataframe from csv
        df_q = pd.read_csv(csvs_path + csv, delimiter=";")
        # Columns selection and rename
        df_q = df_q[NEW_COLUMNS.keys()]
        df_q.rename(columns = NEW_COLUMNS, inplace=True)
        df_q.sort_values(by="Street_id", inplace=True)
        
        # Save Traffic column
        if len(df_q) == N:
            l.append([(x if x<5 else x-5) for x in df_q["Traffic"]])
        
        # Append rows aggregated by hour
        if t[4] == "45":
            # Modify traffic register
            l2 = []
            for i in range(N):
                l2.append([l[j][i] for j in range(len(l))])
            
            # Create new traffic column
            l3 = []
            for x in l2:
                x = [y for y in x if y != 4]
                if x == []:
                    l3.append(4)
                elif 3 in x:
                    l3.append(3)
                else:
                    l3.append(round(sum(x)/len(x),2))
            
            # Columns addition
            df_q = df_s.copy()
            date = "-".join(t[:3])
            weekday = datetime.strptime(date, "%d-%m-%Y").strftime("%A")[:3]
            
            # Columns addition
            df_q.insert(0, "Date", date)
            df_q.insert(1, "Time", f"{t[3]}:00:00")
            df_q.insert(2, "Weekday", weekday)
            df_q.insert(3, "Is_holiday", weekday == "Sun" or date in holidays[t[2]])
            df_q.insert(5, "Traffic", l3)
            
            # Dataframes union
            df = pd.concat([df, df_q], axis=0)
            l = []
        
        # Csv delete
        os.remove(csvs_path + csv)
    
    # Dataframe to csv and folder delete
    if len(df) != 0:
        df.to_csv("../TrafficData/Traffic2.csv", sep = ";", index = False)
    os.rmdir(csvs_path)
    
    # Final time --> 2.5s/day
    t1 = time.time()
    print(f"\tModification csvs time: {round(t1-t0,4)}s")
    
    return df

# Step 6: Join Traffic csvs
def JoinTraffic():
    # Initial point: 1 or 2 Traffic csvs in TrafficData folder
    # Final point: 1 Traffic csv in TrafficData folder
    
    # Starting time
    t0 = time.time()
    print("Step 6:")
    
    # Trying to read the data
    data_path = "../TrafficData/"
    try:
        # Dataframes from csvs
        df = pd.read_csv(data_path + "Traffic.csv", delimiter=";")
        df_2 = pd.read_csv(data_path + "Traffic2.csv", delimiter=";")
        
        # Dataframes union
        df = pd.concat([df, df_2], axis=0)
        
        # Dataframe to csv and Traffic2.csv delete
        df.to_csv(data_path + "Traffic.csv", sep = ";", index = False)
        os.remove(data_path + "Traffic2.csv")
    
    # If Traffic.csv doesn't exist
    except FileNotFoundError:
        # Traffic2.csv will be renamed as Traffic.csv
        os.rename(data_path + "Traffic2.csv", data_path + "Traffic.csv")
    
    # If Traffic2.csv is empty
    except:
        # Delete Traffic2.csv
        os.remove(data_path + "Traffic2.csv")
    
    # Final time
    t1 = time.time()
    print(f"\tJoining traffic time: {round(t1-t0,4)}s")
    
if __name__ == "__main__":
    DownloadZips()
    DownloadHolidays()
    ExtractDataFromZips()
    CreateGeoData()
    df = ModifyCsvs()
    JoinTraffic()