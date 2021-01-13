#!/usr/bin/python
# coding: utf-8
#Imports:
import pandas as pd
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.properties import ObjectProperty
import mysql.connector as mdb,sys
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
import os

pymysql.install_as_MySQLdb()
from health_data import sick_with_pop_socio_grouped as health_file

# Main Class of our application
class InputForm(BoxLayout):
    'User Input form'
    'All references from KV file should come here'
    # Variables from the kivy file.
    ip = ObjectProperty()
    results = ObjectProperty()

    # Creating the connection with MySQL DB, Creating a new Data-Base with a query
    def connect(self,):
        'Building new DB and new table if not exists already'
        con = mdb.connect(host = self.ip.text, user = 'root', passwd = '12yotam3')
        cur = con.cursor()
        cur.execute('CREATE DATABASE if not exists corona_db')
        con.commit()  # Makes transaction
        con.close()  # Close transaction

    # Creating the method which will insert the whole df from the health_data.py
    def insert(self):
        'Inserting the DataFrame to DB'
    #    df = pd.read_csv(r'sick_with_pop_socio_grouped.csv')
        df = health_file
        host = self.ip.text
        user = 'root'
        passwd = '12yotam3'
        engine = create_engine("mysql://" + user + ':' + passwd + '@' + host + '/corona_db')
        con = engine.connect()
        df.to_sql(con=con, name='corona_data', if_exists='replace', schema= 'corona_db', index= True, chunksize= 100)
        con.close()


    def display_data_town_level(self):
        'Create new screen, initialize data_town_level'
        # Clear all widgets from the InputForm.
        self.clear_widgets()
        # Add a new form - data town level. Initialize the data town level form with the ip we already have.
        self.add_widget(show_data_town_level(self.ip.text))

    def display_data_national_level(self):
        'Create new screen, initialize data_national_level'
        # Clear all widgets from the InputForm.
        self.clear_widgets()
        # Add a new form - data national level. Initialize the data national level form with the ip we already have.
        self.add_widget(show_data_national_level(self.ip.text))

    def display_sample_from_db(self):
        'Create new screen, initialize ShowRecords'
        # Clear all widgets from the InputForm.
        self.clear_widgets()
        # Add a new form - sample from db. Initialize the sample from db form with the ip we already have.
        self.add_widget(show_sample_from_db(self.ip.text))


    def show(self, rows, features= None):
        'Show all query results on screen'
        # Clear previous results.
        self.results.text=''
        if not rows:
            # If we didn't receive any rows to show, display 'No records'.
            self.results.text='No records'
        else:
           #If there are any rows to show, Iterate over the rows to show them.
            if features != None:         # If there's a need to show the columns, It means that there's only one row to show.
                for column in features:
                    self.results.text += str(column) + "    "
                self.results.text += '\n'

                spacing = ""  # Defining dynamic spacing between values of the row
                for row in rows:
                    for i in range(len(row)):   # indexing
                        self.results.text += str(row[i]) + spacing
                        if i <= 3:
                            spacing = "                                 "

            else:                       # If there's no need to show the columns
                for row in rows:
                    for element in row:
                        self.results.text += str(element) + "  "
                    self.results.text += '\n'

    def out(self):
        'User close the application'
        sys.exit()


class show_sample_from_db(BoxLayout):
    'Display query results to user'
    # Variables from the kivy file.
    results = ObjectProperty()

    # Inherit the show method from InputForm class
    show_sample = InputForm.show
    # Inherit the out method from InputForm class
    out_sample = InputForm.out

    def __init__(self, ip):
        'Make a select * query pass all result rows to method show'
        super(show_sample_from_db, self).__init__()
        # Save the ip we got to this instance.
        self.ip = ip
        # Connect to the db.
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("select date, town_code, town from corona_data order by rand() Limit 5;")
        rows = cur.fetchall()
        # Call show with the rows we got.
        self.show_sample(rows)

    def go_home(self):
        # Create new screen
        self.clear_widgets()
        # Send back to InputForm
        self.add_widget(InputForm())


class show_data_town_level(BoxLayout):

    # Variables from the kivy file.
    results = ObjectProperty()
    town = ObjectProperty()

    def __init__(self, ip):
        super(show_data_town_level, self).__init__()
        # Save the ip we got to this instance.
        self.ip = ip

    def show_total_pop(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT DISTINCT(population) FROM corona_data WHERE town like '%{}%';".format(self.town.text))
        rows = cur.fetchall()
        # Call show with the rows we got.
        self.show_method_town_level(rows)

    def show_socioeconomic_score(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT DISTINCT(Poverty_De) FROM corona_data WHERE town like '%{}%';".format(self.town.text))
        rows = cur.fetchall()
        # Creating the explanation for the results
        rows.append(['\n','The socio-economic score is scaled between 1 to 10 as follows:', '\n', 'The score 1 represents the highest class, and scor 10 represents the poorest'])
        # Call show with the rows we got.
        self.show_method_town_level(rows)

    def show_health_data(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        features = ['town        ','accumulated_tested','accumulated_cases','accumulated_recoveries','active_cases','positivity_rate']
        cur.execute("SELECT town,accumulated_tested,accumulated_cases,accumulated_recoveries,active_cases,positivity_rate FROM corona_data where town like '%{}%' ORDER BY date DESC LIMIT 1;".format(self.town.text))
        rows = cur.fetchall()
        # Call show with the rows we got.
        self.show_method_town_level(rows, features)

    def show_trend_figure_town(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT date, active_cases, population  FROM corona_data WHERE town like '%{}%';".format(self.town.text))
        rows = cur.fetchall()

        # Creating a Dataframe for the plot
        df = pd.DataFrame(rows)
        df.columns = ['date', 'active_cases', 'population']
        df.set_index('date', inplace= True)
        df['active_cases_per_pop'] = (df['active_cases'] / df['population'].iloc[0] ) * 100
        df.drop(columns= ['active_cases', 'population'], axis = 1, inplace= True)

        # Call show with the rows we got.
        self.show_figure(df)

    def show_figure(self, df):
        # Creating the figure
        figure = df.plot().get_figure()
        plt.ylabel("Percantage of active cases")
        plt.title("Percantage of active cases - over time")
        plt.xticks(rotation=20)
        plt.rcParams["figure.figsize"] = (10, 5)
        if os.path.exists('figure_trend_over_time_town.jpeg'):
            os.remove(r'figure_trend_over_time_town.jpeg')
        figure.savefig('figure_trend_over_time_town.jpeg')
        # Create new screen to show the figure
        self.clear_widgets()
        # Add a new form - screen_for_trend_town
        self.add_widget(screen_for_trend_town())


    # Inherit the show method from InputForm class
    show_method_town_level = InputForm.show

    # Inherit the out method from InputForm class
    out_method_town_level = InputForm.out

    # Inherit the go home method from show_sample_from_db class
    go_home_town_level = show_sample_from_db.go_home

# A class to show the figure of the trend over time on the town level
class screen_for_trend_town(BoxLayout):
    # Inherit the out method from InputForm class
    out_method_screen_for_trend = InputForm.out
    # Inherit the go home method from show_sample_from_db class
    go_home_screen_for_trend = show_sample_from_db.go_home


class show_data_national_level(BoxLayout):
    # Variables from the kivy file.
    results = ObjectProperty()

    def __init__(self, ip):
        super(show_data_national_level, self).__init__()
        # Save the ip we got to this instance.
        self.ip = ip

    def show_color_prevalence(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT town_color, COUNT(DISTINCT(town)) FROM corona_data GROUP BY town_color;")
        rows = cur.fetchall()
        # Call show with the rows we got.
        self.show_method_national_level(rows)

    def show_town_highest_positive_cases(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT town, accumulated_cases FROM corona_data ORDER BY date DESC, accumulated_cases DESC LIMIT 1;")
        rows = cur.fetchall()
        # Call show with the rows we got.
        self.show_method_national_level(rows)

    def show_national_active_cases(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT date, SUM(active_cases) as active_cases FROM corona_data GROUP BY date ORDER BY date;")
        rows = cur.fetchall()

        # Creating a Dataframe from the plot
        df = pd.DataFrame(rows)
        df.columns = ['date', 'active cases']
        df.set_index('date', inplace=True)
        df['active_cases'] = df['active cases'].astype(float)
        # Call show with the rows we got.
        self.show_active_cases_figure(df)

    def show_active_cases_figure(self, df):
        # Creating the figure
        figure = df.plot().get_figure()
        plt.ylabel("Active cases")
        plt.title("National active cases - over time")
        plt.xticks(rotation=20)
        plt.rcParams["figure.figsize"] = (10, 5)
        figure.savefig('figure_active_cases_national.jpeg')
        # Create new screen to show the figure
        self.clear_widgets()
        # Add a new form - screen_for_national_active_cases.
        self.add_widget(screen_for_national_active_cases())

    def show_national_positivity_rate(self):
        con = mdb.connect(host=self.ip, user='root', passwd='12yotam3', database='corona_db')
        cur = con.cursor()
        # Query the db to get all stored rows.
        cur.execute("SELECT date, ROUND(SUM(count_positive_cases) * 100 / SUM(count_tests),2) as positivity_rate FROM corona_data GROUP BY date ORDER BY date;")
        rows = cur.fetchall()

        # Creating a Dataframe from the plot
        df = pd.DataFrame(rows)
        df.columns = ['date', 'positivity rate']
        df.set_index('date', inplace=True)
        # Call show with the rows we got.
        self.show_positivity_rate_figure(df)

    def show_positivity_rate_figure(self,df):
        # Creating the figure
        figure = df.plot().get_figure()
        plt.ylabel("Positivity rate")
        plt.title("National positivity rate - over time")
        plt.xticks(rotation=20)
        plt.rcParams["figure.figsize"] = (10, 5)
        figure.savefig('figure_positivity_rate_national.jpeg')
        # Create new screen to show the figure
        self.clear_widgets()
        # Add a new form - sample from db. Initialize the sample from db form with the ip we already have.
        self.add_widget(screen_for_national_positivity_rate())


    # Inherit the show method from InputForm class
    show_method_national_level = InputForm.show

    # Inherit the show method from InputForm class
    out_method_national_level = InputForm.out

    # Inherit the go home method from show_sample_from_db class
    go_home_national_level = show_sample_from_db.go_home

# A class to show the figure of national active cases
class screen_for_national_active_cases(BoxLayout):
    # Inherit the show method from InputForm class
    out_method_screen_for_active_cases = InputForm.out
    # Inherit the go home method from show_sample_from_db class
    go_home_screen_for_national_active_cases = show_sample_from_db.go_home

# A class to show the figure of national positivity rate
class screen_for_national_positivity_rate(BoxLayout):
    # Inherit the show method from InputForm class
    out_method_screen_for_active_cases = InputForm.out
    # Inherit the go home method from show_sample_from_db class
    go_home_screen_for_national_positivity_rate = show_sample_from_db.go_home


# A class for our kivy application
class Corona_kivyApp(App):
    pass

if __name__ == '__main__':
    Corona_kivyApp().run()