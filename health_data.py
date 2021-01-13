#!/usr/bin/env python
# coding: utf-8

# In[1]:

# Importing:
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import urllib.request
import plotly.express as px
import warnings 
warnings.simplefilter('ignore')
import datetime
desired_width=320
pd.set_option('display.max_columns',10)
pd.set_option('display.width', desired_width)

# Display format
pd.set_option('display.float_format', lambda x: '%.5f' % x)

# Accessing the API
url = 'https://data.gov.il/api/3/action/datastore_search?resource_id=d07c0771-01a8-43b2-96cc-c6154e7fa9bd&limit=1000000'
fileobj = urllib.request.urlopen(url)
df = pd.read_json(fileobj)
data = df.loc['records']['result']
sick = pd.DataFrame(data)

# In[2]:

#creating a mapper of town_code to town
town_code_to_town = sick[['town_code', 'town']]
town_code_to_town['town_code'] = town_code_to_town['town_code'].astype(int)

# In[3]:

# Type convertion of fields
sick['date'] = pd.to_datetime(sick['date'])
sick = sick.rename(columns= {'accumulated_diagnostic_tests' : 'accumulated_tested'})
sick = sick.drop('_id', axis = 1)

# filtering the df - Taking only numerical amounts and excluding nulls in the # of tests column
sick_filtered = sick[(sick['accumulated_cases'] != '0') & (sick['accumulated_recoveries'] != '0') & (sick['accumulated_tested'] != '0')]
sick_filtered = sick_filtered[(sick_filtered['accumulated_cases'] != '<15') & (sick_filtered['accumulated_recoveries'] != '<15') & (sick_filtered['accumulated_tested'] != '<15')]
sick_filtered = sick_filtered.loc[sick_filtered['accumulated_tested'].isna() == False]

sick_filtered['accumulated_tested'] = sick_filtered['accumulated_tested'].astype(float)

# In[4]:

# Converting to int for all numeric fields
for c in ['town_code','accumulated_cases','accumulated_recoveries','accumulated_tested']:
    sick_filtered[c] = sick_filtered[c].astype(int)

# In[5]:

# Grouping the file by date and townn, And creating the 'active cases' column
sick = sick_filtered.groupby(['date', 'town_code']).sum().reset_index()
sick['active_cases'] = sick['accumulated_cases'] - sick['accumulated_recoveries']
sick = sick.sort_values(by = ['town_code', 'date'])

# In[6]:
# Filtering towns that don't have more than a week of documented data (after cleaning)
def no_data_for_a_week(df):
    if df.shape[0] < 7:
        pass
    else:
        return df
sick = sick.groupby('town_code', as_index= False).apply(no_data_for_a_week).reset_index()
sick = sick.drop('index', axis= 1)

sick = sick.merge(town_code_to_town, how= 'left', left_on= 'town_code', right_on= 'town_code')

# In[7]:

sick = sick.drop_duplicates(subset= ['date', 'town_code'])

# In[8]:

# Converting from accumulated values to Count values, Creating 2 new columns:
def create_count_heealth(df):
    df["count_tests"] = df['accumulated_tested'].diff(periods = 1)
    df['count_positive_cases'] = df['accumulated_cases'].diff(periods = 1)
    df['count_active_cases'] = df['active_cases'].diff(periods = 1)
    df = df.loc[df["count_tests"].notna()]
    return df

# In[9]:

sick_with_count_health = sick.groupby('town_code', as_index= False).apply(create_count_heealth).reset_index()
sick_with_count_health = sick_with_count_health.reset_index().drop(labels= ['index','level_0', 'level_1'], axis = 1)

# In[10]:

sick_with_count_health['positivity_rate'] = (sick_with_count_health['count_positive_cases'] / sick_with_count_health['count_tests']) * 100


# ### importing the population file:

# In[11]:

# Creating a mapper with population for each town
pop_file = pd.read_excel(r'סהכ-אוכלוסייה-לפי-אס-גיל-ומין-סוף-2017.xlsx', engine= 'openpyxl', usecols= 'B,D,E')
pop_file = pop_file.loc[pop_file['אזור סטטיסטי'] == 'סה"כ']
pop_file = pop_file.rename(columns= {'אוכלוסייה בסוף 2017' : 'population', 'סמל יישוב' : 'town_id' }).drop('אזור סטטיסטי', axis = 1)
pop_file['population'] = pop_file['population'].astype(int)
pop_file['town_id'] = pop_file['town_id'].astype(int)


# In[12]:


sick_with_pop = sick_with_count_health.merge(pop_file, how= 'left', left_on= 'town_code', right_on= 'town_id').drop('town_id', axis = 1)

# ### Envolving the socio demographic data

# In[13]:

socio_df = pd.read_csv(r'sociodemographic_stat.csv')
socio_df_grouped = socio_df.groupby(by= 'SEMEL_YISH')['Poverty_De'].mean()

# In[14]:

sick_pop_poverty = sick_with_pop.merge(socio_df_grouped, how= 'left', left_on= 'town_code', right_on= 'SEMEL_YISH')

# In[15]:

# A mapper to fill null values
town_socio_mapper = sick_pop_poverty[['town_code','Poverty_De']].loc[~sick_pop_poverty[['town_code','town']].duplicated()]
sick_pop_poverty.drop('Poverty_De', axis = 1, inplace = True)


# In[16]:

# Handeling null values
np.random.seed(100)
def fill_na_values(row): 
    row['Poverty_De'] = row['Poverty_De'].fillna(np.random.uniform(low= 0, high= 10))
    return row

# In[17]:

town_socio_mapper = town_socio_mapper.groupby(by= 'town_code').apply(fill_na_values)

# In[18]:

sick_with_pop_socio = sick_with_pop.merge(town_socio_mapper, how= 'left', left_on= 'town_code', right_on= 'town_code')

# ### Creating the indexes for traffic light model

# #### index number 1:
# In[19]:

sick_with_pop_socio['sick_for_10,000'] = sick_with_pop_socio['population'] * sick_with_pop_socio['count_active_cases'] / 10000

# In[20]:

sick_with_pop_socio['sick_for_10,000'][sick_with_pop_socio['sick_for_10,000'] < 1] = 1

# #### index number 2:

# In[21]:

def create_index_2(df):
    last_week_active_cases = list(df.loc[df['date'] == (df['date'].max() - datetime.timedelta(days=6))]['active_cases'])[0]
    todays_positive_cases = list(df.loc[df['date'] == df['date'].max()]['count_positive_cases'])[0]
    try:    
        df['growth_rate'] = ((todays_positive_cases / last_week_active_cases)) ** 2
        return df
    except ZeroDivisionError:
        df['growth_rate'] = 0.01
        return df

# In[22]:

sick_with_pop_socio = sick_with_pop_socio.groupby(by= 'town_code').apply(create_index_2)

# In[23]:

sick_with_pop_socio['growth_rate'][sick_with_pop_socio['growth_rate'] < 0.5] = 1

# #### index number 3:

# In[24]:

# taking the rows within the past week:
mask = (sick_with_pop_socio['date'] > (sick_with_pop_socio['date'].max() - datetime.timedelta(days=7))) & (sick_with_pop_socio['date'] <= sick_with_pop_socio['date'].max())
df_past_week = sick_with_pop_socio.loc[mask]


# In[25]:

df_past_week_grouped = df_past_week.groupby('town_code')['positivity_rate'].mean().to_frame().reset_index()

# In[26]:

df_past_week_grouped['positivity_rate_last_week'] = (((df_past_week_grouped['positivity_rate'])+0.25 / 100)) * (100/8)

# In[27]:

df_past_week_grouped = df_past_week_grouped.drop('positivity_rate', axis = 1)

# In[28]:

# Merging it with the health df
sick_with_pop_socio = sick_with_pop_socio.merge(df_past_week_grouped, how= 'left', left_on= 'town_code', right_on= 'town_code')

# In[29]:

sick_with_pop_socio.head()

# ### Creating the traffic-light index for each town

# In[30]:

# The traffic light formula as it defined by Israel's ministrey of health
sick_with_pop_socio['traffic_light_index'] = 2 + np.log(sick_with_pop_socio['sick_for_10,000']) + np.log(sick_with_pop_socio['growth_rate']) + np.log(sick_with_pop_socio['positivity_rate_last_week'])

# In[31]:

# Determine the color of the town according to the traffic light model
def set_town_color(df):
    if list(df['traffic_light_index'])[0] >= 7.5:
        df['town_color'] = 'red'
    elif (list(df['traffic_light_index'])[0] < 7.5) and (list(df['traffic_light_index'])[0] >= 6):
        df['town_color'] = 'orange'
    elif (list(df['traffic_light_index'])[0] < 6) and (list(df['traffic_light_index'])[0] >= 4.5):
        df['town_color'] = 'yellow'
    else:
        df['town_color'] = 'green'
    return df

# In[32]:

sick_with_pop_socio_grouped = sick_with_pop_socio.groupby('town_code').apply(set_town_color)

# In[33]:
### Importing the towns hebrew english file to have the town names in english
towns_hebrew_english = pd.read_csv(r'towns_hebrew_english.csv', encoding= 'utf-8')
sick_with_pop_socio_grouped = sick_with_pop_socio_grouped.merge(towns_hebrew_english, how= 'inner', left_on= 'town_code', right_on= 'town_code')
sick_with_pop_socio_grouped['town'] = sick_with_pop_socio_grouped['town_name']
sick_with_pop_socio_grouped.drop('town_name', axis= 1, inplace= True)

sick_with_pop_socio_grouped = round(sick_with_pop_socio_grouped, 2)

# In[ ]:
sick_with_pop_socio_grouped['town'] = sick_with_pop_socio_grouped['town'].str.replace("'", "")
sick_with_pop_socio_grouped.to_csv(r"sick_with_pop_socio_grouped.csv", index = False)


