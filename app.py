import re

import gspread as gs
import pandas as pd

market_info = {
    'San Francisco': {
        'Google spreadsheet': 'https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0',
        'Directory worksheet': 'SFC directory',
        'Database worksheet': 'SFC DB'
    },
    'San Antonio': {
        'Google spreadsheet': 'https://docs.google.com/spreadsheets/d/14x5hCAtOqDyTuxgqe01jisuHOwzmqRnfQBXifB8zWLc/edit#gid=2043417163',
        'Directory worksheet': 'SAEN directory',
        'Database worksheet': 'SAEN DB'
    }
}

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f'🏙️ Working on {market}!')
    print(f'🐝 Google spreadsheet: {info["Google spreadsheet"]}')

sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0')

roundup_ws = sh.worksheet('SFC directory')

roundup_df = pd.DataFrame(roundup_ws.get_all_records())

# Turn the "URL" column into a list
url_list = roundup_df['URL'].tolist()

central_df = pd.DataFrame()

for i, url in enumerate(url_list):
    individual_roundup_sh = gc.open_by_url(url)

    # Get the title of the spreadsheet
    title = individual_roundup_sh.title
    print(f'🐝 Working on {title}...')

    individual_roundup_ws = individual_roundup_sh.worksheet('listings')
    
    nav_roundup_ws = individual_roundup_sh.worksheet('nav')
    nav_roundup_df = pd.DataFrame(nav_roundup_ws.get_all_records())
    # Drop the first column of the nav_roundup_df
    nav_roundup_df = nav_roundup_df.drop(columns=['Display_Name', 'Location'])

    df = pd.DataFrame(individual_roundup_ws.get_all_records())

    # Join the nav_roundup_df to the df on the Listing_Id column
    df = df.merge(nav_roundup_df, on='Listing_Id', how='left')

    # If the value in the "Display_Name" column is "", then drop the row
    df = df[df['Display_Name'] != '']

    # Drop the first row, which is the header row
    df = df.drop([0])

    # Drop duplicate rows based on the "Display_Name" column
    df = df.drop_duplicates(subset=['Display_Name'])

    # Add a column that contains the title of the spreadsheet
    df['Review roundup'] = title

    # Add a column that contains the URL of the spreadsheet
    df['C2P_Sheet'] = url

    # convert the "Text" column to a list and print it
    # text_list = df['Text'].tolist()
    # print(text_list)

    # Using the re.sub() function, replace the HTML tags with nothing
    df['Plain_Text'] = df['Text'].apply(lambda x: re.sub('<[^<]+?>', '', x))

    story_settings_ws = individual_roundup_sh.worksheet('story_settings')

    # Search for the column of the appearance of the key: Publish_Date
    publish_date_index = story_settings_ws.find('LastModDate_C2P').col

    # Get the value of the cell in the row below the key
    publish_date = story_settings_ws.cell(2, publish_date_index).value

    slug_index = story_settings_ws.find('Slug').col
    year_index = story_settings_ws.find('Year').col
    df['C2P_Live_Link'] = f'https://www.sfchronicle.com/{story_settings_ws.cell(2, year_index).value}/{story_settings_ws.cell(2, slug_index).value}'

    # In the roundup_ws, update the cell in the "Last updated" column that corresponds to the URL with the publish_date
    roundup_ws.update_cell(i + 2, 3, publish_date)

    # Concatenate the individual dataframes into one big dataframe
    central_df = pd.concat([central_df, df])

# Sort the dataframe by the "Display_Name" column in ascending order
central_df = central_df.sort_values(by=['Display_Name'], ascending=True)

# Write the central dataframe to the 'SF DB' worksheet
central_ws = sh.worksheet('SFC DB')

# Replace the existing worksheet with the data in the dataframe
central_ws.clear()
central_ws.update([central_df.columns.values.tolist()] + central_df.values.tolist())

print('🥳 Done!')