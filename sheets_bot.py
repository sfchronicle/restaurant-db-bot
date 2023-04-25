# We decided on 2023-04-24 to go in a different direction and scrape the live pages directly. I'm too proud of this functional code to completely wipe it out, so I'm leaving it here for posterity.

import re
import time

import gspread as gs
import pandas as pd
from gspread_dataframe import set_with_dataframe

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

# This is the dictionary that contains the information about each market's spreadsheet.
# market_info = {
#     'San Francisco': {
#         'Google spreadsheet': 'https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0',
#         'Directory worksheet': 'SFC directory',
#         'Database worksheet': 'SFC DB'
#     },
#     'San Antonio': {
#         'Google spreadsheet': 'https://docs.google.com/spreadsheets/d/14x5hCAtOqDyTuxgqe01jisuHOwzmqRnfQBXifB8zWLc/edit#gid=2043417163',
#         'Directory worksheet': 'SAEN directory',
#         'Database worksheet': 'SAEN DB'
#     }
# }
market_info = {
    'San Francisco': {
        'Google spreadsheet': 'https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0',
        'Directory worksheet': 'SFC directory',
        'Database worksheet': 'SFC DB'
    }
}

# This handy dandy function will retry the api call if it fails.
def api_call_handler(func):
    # Number of retries
    for i in range(0,10):
        try:
            return func()
        except Exception as e:
            print(f'🤦‍♂️ {e}')
            print(f'🤷‍♂️ Retrying in {2 ** i} seconds...')
            time.sleep(2 ** i)
    print('🤬 Giving up...')
    raise SystemError

def check_last_mod_date(story_settings_df):
    '''
    This function checks the last modified date in the story_settings_df and returns the date.
    '''

    last_mod_date_index = story_settings_df.columns.get_loc('LastModDate_C2P')

    last_mod_date = story_settings_df.iloc[1, last_mod_date_index]

    return last_mod_date

def check_mod_date_changed(last_mod_date, last_known_mod_date):
    '''
    This function checks if the last modified date has changed. If it has, it returns True. If it hasn't, it returns False.
    '''

    if last_mod_date == last_known_mod_date:
        return False
    else:
        return True

def open_market_spreadsheet(url, directory, db):
    """
    This function opens each market's main spreadsheet and returns the worksheets and dataframes.
    """
    print('📂 Opening market spreadsheet...')

    # Open the main spreadsheet
    market_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

    # Open the directory and database worksheets contained in the main market_spreadsheet
    market_directory_ws = api_call_handler(lambda: market_spreadsheet.worksheet(directory))
    market_database_ws = api_call_handler(lambda: market_spreadsheet.worksheet(db))

    # Store the directory and database worksheets in pandas dataframes
    market_directory_df = api_call_handler(lambda: pd.DataFrame(market_directory_ws.get_all_records()))
    market_database_df = api_call_handler(lambda: pd.DataFrame(market_database_ws.get_all_records()))

    # Return the worksheets and dataframes
    return market_spreadsheet, market_directory_ws, market_database_ws, market_directory_df, market_database_df

def open_guide_spreadsheet(url, name):
    '''
    This function opens the guide spreadsheet and returns the three dataframes.
    '''
    # print('🍑 Opening guide spreadsheet...')

    # Open the guide spreadsheet
    guide_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

    guide_worksheets = guide_spreadsheet.values_batch_get(
        ranges=['listings!A1:Z1000', 'nav!A1:Z1000', 'story_settings!A1:Z1000']
    )

    guide_worksheets = guide_worksheets['valueRanges']
    
    # Loop through the worksheets in the spreadsheet_dict. Add the values to the appropriate dataframe
    for n, worksheet in enumerate(guide_worksheets):
        # The first worksheet is the listings worksheet
        if n == 0:
            restaurant_listings_df = pd.DataFrame(worksheet['values'])
            # Make the first row the header
            restaurant_listings_df.columns = restaurant_listings_df.iloc[0]
        # The second worksheet is the nav worksheet
        elif n == 1:
            restaurant_nav_df = pd.DataFrame(worksheet['values'])
            # Make the first row the header
            restaurant_nav_df.columns = restaurant_nav_df.iloc[0]
        # The third worksheet is the story_settings worksheet
        elif n == 2:
            story_settings_df = pd.DataFrame(worksheet['values'])
            # Make the first row the header
            story_settings_df.columns = story_settings_df.iloc[0]
    
    # Return the three dataframes
    return restaurant_listings_df, restaurant_nav_df, story_settings_df

def process_market_directory(url, directory, db):
    """
    This function processes the market directory and updates the market database. It's the main function. It calls all the other necessary functions.
    """
    
    # Open the main market_spreadsheet and store the worksheets and dataframes
    market_spreadsheet, market_directory_ws, market_database_ws, market_directory_df, market_database_df = open_market_spreadsheet(url, directory, db)

    guide_modified_status = []
    last_modified_date_values = []

    # Loop through each GUIDE in the market_directory_df
    for index, row in market_directory_df.iterrows():
        print(f'🥡 Checking if {row["Guide name"]} has been modified...')

        # Open the guide spreadsheet and store the worksheets and dataframes
        restaurant_listings_df, restaurant_nav_df, story_settings_df = open_guide_spreadsheet(row['URL'], row['Guide name'])

        # Check the last modified date of the current guide in its system settings
        last_mod_date = check_last_mod_date(story_settings_df)
        last_modified_date_values.append(last_mod_date)

        # Get the last modified date of the current row's guide in the market directory
        last_mod_date_index = market_directory_df.columns.get_loc('Last modified')
        last_known_mod_date = market_directory_df.iloc[index, last_mod_date_index]

        # Check if the last modified date has changed
        modified_boolean = check_mod_date_changed(last_mod_date, last_known_mod_date)

        # Append the modified_boolean to the guide_mod_status list
        guide_modified_status.append(modified_boolean)

    # Use batch_update on the market_directory_ws to update the last modified date for each guide. The column to update is C.
    market_directory_ws.batch_update([
        {
            'range': f'C{index + 2}',
            'values': [[last_modified_date_values[index]]]
        }
        for index, row in market_directory_df.iterrows()
    ])

    # If any of the guides have been modified, print a message and update the market database
    if True in guide_modified_status:
        # Find the index of the modified guides
        modified_guide_indices = [i for i, x in enumerate(guide_modified_status) if x]

        # Print the names of the modified guides
        for index in modified_guide_indices:
            print(f'🚨 {market_directory_df.iloc[index]["Guide name"]} has been modified!')
    # Else, print a message saying that no guides have been modified and return. This will end the function.
    else:
        print('👍 No guides have been modified.')
        return
    
    if not market_database_df.empty:
        # Drop any row in the market_database_df that has a guide name that matches a guide that was modified. Use the guide_modified_status list to find the indices of the modified guides
        market_database_df = market_database_df[~market_database_df['Guide name'].isin(market_directory_df.iloc[modified_guide_indices]['Guide name'])]

    # Loop through the modified_guide_indices list
    for index in modified_guide_indices:
        # Open the guide spreadsheet and store the worksheets and dataframes
        restaurant_listings_df, restaurant_nav_df, story_settings_df = open_guide_spreadsheet(market_directory_df.iloc[index]['URL'], market_directory_df.iloc[index]['Guide name'])

        # Add the guide name to the restaurant_listings_df
        restaurant_listings_df['Guide name'] = market_directory_df.iloc[index]['Guide name']

        # Drop the "Display_Name", "Location" columns from the restaurant_nav_df
        restaurant_nav_df = restaurant_nav_df.drop(columns=['Display_Name', 'Location'])

        # Join the restaurant_listings_df and restaurant_nav_df on the 'Listing_Id' column
        restaurant_listings_df = restaurant_listings_df.merge(restaurant_nav_df, on='Listing_Id', how='left')

        # If the value in the "Display_Name" column is "", drop the row.
        restaurant_listings_df = restaurant_listings_df[restaurant_listings_df['Display_Name'] != '']

        # Drop the first row of the restaurant_listings_df. This is the header row.
        restaurant_listings_df = restaurant_listings_df.iloc[1:]

        # Drop any row that contains the words "Name that will be displayed" in the first column
        restaurant_listings_df = restaurant_listings_df[~restaurant_listings_df['Display_Name'].str.contains('Name that will be displayed')]

        # Using the index of the modified guide, find the URL of the guide in the market_directory_df and store it in a column called "C2P_URL"
        restaurant_listings_df['C2P_Sheet'] = market_directory_df.iloc[index]['URL']

        # Using the re.sub() function, replace the HTML tags with nothing
        restaurant_listings_df['Plain_Text'] = restaurant_listings_df['Text'].apply(lambda x: re.sub('<[^<]+?>', '', x))

        # Dedupe based on the Display_Name column
        # TODO: DEDUPE based on both the display name 
        restaurant_listings_df = restaurant_listings_df.drop_duplicates(subset=['Display_Name'])

        # STORY SETTINGS EXTRACTION
        # Search for the column index for the appearance of "Slug" in the header of story_settings_df
        slug_index = story_settings_df.columns.get_loc('Slug')

        # Get the value of the cell in the row below the key
        slug = story_settings_df.iloc[1, slug_index]

        # Search for the column index for the appearance of "Year" in the header of story_settings_df
        year_index = story_settings_df.columns.get_loc('Year')

        # Get the value of the cell in the row below the key
        year = story_settings_df.iloc[1, year_index]

        restaurant_listings_df['live_url'] = f'https://www.sfchronicle.com/{year}/{slug}'

        # Concatenate the restaurant_listings_df to the market_database_df
        market_database_df = pd.concat([market_database_df, restaurant_listings_df], ignore_index=True)

    # Write the market_database_df to the market_database_ws. Make sure to clear the existing data first.
    market_database_ws.clear()

    # Sort the market_database_df by the Display_Name column
    market_database_df = market_database_df.sort_values(by=['Display_Name'])

    # Use set_with_dataframe to write the market_database_df to the market_database_ws
    set_with_dataframe(market_database_ws, market_database_df, include_index=False, include_column_header=True)

# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f'🏙️ Working on {market}!')
    process_market_directory(info['Google spreadsheet'], info['Directory worksheet'], info['Database worksheet'])
    # time.sleep(10)

print('👍 Done!')