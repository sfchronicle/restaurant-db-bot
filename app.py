import re
import time

import gspread as gs
import pandas as pd
from gspread_dataframe import set_with_dataframe

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

# I need a function that finds all the guides in the directory that have been modified since the last time the script ran
def find_modified_guides(market, directory_df, memory_df):
    """
    This function will find all the guides in the directory that have been modified since the last time the script ran.
    """
    memory_df['Guide id'] = memory_df['Guide name'] + memory_df['Last updated']
    
    # Create an empty dataframe called modified_guides to store the modified guides
    modified_guides = pd.DataFrame(columns=['Guide name', 'URL', 'Last updated'])
    modified_guide_ids = []

    for i, row in directory_df.iterrows():
        guide_id = f'{row["Guide name"]}{row["Last updated"]}'
        if guide_id not in memory_df['Guide id'].values:
            print(f'👀 {row["Guide name"]} has been modified! Updating database...')
            # Concatenate the row to the modified_guides dataframe
            modified_guides = pd.concat([modified_guides, pd.DataFrame(row).T], ignore_index=True)
            modified_guide_ids.append(guide_id)
    
    modified_guides['Guide id'] = modified_guide_ids

    return modified_guides, modified_guide_ids

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

def update_restaurant_db(market, info):
    """
    This function will update the restaurant database for the market specified in the info dictionary.
    """

    # Open the market's spreadsheet
    spreadsheet = gc.open_by_url(info['Google spreadsheet'])
    
    # Now we access the market's directory worksheet
    directory_ws = spreadsheet.worksheet(info['Directory worksheet'])

    # Here we convert the directory worksheet into a dataframe
    directory_df = pd.DataFrame(directory_ws.get_all_records())

    market = market.lower().replace(' ', '_')

    # We need to find the guides that have been modified since the last time the script ran.
    # First we need to get the memory dataframe
    memory_df = pd.read_json(f'data/{market}_memory.json')

    # Now we need to find the modified guides
    modified_guides, modified_guide_ids = find_modified_guides(market, directory_df, memory_df)

    # Turn the "Guide name" column into a list
    title_list = modified_guides['Guide name'].tolist()

    # Turn the "URL" column into a list
    directory_url_list = modified_guides['URL'].tolist()

    # Turn the "Last updated" column into a list
    last_updated_list = modified_guides['Last updated'].tolist()

    # Drop the Last updated column
    modified_guides = modified_guides.drop(columns=['Last updated'])


    # Create an empty dataframe titled db_df. This is what will hold all the data from the various guides.
    db_df = pd.DataFrame()

    # This list will hold the last modified dates for each guide
    last_updated_values = []

    # Loop through the URLs in the directory_url_list
    for i, url in enumerate(directory_url_list):
        # Get the title of the spreadsheet
        title = title_list[i]
        print(f'🐝 Working on {title}...')

        # Open the spreadsheet. We use the api_call_handler() function to retry the api call if it fails.
        restaurant_guide_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

        # Here we're grabbing all the values from the listings, nav, and story_settings worksheets in one go. This is more efficient than grabbing the values one worksheet at a time.
        spreadsheet_values = restaurant_guide_spreadsheet.values_batch_get(
                # The range of cells we want to get. We go all the way to Z1000 to make sure we get all the data.
                ranges=['listings!A1:Z1000', 'nav!A1:Z1000', 'story_settings!A1:Z1000']
        )

        # Get the values from the spreadsheet
        spreadsheet_dict = spreadsheet_values['valueRanges']

        # Loop through the worksheets in the spreadsheet_dict. Add the values to the appropriate dataframe
        for n, worksheet in enumerate(spreadsheet_dict):
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

        # Drop the first column of the restaurant_nav_df
        restaurant_nav_df = restaurant_nav_df.drop(columns=['Display_Name', 'Location'])

        # Join the restaurant_nav_df to the df on the Listing_Id column
        merged_df = restaurant_listings_df.merge(restaurant_nav_df, on='Listing_Id', how='left')

        # If the value in the "Display_Name" column is "", then drop the row
        merged_df = merged_df[merged_df['Display_Name'] != '']

        # Drop the first row, which is the header row
        merged_df = merged_df.drop([0])

        # Add a column that contains the title of the spreadsheet
        merged_df['Review roundup'] = title

        # Add a column that contains the URL of the spreadsheet
        merged_df['C2P_Sheet'] = url

        # Using the re.sub() function, replace the HTML tags with nothing
        merged_df['Plain_Text'] = merged_df['Text'].apply(lambda x: re.sub('<[^<]+?>', '', x))

        # Search for the column index for the appearance of "LastModDate_C2P" in the header of story_settings_df
        LastModDate_index = story_settings_df.columns.get_loc('LastModDate_C2P')

        # Get the value of the cell in the row below the key
        last_mod_date = story_settings_df.iloc[1, LastModDate_index]

        last_updated_values.append(last_mod_date)

        # Search for the column index for the appearance of "Slug" in the header of story_settings_df
        slug_index = story_settings_df.columns.get_loc('Slug')

        # Get the value of the cell in the row below the key
        slug = story_settings_df.iloc[0, slug_index]

        # Search for the column index for the appearance of "Year" in the header of story_settings_df
        year_index = story_settings_df.columns.get_loc('Year')

        # Get the value of the cell in the row below the key
        year = story_settings_df.iloc[0, year_index]

        merged_df['C2P_Live_Link'] = f'https://www.sfchronicle.com/{year}/{slug}'

        # Concatenate the db_df and the merged_df
        db_df = pd.concat([db_df, merged_df])
        time.sleep(5)

    # Using the last_updated_values list, batch update the "Last Updated" column in the directory worksheet. This is in column C.
    directory_ws.batch_update([{
        'range': f'C{i+2}:C{i+2}',
        'values': [[last_updated_values[i]]]
    } for i in range(0, len(last_updated_values))])

    # Convert the directory_ws to a dataframe
    directory_df = pd.DataFrame(directory_ws.get_all_records())

    # Drop the URL column from the directory_df
    directory_df = directory_df.drop(columns=['URL'])

    # Lowercase the market value and replace spaces with underscores
    market = market.lower().replace(' ', '_')

    # Write the directory_df to a json file. Make the current market the name of the file.
    directory_df.to_json(f'data/{market}_memory.json', orient='records')

    # Sort the db_df by the "Display_Name" column
    db_df = db_df.sort_values(by=['Display_Name'])

    # Drop duplicate rows based on the "Listing-Id" column
    db_df = db_df.drop_duplicates(subset=['Listing_Id'])

    # Write the db_df to the database worksheet
    print('📝 Writing to database worksheet...')
    db_worksheet = spreadsheet.worksheet(info['Database worksheet'])
    db_worksheet.clear()
    
    # Update the values in db_worksheet with the values in db_df using the set_with_dataframe() method
    set_with_dataframe(db_worksheet, db_df, include_index=False, include_column_header=True)

    print(f'🥳 {info["Database worksheet"]} has been updated!')

# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f'🏙️ Working on {market}!')
    update_restaurant_db(market, info)
    # time.sleep(10)

print('✅ All done!')