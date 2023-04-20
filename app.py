import re
import time

import gspread as gs
import pandas as pd
from gspread_dataframe import set_with_dataframe

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

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

def update_restaurant_db(info):
    spreadsheet = gc.open_by_url(info['Google spreadsheet'])
    
    directory_ws = spreadsheet.worksheet(info['Directory worksheet'])

    directory_df = pd.DataFrame(directory_ws.get_all_records())

    # Turn the "URL" column into a list
    directory_url_list = directory_df['URL'].tolist()
    title_list = directory_df['Guide name'].tolist()

    db_df = pd.DataFrame()

    last_updated_values = []

    for i, url in enumerate(directory_url_list):
        restaurant_guide_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

        spreadsheet_values = restaurant_guide_spreadsheet.values_batch_get(
                # The range of cells we want to get
                ranges=['listings!A1:Z1000', 'nav!A1:Z1000', 'story_settings!A1:Z1000']
        )

        spreadsheet_dict = spreadsheet_values['valueRanges']

        # Create an empty dataframe titled restaurant_listings_df
        restaurant_listings_df = pd.DataFrame()

        # Create an empty dataframe titled restaurant_nav_df
        restaurant_nav_df = pd.DataFrame()

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

        # Get the title of the spreadsheet
        title = title_list[i]
        print(f'🐝 Working on {title}...')

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

        # In the directory_ws, update the cell in the "Last updated" column that corresponds to the URL with the last_mod_date
        # directory_ws.update_cell(i + 2, 3, last_mod_date)

        # Concatenate the db_df and the merged_df
        db_df = pd.concat([db_df, merged_df])
        time.sleep(5)

    # Using the last_updated_values list, batch update the "Last Updated" column in the directory worksheet. This is in column C.
    directory_ws.batch_update([{
        'range': f'C{i+2}:C{i+2}',
        'values': [[last_updated_values[i]]]
    } for i in range(0, len(last_updated_values))])
    
    # Sort the db_df by the "Display_Name" column
    db_df = db_df.sort_values(by=['Display_Name'])

    # Drop duplicate rows based on the "Listing-Id" column
    db_df = db_df.drop_duplicates(subset=['Listing_Id'])

    # Write the db_df to the database worksheet
    print('📝 Writing to database worksheet...')
    db_worksheet = spreadsheet.worksheet(info['Database worksheet'])
    db_worksheet.clear()
    
    # Update the values in db_worksheet with the values in db_df using the set_with_dataframe() method
    # db_worksheet.set_with_dataframe(db_df, include_index=False, include_column_header=True)
    set_with_dataframe(db_worksheet, db_df, include_index=False, include_column_header=True)

    print(f'🥳 {info["Database worksheet"]} has been updated!')

# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f'🏙️ Working on {market}!')
    update_restaurant_db(info)
    time.sleep(10)

print('✅ All done!')