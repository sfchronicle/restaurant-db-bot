import re
import time

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

    db_df = pd.DataFrame()

    for i, url in enumerate(directory_url_list):
        restaurant_guide_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

        # Get the title of the spreadsheet
        title = restaurant_guide_spreadsheet.title
        print(f'🐝 Working on {title}...')

        restaurant_listings_worksheet = api_call_handler(lambda: restaurant_guide_spreadsheet.worksheet('listings'))
        
        restaurant_nav_worksheet = restaurant_guide_spreadsheet.worksheet('nav')
        restaurant_nav_df = pd.DataFrame(restaurant_nav_worksheet.get_all_records())
        # Drop the first column of the restaurant_nav_df
        restaurant_nav_df = restaurant_nav_df.drop(columns=['Display_Name', 'Location'])

        restaurant_listings_df = pd.DataFrame(api_call_handler(lambda: restaurant_listings_worksheet.get_all_records()))

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

        story_settings_worksheet = restaurant_guide_spreadsheet.worksheet('story_settings')

        story_settings_df = pd.DataFrame(story_settings_worksheet.get_all_records())

        # Search for the column index for the appearance of "LastModDate_C2P" in the header of story_settings_df
        LastModDate_index = story_settings_df.columns.get_loc('LastModDate_C2P')

        # Get the value of the cell in the row below the key
        last_mod_date = story_settings_df.iloc[0, LastModDate_index]

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
        directory_ws.update_cell(i + 2, 3, last_mod_date)

        # Concatenate the db_df and the merged_df
        db_df = pd.concat([db_df, merged_df])
        # time.sleep(10)
    
    # Sort the db_df by the "Display_Name" column
    db_df = db_df.sort_values(by=['Display_Name'])

    # Drop duplicate rows based on the "Listing-Id" column
    db_df = db_df.drop_duplicates(subset=['Listing_Id'])

    # Write the db_df to the database worksheet
    print('📝 Writing to database worksheet...')
    db_worksheet = spreadsheet.worksheet(info['Database worksheet'])
    db_worksheet.clear()
    db_worksheet.update([db_df.columns.values.tolist()] + db_df.values.tolist())

    print(f'🥳 {info["Database worksheet"]} has been updated!')

# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f'🏙️ Working on {market}!')
    update_restaurant_db(info)
    time.sleep(10)

print('✅ All done!')