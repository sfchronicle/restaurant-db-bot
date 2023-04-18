import gspread as gs
import pandas as pd

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0')

roundup_ws = sh.worksheet('SF roundups')

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

    df = pd.DataFrame(individual_roundup_ws.get_all_records())

    # If the value in the "Display_Name" column is "", then drop the row
    df = df[df['Display_Name'] != '']

    # Drop the first row, which is the header row
    df = df.drop([0])

    # Drop duplicate rows based on the "Display_Name" column
    df = df.drop_duplicates(subset=['Display_Name'])

    # Add a column that contains the title of the spreadsheet
    df['Review roundup'] = title

    # Add a column that contains the URL of the spreadsheet
    df['URL'] = url

    # Concatenate the individual dataframes into one big dataframe
    central_df = pd.concat([central_df, df])

    story_settings_ws = individual_roundup_sh.worksheet('story_settings')

    # Search for the column of the appearance of the key: Publish_Date
    publish_date_index = story_settings_ws.find('LastModDate_C2P').col

    # Get the value of the cell in the row below the key
    publish_date = story_settings_ws.cell(2, publish_date_index).value

    # In the roundup_ws, update the cell in the "Last updated" column that corresponds to the URL with the publish_date
    roundup_ws.update_cell(i + 2, 3, publish_date)

# Write the central dataframe to the 'SF DB' worksheet
central_ws = sh.worksheet('SF DB')

# Replace the existing worksheet with the data in the dataframe
central_ws.clear()
central_ws.update([central_df.columns.values.tolist()] + central_df.values.tolist())

print('🥳 Done!')