import os
import re
import time
from datetime import datetime, timedelta

import gspread as gs
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from gspread_dataframe import set_with_dataframe

# We grab our service account from a Github secret
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open("service_account.json", "w") as f:
    f.write(SERVICE_ACCOUNT)

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename="service_account.json")

market_info = {
    "San Francisco": {
        "Google spreadsheet": "https://docs.google.com/spreadsheets/d/1_ZMnD69rrVH53194HWHUoHfKnK0yq5gJ6J83dGWle5E/edit#gid=0",
        "Directory worksheet": "SFC directory",
        "Database worksheet": "SFC DB",
        "Metadata worksheet": "SFC meta",
        "timezone": "US/Pacific",
    }
}


def create_time_stamp(timezone):
    """
    Simple function to create a time stamp for the metadata worksheet.
    """
    # Save the current time as a string in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime("%Y-%m-%d")

    # Save the current time in 12-hour format as a string in the following format: HH:MM AM/PM
    time = datetime.now(pytz.timezone(timezone)).strftime("%-I:%M %p")

    # Find the time one hour from now
    next_run = datetime.now(pytz.timezone(timezone)) + timedelta(hours=1)

    return date, time, next_run


def api_call_handler(func):
    """
    This function will retry the api call if it fails.
    """
    # Number of retries
    for i in range(0, 10):
        try:
            return func()
        except Exception as e:
            print(f"🤦‍♂️ {e}")
            print(f"🤷‍♂️ Retrying in {2 ** i} seconds...")
            time.sleep(2**i)
    print("🤬 Giving up...")
    raise SystemError


def process_market_directory(url, directory, db, timezone, metadata):
    """
    This function processes the market directory and updates the market database. It's the main function. It calls all the other necessary functions.
    """

    # Open the main market_spreadsheet and store the worksheets and dataframes
    (
        market_spreadsheet,
        market_directory_ws,
        market_database_ws,
        market_metadata_ws,
        market_directory_df,
        market_database_df,
        market_metadata_df,
    ) = open_market_spreadsheet(url, directory, db, metadata)

    updated_market_database_df = pd.DataFrame()

    # Loop through each GUIDE in the market_directory_df
    for index, row in market_directory_df.iterrows():
        print(f'🥡 Working on {row["Guide name"]}...')
        # Open the guide spreadsheet and store the worksheets and dataframes
        (
            restaurant_listings_df,
            restaurant_nav_df,
            story_settings_df,
        ) = open_guide_spreadsheet(row["C2P Sheet URL"], row["Guide name"])

        live_page_df = scrape_live_guide(
            row["Live URL"], row["Guide name"], row["C2P Sheet URL"]
        )

        # Dedupe the restaurant_nav_df
        restaurant_nav_df = restaurant_nav_df.drop_duplicates(subset=["Listing_Id"])

        # Join the restaurant_nav_df to the live_page_df on the "Listing_Id" column. From the restaurant_nav_df, I only want the Lat and Lng columns.
        live_page_df = live_page_df.join(
            restaurant_nav_df[["Listing_Id", "Lat", "Lng"]].set_index("Listing_Id"),
            on="Listing_Id",
        )

        # Concateenate the live_page_df to the updated_market_database_df
        updated_market_database_df = pd.concat(
            [updated_market_database_df, live_page_df]
        )

    # Sort the updated_market_database_df by the Display_Name column
    updated_market_database_df = updated_market_database_df.sort_values(
        by=["Display_Name"]
    )

    # Clear the market_database_ws
    market_database_ws.clear()

    set_with_dataframe(market_database_ws, updated_market_database_df)

    # Get the current time and date
    date, time, next_run = create_time_stamp(timezone)

    # Rewrite the above updates to be in one batch call
    market_metadata_ws.batch_update(
        [
            {"range": "B1", "values": [[date]]},
            {"range": "B2", "values": [[time]]},
            {"range": "B3", "values": [[next_run.strftime("%-I:%M %p")]]},
        ]
    )


def open_market_spreadsheet(url, directory, db, metadata):
    """
    This function opens each market's main spreadsheet and returns the worksheets and dataframes.
    """
    print("📂 Opening market spreadsheet...")

    # Open the main spreadsheet
    market_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

    # Open the directory and database worksheets contained in the main market_spreadsheet
    market_directory_ws = api_call_handler(
        lambda: market_spreadsheet.worksheet(directory)
    )
    market_database_ws = api_call_handler(lambda: market_spreadsheet.worksheet(db))
    market_metadata_ws = api_call_handler(
        lambda: market_spreadsheet.worksheet(metadata)
    )

    # Store the directory and database worksheets in pandas dataframes
    market_directory_df = api_call_handler(
        lambda: pd.DataFrame(market_directory_ws.get_all_records())
    )
    market_database_df = api_call_handler(
        lambda: pd.DataFrame(market_database_ws.get_all_records())
    )
    market_metadata_df = api_call_handler(
        lambda: pd.DataFrame(market_metadata_ws.get_all_records())
    )

    # Return the worksheets and dataframes
    return (
        market_spreadsheet,
        market_directory_ws,
        market_database_ws,
        market_metadata_ws,
        market_directory_df,
        market_database_df,
        market_metadata_df,
    )


def open_guide_spreadsheet(url, name):
    """
    This function opens the guide spreadsheet and returns the three dataframes.
    """
    # print('🍑 Opening guide spreadsheet...')

    # Open the guide spreadsheet
    guide_spreadsheet = api_call_handler(lambda: gc.open_by_url(url))

    # guide_worksheets = guide_spreadsheet.values_batch_get(
    #     ranges=["listings!A1:Z1000", "nav!A1:Z1000", "story_settings!A1:Z1000"]
    # )

    guide_worksheets = api_call_handler(
        lambda: guide_spreadsheet.values_batch_get(
            ranges=["listings!A1:Z1000", "nav!A1:Z1000", "story_settings!A1:Z1000"]
        )
    )

    guide_worksheets = guide_worksheets["valueRanges"]

    # Loop through the worksheets in the spreadsheet_dict. Add the values to the appropriate dataframe
    for n, worksheet in enumerate(guide_worksheets):
        # The first worksheet is the listings worksheet
        if n == 0:
            restaurant_listings_df = pd.DataFrame(worksheet["values"])
            # Make the first row the header
            restaurant_listings_df.columns = restaurant_listings_df.iloc[0]
        # The second worksheet is the nav worksheet
        elif n == 1:
            restaurant_nav_df = pd.DataFrame(worksheet["values"])
            # Make the first row the header
            restaurant_nav_df.columns = restaurant_nav_df.iloc[0]
        # The third worksheet is the story_settings worksheet
        elif n == 2:
            story_settings_df = pd.DataFrame(worksheet["values"])
            # Make the first row the header
            story_settings_df.columns = story_settings_df.iloc[0]

    # Return the three dataframes
    return restaurant_listings_df, restaurant_nav_df, story_settings_df


def getSoup(url):
    """
    This function returns the BeautifulSoup object for the given URL.
    """
    headers = {"x-px-access-token": ACCESS_TOKEN}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup


def scrape_live_guide(url, guide_name, c2p_sheet_url):
    """
    This function scrapes the live guide and returns a dataframe with the scraped data.
    """

    soup = getSoup(url)

    places = soup.find_all("div", class_="place")

    place_data = []

    for place in places:
        # The main image is in an img tag with a class of image-gallery-image. Scrape the src attribute of the first one.
        image = place.find_all("img", class_="image-gallery-image")

        img_src_list = []
        alt_text_list = []
        credits_list = []

        if image:
            for img in image:
                # Use the following regex to extract the wcm_id from the src attribute: \/(\d{5,})\/
                wcm_id = re.search(r"\/(\d{5,})\/", img["src"]).group(1)
                img_src_list.append(wcm_id)

                # The alt text is in the alt attribute.
                if img["alt"]:
                    alt_text_list.append(img["alt"])
                else:
                    alt_text_list.append("")

                # The credits are in a span with a class of image-gallery-description
                credits = img.find_next("span", class_="image-gallery-description")
                if credits:
                    credits = credits.text.strip()
                else:
                    credits = ""
                credits_list.append(credits)

        # Join the list of images into a string separated by semicolons.
        image_src = "; ".join(img_src_list)

        # Join the list of alt text into a string separated by semicolons.
        alt_text = "; ".join(alt_text_list)
        if alt_text == "; ; ":
            alt_text = None

        # Join the list of credits into a string separated by semicolons.
        credits = "; ".join(credits_list)

        # Find all the label elements in the place. Ignore ones that contain a span.
        label_list = place.find_all("label")

        # Extract the text from each label. Keep in list.
        label_list = [
            label.text.strip() for label in label_list if not label.find("span")
        ]

        # Grab the div that has a class that starts with listing-module--details. This contains the address and description.
        details = place.find("div", class_=re.compile("listing-module--details"))

        # In the details div, find the p tag that contains the words "Payment Options". The next span contains the payment options.
        payment_options = details.find("span", string=re.compile("Payment options"))

        # If there are payment options, extract the text from the next span.
        if payment_options:
            payment_options = payment_options.find_next("span").text.strip()
        else:
            payment_options = ""

        # If there are Drinks, extract the text from the next span.
        drinks = details.find("span", string=re.compile("Drinks"))

        if drinks:
            drinks = drinks.find_next("span").text.strip()
        else:
            drinks = ""

        # If there are Hours, extract the text from the next span.
        hours = details.find("span", string=re.compile("Hours"))
        if hours:
            hours = hours.find_next("span").text.strip()
        else:
            hours = ""

        # If there is Phone, extract the text from the next span.
        phone = details.find("span", string=re.compile("Phone"))
        if phone:
            phone = phone.find_next("span").text.strip()
        else:
            phone = ""

        # If there is Website, extract the href from the current span.
        website = details.find("span", string=re.compile("Website"))
        if website:
            website = website.find("a")["href"]
        else:
            website = ""

        # If there is Order online, extract the href from the current span.
        order_online = details.find("span", string=re.compile("Order online"))
        if order_online:
            order_online = order_online.find("a")["href"]
        else:
            order_online = ""

        # If there is More coverage, extract the href from the current span.
        more_coverage = details.find("span", string=re.compile("More coverage"))
        if more_coverage:
            more_coverage = more_coverage.find("a")["href"]
        else:
            more_coverage = ""

        # If there is Read the full review, extract the href from the current span.
        read_the_full_review = details.find(
            "span", string=re.compile("Read the full review")
        )
        if read_the_full_review:
            read_the_full_review = read_the_full_review.find("a")["href"]
        else:
            read_the_full_review = ""

        # Get the ID of the current place div
        Listing_Id = place["id"]

        address = place.find("div", itemprop="address")
        if address:
            address = address.text.strip()
        else:
            address = "Location varies"

        place_data.append(
            {
                "Display_Name": place.find("h2").text.strip(),
                "Listing_Id": Listing_Id,
                "Location": address,
                "Text_plain": place.find("div", itemprop="description").text.strip(),
                "text_rich": place.find("div", itemprop="description")
                .decode_contents()
                .strip(),
                "Images": image_src,
                "Alt_Text": alt_text,
                "Credits": credits,
                "Takeout": "Takeout" in label_list,
                "Delivery": "Delivery" in label_list,
                "Outdoor seating": "Outdoor seating" in label_list,
                "Indoor seating": "Indoor seating" in label_list,
                "Vegetarian options": "Vegetarian options" in label_list,
                "Top 25 restaurant": "Top 25 restaurant" in label_list,
                "Payment options": payment_options,
                "Drinks": drinks,
                "Hours": hours,
                "Phone": phone,
                "Website": website,
                "Order online": order_online,
                "Related story": more_coverage,
                "Review link": read_the_full_review,
                "Guide name": guide_name,
                "Live URL": url,
                "C2P Sheet URL": c2p_sheet_url,
            }
        )

    # Create a dataframe from the list of dictionaries.
    guide_df = pd.DataFrame(place_data)

    return guide_df


# Loop through the market_info dictionary
for market, info in market_info.items():
    # Print the print the market name and its corresponding Google spreadsheet URL
    print(f"🏙️ Working on {market}!")
    process_market_directory(
        info["Google spreadsheet"],
        info["Directory worksheet"],
        info["Database worksheet"],
        info["timezone"],
        info["Metadata worksheet"],
    )
    # time.sleep(10)

print("👍 Done!")

# Remove the temporary json file. We don't anyone to see our service account credentials!
os.remove("service_account.json")
