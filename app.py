import re

import gspread as gs
import pandas as pd
import requests
from bs4 import BeautifulSoup
from gspread_dataframe import set_with_dataframe

# We authenticate with Google using the service account json we created earlier.
gc = gs.service_account(filename='service_account.json')

def getSoup(url):
    page = requests.get(url) 
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup

def scrape_live_guide(url):
    print('🐝 Scraping live guide...')
    soup = getSoup(url)
    
    places = soup.find_all('div', class_='place')

    place_data = []

    for place in places:
        # The main image is in an img tag with a class of image-gallery-image. Scrape the src attribute of the first one.
        image = place.find_all('img', class_='image-gallery-image')

        img_src_list = []
        alt_text_list = []
        credits_list = []

        if image:
            for img in image:
                # Use the following regex to extract the wcm_id from the src attribute: \/(\d{5,})\/
                wcm_id = re.search(r'\/(\d{5,})\/', img['src']).group(1)
                img_src_list.append(wcm_id)

                # The alt text is in the alt attribute.
                alt_text_list.append(img['alt'])

                # The credits are in a span with a class of image-gallery-description
                credits = img.find_next('span', class_='image-gallery-description').text.strip()
                credits_list.append(credits)

        # Join the list of images into a string separated by semicolons.
        image_src = '; '.join(img_src_list)

        # Join the list of alt text into a string separated by semicolons.
        alt_text = '; '.join(alt_text_list)

        # Join the list of credits into a string separated by semicolons.
        credits = '; '.join(credits_list)

        # Find all the label elements in the place. Ignore ones that contain a span.
        label_list = place.find_all('label')

        # Extract the text from each label. Keep in list.
        label_list = [label.text.strip() for label in label_list if not label.find('span')]

        # Grab the div that has a class that starts with listing-module--details. This contains the address and description.
        details = place.find('div', class_=re.compile('listing-module--details'))

        # In the details div, find the p tag that contains the words "Payment Options". The next span contains the payment options.
        payment_options = details.find('span', string=re.compile('Payment options'))

        # If there are payment options, extract the text from the next span.
        if payment_options:
            payment_options = payment_options.find_next('span').text.strip()
        else:
            payment_options = ''

        # If there are Drinks, extract the text from the next span.
        drinks = details.find('span', string=re.compile('Drinks'))

        if drinks:
            drinks = drinks.find_next('span').text.strip()
        else:
            drinks = ''

        # If there are Hours, extract the text from the next span.
        hours = details.find('span', string=re.compile('Hours'))
        if hours:
            hours = hours.find_next('span').text.strip()
        else:
            hours = ''

        # If there is Phone, extract the text from the next span.
        phone = details.find('span', string=re.compile('Phone'))
        if phone:
            phone = phone.find_next('span').text.strip()
        else:
            phone = ''

        # If there is Website, extract the href from the current span.
        website = details.find('span', string=re.compile('Website'))
        if website:
            website = website.find('a')['href']
        else:
            website = ''

        # If there is Order online, extract the href from the current span.
        order_online = details.find('span', string=re.compile('Order online'))
        if order_online:
            order_online = order_online.find('a')['href']
        else:
            order_online = ''

        # If there is More coverage, extract the href from the current span.
        more_coverage = details.find('span', string=re.compile('More coverage'))
        if more_coverage:
            more_coverage = more_coverage.find('a')['href']
        else:
            more_coverage = ''

        # If there is Read the full review, extract the href from the current span.
        read_the_full_review = details.find('span', string=re.compile('Read the full review'))
        if read_the_full_review:
            read_the_full_review = read_the_full_review.find('a')['href']
        else:
            read_the_full_review = ''

        # Get the ID of the current place div
        Listing_Id = place['id']

        place_data.append({
            'Display_Name': place.find('h2', class_='subhead-bold').text.strip(),
            'Listing_Id': Listing_Id,
            'Location': place.find('div', itemprop='address').text.strip(),
            'Text_plain': place.find('div', itemprop='description').text.strip(),
            'Images': image_src,
            'Alt_Text': alt_text,
            'Credits': credits,
            'Takeout': 'Takeout' in label_list,
            'Delivery': 'Delivery' in label_list,
            'Outdoor seating': 'Outdoor seating' in label_list,
            'Indoor seating': 'Indoor seating' in label_list,
            'Vegetarian options': 'Vegetarian options' in label_list,
            'Top 25 restaurant': 'Top 25 restaurant' in label_list,
            'Payment options': payment_options,
            'Drinks': drinks,
            'Hours': hours,
            'Phone': phone,
            'Website': website,
            'Order online': order_online,
            'Related story': more_coverage,
            'Review link': read_the_full_review
        })

    # Create a dataframe from the list of dictionaries.
    guide_df = pd.DataFrame(place_data)

    print(guide_df.head())

    print('🐝 Done scraping live guide.')

url = 'https://www.sfchronicle.com/projects/2023/best-sourdough-sf-bay-area/'

scrape_live_guide(url)