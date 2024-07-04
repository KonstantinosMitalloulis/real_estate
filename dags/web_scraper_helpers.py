import re
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import psycopg2
#Final Version
def rooms_area_function(soupa):
    first_part_infos= soupa.find_all('div', {'class': 'hardfact ng-star-inserted'})
    property_rooms = "blank"
    property_area = "blank"
    for i in first_part_infos :
        if i.find('div', {'class': 'hardfact__label'}).get_text(strip=True) == "Zimmer":
            property_rooms = i.find('span', {'class': 'has-font-300'}).get_text(strip=True)
        if i.find('div', {'class': 'hardfact__label'}).get_text(strip=True) == "Wohnfläche ca.":
            property_area = i.find('span', {'class': 'has-font-300'}).get_text(strip=True)
    return property_rooms,property_area




def property_price_function(price_soupa):
    first_part_infos= price_soupa.find_all('strong', {'class':'ng-star-inserted'})
    return first_part_infos[0].get_text(strip=True)



def construction_year_function(tipos_idioktisias,inserted_soup):
    construction_year = "blank"
    if tipos_idioktisias == "wohnungen":
        construction_year_apartment = inserted_soup.find_all('li', {'class': 'ng-star-inserted'})
        for i in construction_year_apartment:
            if "Baujahr" in i.get_text(strip=True):
                construction_year = i.get_text(strip=True).split(":")[1][-4:]
    else:
        construction_year_house = inserted_soup.find_all('sd-cell', {'class': 'cell ng-star-inserted'})
        for i in construction_year_house:
            if "Baujahr" in i.get_text(strip=True):
                construction_year = i.get_text(strip=True)[-4:]#.split(":")[1]
                break  # Exit the loop once the construction year is found
    return construction_year

def commercial_or_private_provider_property(inserted_soup):
    try:
        commercial_or_private_provider_property = inserted_soup.find('sd-badge', {'class': 'my-75 badge--secondary badge badge--text'}).get_text(strip=True)
    except AttributeError:commercial_or_private_provider_property = "blank"
    return commercial_or_private_provider_property






def location_details(inserted_soup):
    pattern_postal_code = r"\b\d{5}\b"
    pattern_city = r"\b\d{5}\b\s+(.+)"
    address_street = inserted_soup.find('span', {'data-cy': 'address-street'}).get_text(strip=True)
    #print(address_street)
    address_city = inserted_soup.find('span', {'data-cy': 'address-city'}).get_text(strip=True)
    postal_code =  re.search(pattern_postal_code, address_city).group(0)
    city = re.search(pattern_city, address_city).group(1)
    return city,address_street,postal_code



def floor(inserted_soup):
    try:
        floor = inserted_soup.find('p', text='Wohnungslage').find_next('p').get_text(strip=True)
    except AttributeError:floor = "blank"
    return floor




def title_property(inserted_soup):
    try:
        title_property= inserted_soup.find('h1', {'class': 'ng-star-inserted'}).get_text(strip=True)
    except AttributeError:title_property = "blank"
    return title_property


def ref_number(inserted_soup):
    try:
        ref_number = inserted_soup.find('p', {'data-cy': 'refnumber'}).get_text(strip=True).split(":")[1]
    except AttributeError:ref_number = "blank"
    return  ref_number


def online_id(inserted_soup):
    try:
        online_id = inserted_soup.find('p', {'data-cy': 'online-id'}).get_text(strip=True).split(":")[1]
    except AttributeError:online_id = "blank"
    return online_id


def delivery_time():
    utc_tz = pytz.utc
    utc_time = datetime.now(utc_tz)
    return utc_time.strftime('%Y-%m-%d %H:%M:%S')


def property_condition(inserted_soup):
    property_condition_findings = inserted_soup.find_all('li', {'class': 'ng-star-inserted'})
    zustand_text = None

    for li in property_condition_findings:
        span = li.find('span', {'class': 'color-grey-500'})
        if span and 'Zustand:' in span.get_text(strip=True):
            zustand_text = li.get_text(strip=True).replace('Zustand:', '')
            break

    if zustand_text:
        property_condition = zustand_text# Output: renoviert / saniert
    else:
        property_condition = "blank"
    return property_condition



def property_type(index_type):
    if index_type == "haeuser":
        property_type = "house"
    else:
        property_type = "apartment"
    return property_type


def offerer_name(inserted_soup):
    try:
         offerer_name = inserted_soup.find('p', {'class': 'offerer'}).get_text(strip=True)
    except AttributeError:offerer_name = "blank"
    return offerer_name


def category_of_house(inserted_soup):
    try:
        category_of_house =  inserted_soup.find('p', text='Kategorie').find_next('p').get_text(strip=True)
    except AttributeError:category_of_house = "blank"
    return category_of_house

def property_possible_move(inserted_soup):
    try:
        property_possible_move = inserted_soup.find('p', text='Bezug').find_next('p').get_text(strip=True)
    except AttributeError:property_possible_move = "blank"
    return property_possible_move


def energy_consumption(inserted_soup):
    try:
        energy_consumption = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-consumption'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_consumption = "blank"
    return energy_consumption

def energy_vadility(inserted_soup):
    try:
        energy_vadility = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-validity'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_vadility = "blank"
    return energy_vadility


def energy_passtype(inserted_soup):
    try:
        energy_passtype = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-passtype'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_passtype = "blank"
    return energy_passtype


def energy_buildingtype(inserted_soup):
    try:
        energy_buildingtype = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-buildingtype'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_buildingtype = "blank"
    return energy_buildingtype


def energy_class(inserted_soup):
    try:
        energy_class = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-class'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_class = "blank"
    return energy_class


def energy_source(inserted_soup):
    try:
        energy_source = inserted_soup.find('sd-cell-col', {'data-cy': 'energy-source'}).find('p').find_next_sibling('p').get_text(strip=True)
    except AttributeError:energy_source = "blank"
    return energy_source


def energy_equipment(inserted_soup):
    energy_equipment = inserted_soup.find_all('sd-cell-col', class_='cell__col', attrs={'data-cy': 'energy-equipment'})
    if energy_equipment == []:
        energy_provider= "blank"
        form_of_heating= "blank"
    else:              
        for equipment in energy_equipment:
            energy_title = equipment.find('p', class_='color-grey-500 has-font-75').text
            if energy_title == 'Energieträger':
                energy_provider= equipment.find_all('p')[1].text
            else:energy_provider = "blank"
            if energy_title == 'Heizungsart':
                form_of_heating= equipment.find_all('p')[1].text
            else:form_of_heating="blank"
    return energy_provider , form_of_heating

def plot_area(soupa_oikopedo):
    first_part_infos= soupa_oikopedo.find_all('div', {'class': 'hardfact ng-star-inserted'})
    oikopedo_area = "blank"
    for i in first_part_infos:
        if i.find('div', {'class': 'hardfact__label'}).get_text(strip=True) == " Grundstücksfl. ca. ".strip():
            oikopedo_area = i.find('span', {'class': 'has-font-300'}).get_text(strip=True)
    return oikopedo_area

def german_state_function(state):
    return state.replace("bl-","")

def connect_to_database():
    """ Establishes a connection to the PostgreSQL database. """
    try:
        connection = psycopg2.connect(
            dbname='real_estate_project', user='airflow', password='airflow', host='postgres', port='5432'
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None
    

 